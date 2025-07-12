use clap::{Parser, ArgAction};
use log::{info, debug, error};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use mysql::{prelude::*, Pool};
use rusqlite::{params, Connection};

#[derive(Parser, Debug)]
#[command(author, version, about = "Convert MySQL dump or database to SQLite", long_about = None)]
struct Args {
    /// MySQL dump file. If omitted, connect to a remote MySQL database
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Output SQLite file
    #[arg(short, long)]
    output: PathBuf,

    /// Host for remote MySQL connection
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Port for remote MySQL connection
    #[arg(long, default_value_t = 3306)]
    port: u16,

    /// User for remote MySQL connection
    #[arg(long, default_value = "root")]
    user: String,

    /// Password for remote MySQL connection
    #[arg(long)]
    password: Option<String>,

    /// Database for remote MySQL connection
    #[arg(long)]
    database: Option<String>,

    /// Include only the specified tables (comma separated)
    #[arg(long, value_delimiter = ',', value_name = "TABLE")]
    include_tables: Vec<String>,

    /// Exclude the specified tables (comma separated)
    #[arg(long, value_delimiter = ',', value_name = "TABLE")]
    exclude_tables: Vec<String>,

    /// Store JSON columns as TEXT
    #[arg(long, default_value_t = true)]
    json_as_text: bool,

    /// Vacuum SQLite database after import
    #[arg(long, action = ArgAction::SetTrue)]
    vacuum: bool,

    /// Increase verbosity (-v, -vv)
    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,

    /// Quiet mode
    #[arg(short, long, action = ArgAction::SetTrue)]
    quiet: bool,
}

fn setup_logger(verbose: u8, quiet: bool) {
    use env_logger::Env;
    let level = if quiet {
        "error"
    } else {
        match verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        }
    };
    env_logger::Builder::from_env(Env::default().default_filter_or(level)).init();
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    setup_logger(args.verbose, args.quiet);

    info!("Starting conversion");

    let conn = Connection::open(&args.output)?;

    if let Some(ref dump_file) = args.input {
        info!("Importing from dump file: {:?}", dump_file);
        convert_dump(&conn, dump_file.clone(), &args)?;
    } else {
        info!("Connecting to remote MySQL at {}:{}", args.host, args.port);
        convert_remote(&conn, &args)?;
    }

    if args.vacuum {
        info!("Running VACUUM to compact database");
        conn.execute_batch("VACUUM")?;
    }

    info!("Done");
    Ok(())
}

fn should_skip_table(args: &Args, table: &str) -> bool {
    if !args.include_tables.is_empty() && !args.include_tables.contains(&table.to_string()) {
        return true;
    }
    if args.exclude_tables.contains(&table.to_string()) {
        return true;
    }
    false
}

fn convert_dump(conn: &Connection, dump_file: PathBuf, _args: &Args) -> Result<(), Box<dyn Error>> {
    let file = File::open(dump_file)?;
    let reader = BufReader::new(file);
    let mut statement = String::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim_start();
        if trimmed.starts_with("--")
            || trimmed.starts_with("/*")
            || trimmed.starts_with("LOCK TABLES")
            || trimmed.starts_with("UNLOCK TABLES")
            || trimmed.starts_with("SET ")
            || trimmed.starts_with("DELIMITER ")
        {
            continue;
        }

        statement.push_str(trimmed);
        statement.push('\n');
        if trimmed.ends_with(';') {
            let mut exec_stmt = statement.trim().to_string();
            if exec_stmt.starts_with("CREATE TABLE") {
                if let Some(pos) = exec_stmt.rfind(')') {
                    exec_stmt.truncate(pos + 1);
                    exec_stmt.push(';');
                }
            }
            debug!("Executing: {}", exec_stmt);
            if let Err(e) = conn.execute_batch(&exec_stmt) {
                error!("Failed to execute statement: {}", e);
            }
            statement.clear();
        }
    }

    Ok(())
}

fn convert_remote(conn: &Connection, args: &Args) -> Result<(), Box<dyn Error>> {
    let builder = mysql::OptsBuilder::new();
    builder.clone().ip_or_hostname(Some(args.host.as_str()));
    builder.clone().tcp_port(args.port);
    builder.clone().user(Some(args.user.as_str()));
    if let Some(ref p) = args.password {
        builder.clone().pass(Some(p));
    }
    if let Some(ref db) = args.database {
        builder.clone().db_name(Some(db));
    }

    let pool = Pool::new(builder)?;
    let mut mysql_conn = pool.get_conn()?;

    let tables: Vec<String> = mysql_conn.query("SHOW TABLES")?;
    for table in tables {
        if should_skip_table(args, &table) {
            info!("Skipping table {}", table);
            continue;
        }
        info!("Processing table {}", table);
        let desc: Vec<mysql::Row> = mysql_conn.exec(&format!("DESCRIBE `{}`", table), ())?;
        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut create_sql = format!("CREATE TABLE IF NOT EXISTS `{}` (", table);
        for (i, row) in desc.iter().enumerate() {
            let field: String = row.get("Field").unwrap();
            let ty: String = row.get("Type").unwrap();
            let sqlite_ty = if ty.to_lowercase().starts_with("int") {
                "INTEGER"
            } else if ty.to_lowercase().starts_with("varchar") || ty.to_lowercase().starts_with("text") {
                "TEXT"
            } else if ty.to_lowercase().starts_with("json") && args.json_as_text {
                "TEXT"
            } else {
                "BLOB"
            };
            if i > 0 { create_sql.push_str(", "); }
            create_sql.push_str(&format!("`{}` {}", field, sqlite_ty));
            columns.push(field);
            placeholders.push("?".to_string());
        }
        create_sql.push(')');
        debug!("Create SQL: {}", create_sql);
        conn.execute(&create_sql, [])?;

        let query = format!("SELECT * FROM `{}`", table);
        let mut stmt = conn.prepare(&format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            table,
            columns.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(","),
            placeholders.join(",")
        ))?;

        let rows: Vec<mysql::Row> = mysql_conn.exec(query, ())?;
        for row in rows {
            let values: Vec<rusqlite::types::Value> = columns
                .iter()
                .enumerate()
                .map(|(idx, _)| {
                    let value: Option<mysql::Value> = row.get(idx);
                    match value {
                        Some(mysql::Value::Bytes(bytes)) => {
                            rusqlite::types::Value::from(std::str::from_utf8(&bytes).unwrap().to_string())
                        }
                        Some(mysql::Value::NULL) | None => rusqlite::types::Value::Null,
                        Some(mysql::Value::Int(i)) => rusqlite::types::Value::from(i),
                        Some(mysql::Value::UInt(u)) => rusqlite::types::Value::from(u as i64),
                        Some(mysql::Value::Float(f)) => rusqlite::types::Value::from(f),
                        Some(mysql::Value::Double(f)) => rusqlite::types::Value::from(f),
                        Some(mysql::Value::Date(y,m,d,h,min,s,ms)) => {
                            let dt = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}", y,m,d,h,min,s,ms);
                            rusqlite::types::Value::from(dt)
                        }
                        Some(mysql::Value::Time(..)) => rusqlite::types::Value::Null,
                    }
                })
                .collect();
            stmt.execute(rusqlite::params_from_iter(values))?;
        }
    }
    Ok(())
}
