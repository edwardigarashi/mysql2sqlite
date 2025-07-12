# mysql2sqlite

`mysql2sqlite` is a command line tool written in Rust that converts MySQL dumps or a live MySQL database into an SQLite database.

## Features

- Read from a MySQL dump file or connect directly to a MySQL server
- Include or exclude specific tables during conversion
- Optionally treat JSON columns as plain text
- Adjustable logging verbosity (`-v`, `-vv`, `--quiet`)
- Ability to vacuum the resulting SQLite database to minimise disk usage

## Usage

```bash
mysql2sqlite --input dump.sql --output db.sqlite
```

Or connect to a remote MySQL server:

```bash
mysql2sqlite --host localhost --user root --database mydb \
    --output db.sqlite --include-tables users,orders
```

Run with `--help` to see all options.
