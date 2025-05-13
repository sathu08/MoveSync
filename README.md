# MoveSync

**MoveSync** is a lightweight IT migration tool designed to streamline database migrations. Currently, it supports **PostgreSQL** migrations and offers a scriptable, customizable interface for managing data transfer between PostgreSQL instances.

---

## Features

* 📦 Migrate data between PostgreSQL databases
* ⚙️ Multi-threaded processing for performance
* 🐘 Built using SQLAlchemy for efficient database handling
* 📊 Tabular logs and clear CLI output
* 🧪 Easily extensible for future support of other database systems

---

## Requirements

* Python 3.10+
* PostgreSQL access (source and destination)
* pip (Python package installer)

---

## Installation

Clone the repository and install dependencies:

```bash
https://github.com/sathu08/MoveSync.git
cd movesync
pip install -r requirements.txt
```

---

## Usage

```bash
python MoveSync.py [--database=<dbname>] [--info=<client>] [--start] [--help] [-y] [--reports] [--setup] [--startmanual]
```

### Options

* `--database=<dbname>`: Specify the name of the database to migrate
* `--info=<client>`: Provide client-specific metadata
* `--start`: Begin the automated migration process
* `--startmanual`: Start migration in manual mode
* `--reports`: Generate migration reports
* `--setup`: Run setup procedures before starting
* `-y`: Auto-confirm prompts
* `--help`: Show help message and exit

---

## Example

Start an automated migration for a client’s database:

```bash
python MoveSync.py --database=mydb --info=clientA --start -y
```

Generate reports after migration:

```bash
python MoveSync.py --database=mydb --reports
```

Run a manual migration setup:

```bash
python MoveSync.py --database=mydb --setup --startmanual
```

---

## Project Structure

```
movesync/
├── requirements.txt           # Python dependencies
├── README.md                  # Project documentation
├── db_info.py                 # Python file to read/handle DB info
├── migrate_postgres.sh        # Shell script for handling migration logic
├── MoveSync.py                # Core logic for performing the migration
├── db_info_json/              # Folder containing DB configuration
│   └── postgres_info.json     # JSON config with PostgreSQL DB info
```

---

## Future Plans

* Add support for MySQL, SQLite, and MSSQL
* Built-in logging and error recovery
* Dry-run and validation modes
* Web interface (optional)

---

Let me know if you'd like to include sample output, error handling notes, or contribution guidelines.
