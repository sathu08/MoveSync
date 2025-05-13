#!/usr/bin/env python3
"""
Migration Tool - Transfer data between source and target databases.

Usage:
  MoveSync.py [--database=<dbname>] [--info=<client>] [--start] [--help] [-y] [--reports] [--setup] [--startmanual]

Options:
  --database=<dbname>    Name of the database to migrate [default: postgres].
  --info=<client>        Fetch and store database info (source, target, or both).
  --start                Start the migration process interactively.
  --help                 Show this help message and exit.
  -y                     Automatically confirm the migration without prompting.
  --reports              Generate reports for the migration process.
  --setup                Create a configuration file for the database connection.
  --startmanual         Start the migration process with a manual dump file.
"""

import subprocess
from docopt import docopt
import json
from sqlalchemy import create_engine
import urllib.parse
import os
from db_info import fetch_db_info, compare_row_counts

def fetch_db_credentials(credentials_json: str):
    """Loads source and target DB credentials from a JSON file."""
    try:
        with open(credentials_json, 'r') as file:
            credentials = json.load(file)
        return {
            "source": credentials["source"],
            "target": credentials["target"]
        }
    except Exception as e:
        raise Exception(f"Error loading DB credentials: {e}")

def connect_to_db(user, password, host, port, database):
    """Creates a SQLAlchemy engine using PostgreSQL connection string."""
    try:
        engine = create_engine(
            f"postgresql://{user}:{urllib.parse.quote_plus(password)}@{host}:{port}/{database}"
        )
        return engine
    except Exception as e:
        raise Exception(f"Error connecting to database'{database}': {e}")

def setup_connection():
    """Loads credentials and sets up both source and target DB connections."""
    try:
        print("Loading DB credentials...")
        if not os.path.exists("db_config.json"):
            raise FileNotFoundError("Credentials file not found. Please create 'db_config.json' using --setup.")
        credentials = fetch_db_credentials("db_config.json")
        source_engine = connect_to_db(**credentials["source"])
        target_engine = connect_to_db(**credentials["target"])
        return credentials, source_engine, target_engine
    except Exception as e:
        raise Exception(f"Error during setup_connection: {e}")

def start_migration(database_name: str, auto_confirm: bool = False):
    """Starts the migration process by connecting to the source and target databases."""
    try:
        print("Started to connect to the database...")
        credentials, source_engine, target_engine = setup_connection()

        if not auto_confirm:
            confirm = input("Do you want to start the migration? (y/n): ").strip().lower()
            if confirm != "y":
                print("Migration aborted.")
                return

        print(f"Starting migration for database '{database_name}'...")
        args = [
            "bash", "migrate_postgres.sh",
            credentials["source"]["database"],
            credentials["source"]["user"],
            credentials["source"]["password"],
            credentials["source"]["host"],
            str(credentials["source"]["port"]),
            credentials["target"]["database"],
            credentials["target"]["user"],
            credentials["target"]["password"],
            credentials["target"]["host"],
            str(credentials["target"]["port"]),
            "auto"
        ]
        subprocess.run(args, check=True)
        print("Migration completed successfully.")
    except Exception as e:
        raise Exception(f"Error in start_migration: {e}")
    
def manual_migration(database_name: str, auto_confirm: bool = False):
    """Starts the migration process by connecting to the source and target databases."""
    try:
        print("Started to connect to the database...")
        credentials, source_engine, target_engine = setup_connection()

        if not auto_confirm:
            confirm = input("Do you want to start the migration? (y/n): ").strip().lower()
            if confirm != "y":
                print("Migration aborted.")
                return

        print(f"Starting migration for database '{database_name}'...")
        args = [
            "bash", "migrate_postgres.sh",
            credentials["source"]["database"],
            credentials["source"]["user"],
            credentials["source"]["password"],
            credentials["source"]["host"],
            str(credentials["source"]["port"]),
            credentials["target"]["database"],
            credentials["target"]["user"],
            credentials["target"]["password"],
            credentials["target"]["host"],
            str(credentials["target"]["port"]),
            "manual",
            "./dump/pg_dump_20250513_114630.dump"
        ]
        subprocess.run(args, check=True)
        print("Migration completed successfully.")
    except Exception as e:
        raise Exception(f"Error in start_migration: {e}")

def info(database_name: str, client: str):
    """Fetches and prints DB info for the given client(s)."""
    try:
        print("Connecting to the databases...")
        _, source_engine, target_engine = setup_connection()
        print(f"Fetching info for '{client}' database '{database_name}'...")
        if client == "source":
            fetch_db_info(source_engine, database_name, "source")
        elif client == "target":
            fetch_db_info(target_engine, database_name, "target")
        elif client == "both":
            fetch_db_info(source_engine, database_name, "source")
            fetch_db_info(target_engine, database_name, "target")
        else:
            raise ValueError("Invalid client value. Use 'source', 'target', or 'both'.")
    except Exception as e:
        raise Exception(f"Error in info: {e}")

def reports():
    """Generates comparison reports between source and target databases."""
    try:
        print("Connecting to the databases...")
        _, source_engine, target_engine = setup_connection()
        compare_row_counts(source_engine, target_engine)
    except Exception as e:
        raise Exception(f"Error in reports: {e}")

def write_config_file(filename="db_config.json"):
    """Writes a default DB config file if it doesn't exist."""
    db_config = {
        "database": "database_name",
        "user": "user_name",
        "password": "password",
        "host": "localhost",
        "port": 5432
    }
    config_data = {
        "source": db_config,
        "target": db_config
    }
    with open(filename, "w") as file:
        json.dump(config_data, file, indent=4)
    print(f"Configuration file '{filename}' created successfully.")

if __name__ == "__main__":
    try:
        args = docopt(__doc__)
        if args["--info"]:
            info(database_name=args["--database"], client=args["--info"])
        elif args["--start"]:
            start_migration(database_name=args["--database"], auto_confirm=args["-y"])
        elif args["--startmanual"]:
            manual_migration(database_name=args["--database"], auto_confirm=args["-y"])
        elif args["--reports"]:
            reports()
        elif args["--setup"]:
            write_config_file()
    except Exception as e:
        print(f"Fatal error during execution: {e}")
