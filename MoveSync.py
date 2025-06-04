#!/usr/bin/env python3
"""
Migration Tool - Transfer data between source and target databases.

Usage:
  MoveSync.py [--database=<dbname>] [--info=<client>] [--start] [--help] [-y] [--reports] [--setup] [--startmanual]
"""

import subprocess
from docopt import docopt
import json
from sqlalchemy import create_engine
import urllib.parse
import os
from db_info import fetch_db_info, compare_row_counts, logging_setup

# ---------------------------- Logging Setup ----------------------------
logger = logging_setup("Migration.log")

# ---------------------------- Core Functions ----------------------------

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
        logger.error("Error loading DB credentials: %s", e)
        raise

def connect_to_db(user, password, host, port, database):
    """Creates a SQLAlchemy engine using PostgreSQL connection string."""
    try:
        engine = create_engine(
            f"postgresql://{user}:{urllib.parse.quote_plus(password)}@{host}:{port}/{database}"
        )
        return engine
    except Exception as e:
        logger.error("Error connecting to database '%s': %s", database, e)
        raise

def setup_connection():
    """Loads credentials and sets up both source and target DB connections."""
    try:
        logger.info("Loading DB credentials...")
        if not os.path.exists("db_config.json"):
            raise FileNotFoundError("Credentials file not found. Please create 'db_config.json' using --setup.")
        credentials = fetch_db_credentials("db_config.json")
        source_engine = connect_to_db(**credentials["source"])
        target_engine = connect_to_db(**credentials["target"])
        return credentials, source_engine, target_engine
    except Exception as e:
        logger.error("Error during setup_connection: %s", e)
        raise

def start_migration(database_name: str, auto_confirm: bool = False):
    """Starts the migration process using a shell script."""
    try:
        logger.info("Starting automatic migration process...")
        credentials, source_engine, target_engine = setup_connection()

        if not auto_confirm:
            confirm = input("Do you want to start the migration? (y/n): ").strip().lower()
            if confirm != "y":
                logger.info("Migration aborted by user.")
                return

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
        logger.info("Migration completed successfully.")
    except Exception as e:
        logger.error("Error in start_migration: %s", e)
        raise

def manual_migration(database_name: str, auto_confirm: bool = False):
    """Runs migration using a manual dump file."""
    try:
        logger.info("Starting manual migration process...")
        credentials, source_engine, target_engine = setup_connection()

        if not auto_confirm:
            confirm = input("Do you want to start the migration? (y/n): ").strip().lower()
            if confirm != "y":
                logger.info("Migration aborted by user.")
                return

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
        logger.info("Manual migration completed successfully.")
    except Exception as e:
        logger.error("Error in manual_migration: %s", e)
        raise

def info(database_name: str, client: str):
    """Fetches and logs DB info for the given client(s)."""
    try:
        logger.info("Fetching DB info for client: %s", client)
        _, source_engine, target_engine = setup_connection()

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
        logger.error("Error in info: %s", e)
        raise

def reports():
    """Generates comparison reports."""
    try:
        logger.info("Generating migration reports...")
        _, source_engine, target_engine = setup_connection()
        compare_row_counts(source_engine, target_engine)
    except Exception as e:
        logger.error("Error in reports: %s", e)
        raise

def write_config_file(filename="db_config.json"):
    """Creates a template DB config file."""
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
    logger.info("Configuration file '%s' created successfully.", filename)

# ---------------------------- Entrypoint ----------------------------

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
        logger.critical("Fatal error during execution: %s", e)
