import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from tabulate import tabulate

# ---------------------- Logging Setup ----------------------
def logging_setup(logger_name: str, logger_filename: str, log_folder: str = "log"):
    os.makedirs(log_folder, exist_ok=True)
    log_path = os.path.join(log_folder, logger_filename)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler(log_path)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    return logger

logger = logging_setup("migration","migration.log")

# -----------------------------------------------------------

@contextmanager
def get_db_connection(database_url: str):
    """Context manager that yields a SQLAlchemy session."""
    SessionLocal = sessionmaker(bind=database_url)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

def execute_query(conn: SQLAlchemySession, query: str):
    """Executes a SQL query and returns the results."""
    try:
        result = conn.execute(text(query)).fetchall()
        return result
    except Exception as e:
        logger.error(f"Error executing query: {query} | Error: {e}")
        raise

def find_objects(query: str, types_name: str, database_url: str):
    """Finds specific database objects (e.g., tables, views)."""
    with get_db_connection(database_url) as conn:
        if conn is None:
            logger.warning(f"Unable to connect to the database for {types_name}.")
            return []
        result = execute_query(conn, query)
        return result if result else f"{types_name} not found."

def save_results_to_file(filename: str, types_name: str, results):
    """Saves the results of a query to both an Excel and a text file."""
    try:
        df = pd.DataFrame(results) if isinstance(results, list) else pd.DataFrame([results])
        with pd.ExcelWriter(f"{filename}.xlsx", mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=types_name, index=False)

        if df.columns.tolist() == [0] or not df.columns.tolist():
            df.columns = [f"" for _ in range(df.shape[1])]

        with open(f"{filename}.txt", "a") as file:
            file.write(f"\n{types_name}:\n")
            file.write(f"{'=' * 20}\n")
            file.write(f"Total rows: {len(df)}\n")
            file.write(tabulate(df, headers="keys", tablefmt="grid"))
            file.write("\n\n")

        logger.info(f"Saved results for '{types_name}' to file.")
    except Exception as e:
        logger.error(f"Error saving results for {types_name}: {e}")
        raise

def fetch_db_info(database_url: str, database_name: str, client: str):
    os.makedirs('./output_folder', exist_ok=True)
    output_file = f"output_folder/output_{client}_{database_name}"

    open(f'{output_file}.txt', "w").close()
    if os.path.exists(f"{output_file}.xlsx"):
        os.remove(f"{output_file}.xlsx")

    pd.DataFrame(["MoveSync"]).to_excel(f"{output_file}.xlsx", index=False, sheet_name="MoveSync")

    current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    json_path = os.path.join(current_dir, f'db_info_json/{database_name}_info.json')

    try:
        with open(json_path, 'r') as file:
            queries = json.load(file)
    except Exception as e:
        logger.error(f"Failed to load JSON query definitions: {e}")
        raise

    logger.info(f"Starting fetch_db_info for {client} - {database_name}")
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(find_objects, query, types_name, database_url): types_name
            for types_name, query in queries.items()
        }

        for future in as_completed(futures):
            types_name = futures[future]
            try:
                result = future.result()
                if result:
                    save_results_to_file(output_file, types_name, result)
                else:
                    logger.warning(f"No result returned for '{types_name}'")
            except Exception as e:
                logger.error(f"Query '{types_name}' failed: {e}")

    logger.info(f"[DONE] Output saved at {os.path.abspath(output_file)}")

def get_tables(engine):
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT schemaname, relname
            FROM pg_stat_user_tables
            ORDER BY schemaname, relname;
        """)).fetchall()

def count_rows(schema, table, engine, side):
    result = {
        "schema_name": schema,
        "table_name": table,
        f"estimated_rows_{side}": None,
        f"{side}_error": None
    }
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            query = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            result[f"estimated_rows_{side}"] = conn.execute(query).scalar()
    except Exception as e:
        result[f"{side}_error"] = str(e)
        logger.error(f"{side.upper()} count error for {schema}.{table}: {e}")
    return result

def compare_row_counts(source_engine: str, target_engine: str, max_workers=10):
    """
    Compare row counts between source and target PostgreSQL databases and save results to Excel.
    """
    os.makedirs('./output_folder', exist_ok=True)
    output_file = os.path.join("output_folder", "reports")

    open(f"{output_file}.txt", "w").close()
    if os.path.exists(f"{output_file}.xlsx"):
        os.remove(f"{output_file}.xlsx")

    pd.DataFrame(["MoveSync"]).to_excel(f"{output_file}.xlsx", index=False, sheet_name="MoveSync")

    source_tables = set(get_tables(source_engine))
    target_tables = set(get_tables(target_engine))
    all_tables = sorted(source_tables.union(target_tables))
    total_tables = len(all_tables)
    logger.info(f"Total unique tables (source + target): {total_tables}")

    rows = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for schema, table in all_tables:
            futures[executor.submit(count_rows, schema, table, source_engine, "source")] = (schema, table, "source")
            futures[executor.submit(count_rows, schema, table, target_engine, "target")] = (schema, table, "target")

        for i, future in enumerate(as_completed(futures), start=1):
            schema, table, side = futures[future]
            logger.info(f"[{i}/{len(futures)}] Started: {schema}.{table} ({side})")
            try:
                result = future.result()
                key = (schema, table)
                if key not in rows:
                    rows[key] = {
                        "schema_name": schema,
                        "table_name": table,
                        "estimated_rows_source": None,
                        "estimated_rows_target": None,
                        "source_error": None,
                        "target_error": None
                    }
                rows[key].update(result)
                logger.info(f"[{i}/{len(futures)}] Completed: {schema}.{table} ({side})")
            except Exception as e:
                logger.error(f"Failed processing {schema}.{table} ({side}): {e}")

    merged_df = pd.DataFrame(rows.values())
    merged_df["row_count_match"] = (
        merged_df["estimated_rows_source"] == merged_df["estimated_rows_target"]
    )

    source_df = merged_df[merged_df["estimated_rows_source"].notna()]
    target_df = merged_df[merged_df["estimated_rows_target"].notna()]

    source_set = {
        (row["schema_name"], row["table_name"], row["estimated_rows_source"])
        for _, row in source_df.iterrows()
    }
    target_set = {
        (row["schema_name"], row["table_name"], row["estimated_rows_target"])
        for _, row in target_df.iterrows()
    }

    missing_in_source = target_set - source_set
    missing_in_target = source_set - target_set

    df_missing_source = pd.DataFrame(
        list(missing_in_source), columns=["schema_name", "table_name", "estimated_rows"]
    ) if missing_in_source else pd.DataFrame(columns=["schema_name", "table_name", "estimated_rows"])

    df_missing_target = pd.DataFrame(
        list(missing_in_target), columns=["schema_name", "table_name", "estimated_rows"]
    ) if missing_in_target else pd.DataFrame(columns=["schema_name", "table_name", "estimated_rows"])

    # Save results
    save_results_to_file(filename=output_file, types_name="RowCountComparison", results=merged_df.to_dict(orient='records'))
    save_results_to_file(filename=output_file, types_name="MissingInSource", results=df_missing_source.to_dict(orient='records'))
    save_results_to_file(filename=output_file, types_name="MissingInTarget", results=df_missing_target.to_dict(orient='records'))

    logger.info(f"[DONE] Excel and Text File saved at {os.path.abspath(output_file)}")
