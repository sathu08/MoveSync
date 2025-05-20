from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession
import os
import pandas as pd
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from tabulate import tabulate
import json

# SQLAlchemy connection manager using context manager
@contextmanager
def get_db_connection(database_url: str):
    """Context manager that yields a SQLAlchemy session."""
    # engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=database_url)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

# Helper function to execute a query and return results
def execute_query(conn: SQLAlchemySession, query: str):
    """Executes a SQL query and returns the results."""
    try:
        result = conn.execute(text(query)).fetchall()
        return result
    except Exception as e:
        raise Exception(f"Error in execute file: {e}")
        

# Function to find database objects (tables, views, etc.)
def find_objects(query: str, types_name: str, database_url: str):
    """Finds specific database objects (e.g., tables, views)."""
    with get_db_connection(database_url) as conn:
        if conn is None:
            print(f"Unable to connect to the database for {types_name}.")
            return []
        result = execute_query(conn, query)
        return result if result else f"{types_name} not found."

# Save results to both Excel and text files
def save_results_to_file(filename: str, types_name: str, results):
    """Saves the results of a query to both an Excel and a text file."""
    try:
        df = pd.DataFrame(results) if isinstance(results, list) else pd.DataFrame([results])
        with pd.ExcelWriter(f"{filename}.xlsx", mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=types_name, index=False)

        # headers_ = [""] * len(df.columns) if df.columns.tolist() == [0] else ([""] * len(df.columns) if not df.columns.tolist() else df.columns.tolist())
        if df.columns.tolist() == [0] or not df.columns.tolist():
            df.columns = [f"" for i in range(df.shape[1])]
        with open(f"{filename}.txt", "a") as file:
            file.write(f"\n{types_name}:\n")
            file.write(f"{'=' * 20}\n")
            file.write(f"Total rows: {len(df)}\n")
            file.write(tabulate(df, headers="keys", tablefmt="grid"))
            file.write("\n\n")
    except Exception as e:
        raise Exception(f"Error saving results to file: {e}")
        

# Main function to find database objects and save results
def fetch_db_info(database_url: str, database_name: str, client: str):
    """
    Fetches database information and saves it to a file.
    Args:
        database_url (str): The database URL.
        database_name (str): The name of the database.
        client (str): The client type (e.g., source or target).
    """
    # Create output directory if it doesn't exist
    os.makedirs('./output_folder', exist_ok=True)
    output_file = f"output_folder/output_{client}_{database_name}"
    
    # Clear old output files
    open(f'{output_file}.txt', "w").close()
    if os.path.exists(f"{output_file}.xlsx"):
        os.remove(f"{output_file}.xlsx")
    
    # Create a basic Excel file as a placeholder
    pd.DataFrame(["MoveSync"]).to_excel(f"{output_file}.xlsx", index=False, sheet_name="MoveSync")
    # Load query definitions
    current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    json_path = os.path.join(current_dir, f'db_info_json/{database_name}_info.json')
    with open(json_path, 'r') as file:
        queries = json.load(file)

    # Execute each query in parallel
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(find_objects, query, types_name, database_url): types_name
            for types_name, query in queries.items()
        }

        for future in as_completed(futures):
            types_name = futures[future]
            try:
                result = future.result()
                if result:  # Check if result is not None or empty
                    save_results_to_file(output_file, types_name, result)
                else:
                    print(f"[WARN] No result returned for '{types_name}'")
            except Exception as e:
                raise Exception(f"[ERROR] Query '{types_name}' failed: {e}")

    print(f"[DONE] Excel and Text File saved at {os.path.abspath(output_file)}")

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
    return result

def compare_row_counts(source_engine: str, target_engine: str, max_workers=10):
    """
    Compare row counts between source and target PostgreSQL databases and save results to Excel.
    """
    # Create output directory if it doesn't exist
    os.makedirs('./output_folder', exist_ok=True)
    output_file = os.path.join("output_folder", "reports")

    # Clear old output files
    open(f"{output_file}.txt", "w").close()
    if os.path.exists(f"{output_file}.xlsx"):
        os.remove(f"{output_file}.xlsx")

    # Create a basic Excel file as a placeholder
    pd.DataFrame(["MoveSync"]).to_excel(f"{output_file}.xlsx", index=False, sheet_name="MoveSync")

    # Fetch table lists
    source_tables = set(get_tables(source_engine))
    target_tables = set(get_tables(target_engine))
    all_tables = sorted(source_tables.union(target_tables))
    total_tables = len(all_tables)
    print(f"Total unique tables (source + target): {total_tables}")

    rows = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for schema, table in all_tables:
            futures.append(executor.submit(count_rows, schema, table, source_engine, "source"))
            futures.append(executor.submit(count_rows, schema, table, target_engine, "target"))

        for i, future in enumerate(as_completed(futures), start=1):
            print(f"[{i}/{len(futures)}] Started: {key[0]}.{key[1]}")
            result = future.result()
            key = (result["schema_name"], result["table_name"])
            if key not in rows:
                rows[key] = {
                    "schema_name": key[0],
                    "table_name": key[1],
                    "estimated_rows_source": None,
                    "estimated_rows_target": None,
                    "source_error": None,
                    "target_error": None
                }
            rows[key].update(result)
            print(f"[{i}/{len(futures)}] Processed: {key[0]}.{key[1]}")

    # Main comparison
    merged_df = pd.DataFrame(rows.values())
    merged_df["row_count_match"] = (
        merged_df["estimated_rows_source"] == merged_df["estimated_rows_target"]
    )

    # Identify missing tables
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

    # Save results using your custom function
    save_results_to_file(filename=output_file, types_name="RowCountComparison", results=merged_df.to_dict(orient='records'))
    save_results_to_file(filename=output_file, types_name="MissingInSource", results=df_missing_source.to_dict(orient='records'))
    save_results_to_file(filename=output_file, types_name="MissingInTarget", results=df_missing_target.to_dict(orient='records'))

    print(f"[DONE] Excel and Text File saved at {os.path.abspath(output_file)}")
