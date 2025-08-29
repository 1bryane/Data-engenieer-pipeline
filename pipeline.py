import argparse
import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def load_environment() -> None:
    """Load variables from .env if present."""
    load_dotenv(override=False)


def build_connection_string() -> str:
    """Build a SQLAlchemy PostgreSQL connection string from environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def create_db_engine() -> Engine:
    connection_string = build_connection_string()
    return create_engine(connection_string, pool_pre_ping=True)


def read_csv_to_dataframe(csv_path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame with basic options suitable for beginners."""
    df = pd.read_csv(
        csv_path,
        encoding="utf-8",
        sep=",",
        engine="python",
        dtype=str,  # read all as string first to avoid parse issues; we will clean later
    )
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Perform simple beginner-friendly cleaning steps.

    - Trim whitespace on string columns
    - Replace obvious empty values with NaN
    - Drop duplicate rows
    """
    if df.empty:
        return df

    df = df.copy()

    # Normalize empty-like values
    df.replace({"": pd.NA, "NA": pd.NA, "N/A": pd.NA, "null": pd.NA, "None": pd.NA}, inplace=True)

    # Trim whitespace in all string-like columns
    for column_name in df.columns:
        if pd.api.types.is_object_dtype(df[column_name]) or pd.api.types.is_string_dtype(df[column_name]):
            df[column_name] = df[column_name].astype("string").str.strip()

    # Drop exact duplicates
    df.drop_duplicates(inplace=True)

    return df


def ensure_table_exists(engine: Engine, table_name: str, sample_df: pd.DataFrame) -> None:
    """Create a table if it does not exist using pandas.to_sql's schema inference.

    Note: For beginners, we lean on pandas to infer types. In production, define schemas explicitly.
    """
    # Create schema if missing (public is default; keep it simple)
    # Create an empty table by writing zero rows if table doesn't exist
    with engine.begin() as conn:
        # Quick existence check
        result = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = :table
                )
                """
            ),
            {"table": table_name},
        )
        exists = result.scalar() is True

    if not exists:
        # Write just the header by taking zero rows; pandas will create the table
        sample_df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists="fail",
            index=False,
        )


def write_dataframe_to_postgres(engine: Engine, df: pd.DataFrame, table_name: str) -> int:
    """Append DataFrame rows to PostgreSQL and return number of rows written."""
    if df.empty:
        return 0

    df.to_sql(name=table_name, con=engine, if_exists="append", index=False, method=None)
    return len(df)


def run_pipeline(csv_path: str, table_name: str) -> int:
    """Run full pipeline: load env, connect DB, read CSV, clean, ensure table, load."""
    load_environment()
    engine = create_db_engine()

    df_raw = read_csv_to_dataframe(csv_path)
    df_clean = clean_dataframe(df_raw)

    ensure_table_exists(engine, table_name, df_clean)
    inserted = write_dataframe_to_postgres(engine, df_clean, table_name)
    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSV -> PostgreSQL simple pipeline")
    parser.add_argument(
        "--csv",
        dest="csv_path",
        type=str,
        default=os.getenv("CSV_PATH", "./data/input.csv"),
        help="Path to the CSV file",
    )
    parser.add_argument(
        "--table",
        dest="table_name",
        type=str,
        default="csv_data",
        help="Target table name in PostgreSQL (default: csv_data)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = run_pipeline(csv_path=args.csv_path, table_name=args.table_name)
    print(f"Inserted {rows} rows into table '{args.table_name}'.")


if __name__ == "__main__":
    main()


