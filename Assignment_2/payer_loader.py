import argparse
import logging
import os
from datetime import datetime
from functools import singledispatch

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


TABLE_MAPPING = {
    "anthem": "ANTHEM_TABLE",
    "cigna": "CIGNA_TABLE",
    "manual": "GENERIC_CLAIMS",
}


@singledispatch
def prepare_dataframe(source) -> pd.DataFrame:
    """Prepare a dataframe based on source type."""
    raise ValueError("Unsupported source type provided.")


@prepare_dataframe.register
def _(source: str) -> pd.DataFrame:
    """Prepare dataframe from CSV file."""
    logger.info("Reading CSV file from %s", source)
    df = pd.read_csv(source)
    df.columns = df.columns.str.strip()
    logger.info("Columns detected: %s", df.columns.tolist())
    return df


@prepare_dataframe.register
def _(source: list) -> pd.DataFrame:
    """Prepare dataframe from manual list input."""
    logger.info("Creating DataFrame from manual data")
    df = pd.DataFrame(source)
    df.columns = df.columns.str.strip()
    return df


def transform_dataframe(df: pd.DataFrame, payer: str) -> pd.DataFrame:
    """Apply business transformations to the dataframe."""
    logger.info("Applying data transformations")

    required_columns = [
        "member_id",
        "claim_id",
        "claim_amount",
        "service_date",
        "payer_name",
    ]

    missing_cols = [
        col for col in required_columns if col not in df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Missing required columns: {missing_cols}. "
            f"Columns found: {df.columns.tolist()}"
        )

    df["service_date"] = pd.to_datetime(
        df["service_date"]
    ).dt.date

    if payer == "anthem":
        logger.info("Applying Anthem-specific adjustment (+2%)")
        df["claim_amount"] = df["claim_amount"] * 1.02

    df.columns = df.columns.str.upper()

    return df


class BaseLoader:
    """Abstract base loader class."""

    def load(self, df: pd.DataFrame):
        raise NotImplementedError(
            "Subclasses must implement load method."
        )


class PayerLoader(BaseLoader):
    """Loader implementation for payer-specific tables."""

    def __init__(self, payer: str):
        self.payer = payer
        self.table_name = TABLE_MAPPING[payer]

    def create_table_if_not_exists(self, connection):
        """Create target table if it does not exist."""
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            MEMBER_ID STRING,
            CLAIM_ID STRING,
            CLAIM_AMOUNT FLOAT,
            SERVICE_DATE DATE,
            PAYER_NAME STRING,
            INGESTION_TIMESTAMP TIMESTAMP_NTZ
                DEFAULT CURRENT_TIMESTAMP()
        )
        """

        cursor = connection.cursor()
        try:
            cursor.execute(create_query)
            logger.info(
                "Table %s verified/created successfully.",
                self.table_name,
            )
        finally:
            cursor.close()

    def load(self, df: pd.DataFrame):
        """Load dataframe into Snowflake."""
        logger.info("Connecting to Snowflake...")

        connection = snowflake.connector.connect(
            **SNOWFLAKE_CONFIG
        )

        try:
            cursor = connection.cursor()
            cursor.execute(
                f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}"
            )
            cursor.execute(
                f"USE DATABASE {SNOWFLAKE_CONFIG['database']}"
            )
            cursor.execute(
                f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}"
            )
            cursor.close()

            self.create_table_if_not_exists(connection)

            logger.info(
                "Loading data into %s",
                self.table_name,
            )

            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=df,
                table_name=self.table_name,
            )

            if success:
                logger.info(
                    "Successfully loaded %s rows.",
                    nrows,
                )
            else:
                logger.error("Data load failed.")

        finally:
            connection.close()
            logger.info("Snowflake connection closed.")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Multi-Source Payer Loader",
    )

    parser.add_argument(
        "--source",
        type=str,
        help="Path to source CSV file",
    )

    parser.add_argument(
        "--payer",
        type=str,
        required=True,
        choices=["anthem", "cigna", "manual"],
        help="Payer name",
    )

    return parser.parse_args()


def main():
    """Main execution entry point."""
    args = parse_arguments()

    if args.payer == "manual":
        manual_data = [
            {
                "member_id": "M900",
                "claim_id": "C9001",
                "claim_amount": 500.0,
                "service_date": "2025-02-01",
                "payer_name": "manual",
            }
        ]
        df = prepare_dataframe(manual_data)
    else:
        if not args.source:
            raise ValueError(
                "Source file must be provided for file-based load."
            )
        df = prepare_dataframe(args.source)

    df = transform_dataframe(df, args.payer)

    loader = PayerLoader(args.payer)
    loader.load(df)


if __name__ == "__main__":
    main()
