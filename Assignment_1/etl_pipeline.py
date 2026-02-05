import os 
from datetime import datetime, date
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def extract_data():
    csv_df = pd.read_csv(
        "source_data.csv",
        sep=",",
        encoding="utf-8-sig"
    )
    excel_df = pd.read_excel("source_data.xlsx")
    csv_df.columns = csv_df.columns.str.strip().str.upper()
    excel_df.columns = excel_df.columns.str.strip().str.upper()
    return csv_df, excel_df

def standardize_gender(gender):
    if pd.isna(gender):
        return
    gender = str(gender).strip().lower()
    if gender in ["male", "m"]:
        return "M"
    elif gender in ["female", "f"]:
        return "F"
    return "O"

def create_raw_layer(csv_df, excel_df):
    raw_df = pd.concat([csv_df, excel_df], ignore_index=True)
    raw_df["GENDER"] = raw_df["GENDER"].apply(standardize_gender)
    raw_df["DOB"] = pd.to_datetime(raw_df["DOB"], errors="coerce").dt.strftime("%d-%m-%Y")
    raw_df["LOAD_TIMESTAMP"] = datetime.now()
    return raw_df

def calculate_age(dob_series):
    dob = pd.to_datetime(dob_series, format="%d-%m-%Y", errors="coerce")
    today = pd.Timestamp(date.today())
    return (today - dob).dt.days // 365

def create_final_layer(csv_df, excel_df):
    merged_df = pd.merge(
        csv_df,
        excel_df,
        on="USER_ID",
        how="inner",
        suffixes=("_csv", "_xlsx")
    )
    merged_df["AGE"] = calculate_age(merged_df["DOB_csv"])
    final_df = merged_df[merged_df["AGE"] > 18]
    return final_df

def load_to_snowflake(df, table_name, conn):
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True
    )
    print(f"Loaded {nrows} rows into {table_name}")

def main():
    print("Starting ETL Pipeline...")

    csv_df, excel_df = extract_data()
    raw_df = create_raw_layer(csv_df, excel_df)
    final_df = create_final_layer(csv_df, excel_df)

    conn = get_snowflake_connection()
    try:
        load_to_snowflake(raw_df, "RAW_USER_DATA", conn)
        load_to_snowflake(final_df, "FINAL_USER_DATA", conn)
    finally:
        conn.close()

    print("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    main()

