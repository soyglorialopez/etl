import boto3
import pandas as pd
import logging


from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_dimension_table(df, columns, renamed_columns, id_column):
    table = (
        df[columns]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns=renamed_columns)
        .assign(**{id_column: lambda x: x.index + 1})
    )
    return table


# Task 1: Extract data from S3
def extract_from_s3():
    logger.info("Extracting data from S3...")
    
    # try:
    #     s3 = boto3.client("s3")
    #     bucket_name = "retail-transaction-data-project"
    #     file_key = "raw/Retail_Transactions_Dataset.csv"
    #     response = s3.get_object(Bucket=bucket_name, Key=file_key)
    #     data = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))
    # except Exception as e:
    #     logger.error(f"Error extracting data from S3: {e}")
    #     raise
    file_path = "~/data/Retail_Transactions_Dataset.csv"  # Update with your file path
    data = pd.read_csv(file_path)
    transform_data(data)


# Task 2: Transform data
def transform_data(df):
    logger.info("Transforming data: Creating dimension tables...")
    
    # Date Table
    df["Date"] = pd.to_datetime(df["Date"])

    dim_date = create_dimension_table(df, ["Date"], {"Date": "date"}, "date_id").assign(
        date_id=lambda x: x.index + 1,
        year=lambda x: x.date.dt.year,
        quarter=lambda x: x.date.dt.quarter,
        month=lambda x: x.date.dt.month,
        month_name=lambda x: x.date.dt.strftime("%B"),
        day=lambda x: x.date.dt.day,
        day_name=lambda x: x.date.dt.strftime("%A"),
        week_day=lambda x: x.date.dt.dayofweek,
    )

    # Customer Table
    dim_customer = create_dimension_table(
        df,
        ["Customer_Name", "Customer_Category"],
        {"Customer_Name": "customer_name", "Customer_Category": "customer_category"},
        "customer_id",
    ).assign(customer_id=lambda x: x.index + 1)

    
    # Season Table
    dim_season = create_dimension_table(
        df, ["Season"], {"Season": "season"}, "season_id"
    )

    # Store Table
    dim_store = create_dimension_table(
        df,
        ["Store_Type", "City"],
        {"Store_Type": "store_type", "City": "city"},
        "store_id",
    )

    # Payment Method Table
    dim_payment_method = create_dimension_table(
        df,
        ["Payment_Method"],
        {"Payment_Method": "payment_method"},
        "payment_method_id"
    )

    # Promotion Table
    dim_promotion = create_dimension_table(
        df, ["Promotion"], {"Promotion": "promotion"}, "promotion_id"
    ).dropna()

    # Product Table
    dim_product = create_dimension_table(
        df, ["Product"], {"Product": "product"}, "product_id"
    )

    # Fact Table
    fact_sales = (
        df.merge(dim_date, left_on="Date", right_on="date")
        .merge(dim_season, left_on="Season", right_on="season")
        .merge(dim_product, left_on="Product", right_on="product")
        .merge(
            dim_customer,
            left_on=["Customer_Name", "Customer_Category"],
            right_on=["customer_name", "customer_category"],
        )
        .merge(dim_payment_method, left_on="Payment_Method", right_on="payment_method")
        .merge(dim_promotion, left_on="Promotion", right_on="promotion", how="left")
        .merge(
            dim_store, left_on=["Store_Type", "City"], right_on=["store_type", "city"]
        )
    )

    fact_sales = fact_sales[[
        "Transaction_ID", "date_id", "customer_id", "store_id", "product_id",
        "payment_method_id", "Total_Items", "Total_Cost", "Discount_Applied"
    ]].rename(columns={
        "Transaction_ID": "transaction_id",
        "Total_Items": "total_items",
        "Total_Cost": "total_cost",
        "Discount_Applied": "discount_applied"
    })

    upload_data(
        dim_customer, dim_date,
        dim_product, dim_payment_method,
        dim_season, dim_promotion,
        dim_store, fact_sales,
    )


# Task 3: Upload data
def upload_data(
    dim_customer,
    dim_date,
    dim_product,
    dim_payment_method,
    dim_season,
    dim_promotion,
    dim_store,
    fact_sales,
):
    
    # PostgreSQL connection string
    try:
        engine = create_engine(
            "postgresql://postgres:postgres@localhost:5432/retail_dw"
        )
    except SQLAlchemyError as e:
        print(f"Database connection error: {e}")

    # Load data into the dimension tables
    dim_customer.to_sql("dim_customer", engine, if_exists="append", index=False)
    dim_product.to_sql("dim_product", engine, if_exists="append", index=False)
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
    dim_store.to_sql("dim_store", engine, if_exists="append", index=False)
    dim_season.to_sql("dim_season", engine, if_exists="append", index=False)
    dim_promotion.to_sql("dim_promotion", engine, if_exists="append", index=False)
    dim_payment_method.to_sql(
        "dim_payment_method", engine, if_exists="append", index=False
    )

    # Load the fact table
    fact_sales.to_sql("fact_sales", engine, if_exists="append", index=False)
    
    logger.info(f"Fact table created with {len(fact_sales)} rows.")


logger.info("Starting ETL process.")
extract_from_s3()
