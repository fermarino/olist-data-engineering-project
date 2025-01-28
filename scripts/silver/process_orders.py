from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, floor, year, month

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def process_orders(glueContext):
    # Lê dados da camada Bronze
    df_orders = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_orders_dataset_csv")

    # Tratamentos e enriquecimentos
    df_orders_cleaned = df_orders.dropna(subset=["order_id", "customer_id", "order_purchase_timestamp"]) \
        .withColumn(
            "approval_time_minutes",
            when(col("order_approved_at").isNotNull(),
                 floor((unix_timestamp(col("order_approved_at")) - unix_timestamp(col("order_purchase_timestamp"))) / 60))
        ).withColumn(
            "year", year(col("order_purchase_timestamp"))
        ).withColumn(
            "month", month(col("order_purchase_timestamp"))
        )

    # Seleciona e ordena colunas
    ordered_columns = [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
        "approval_time_minutes", "year", "month"
    ]
    df_orders_cleaned = df_orders_cleaned.select(*ordered_columns)

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_orders_cleaned"
    write_data(df_orders_cleaned, output_path, partitionKeys=["year", "month"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_orders(glueContext)
