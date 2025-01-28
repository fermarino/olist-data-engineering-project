from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, countDistinct, round

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def map_region(state_column):
    return (
        when(state_column.isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), "Norte")
        .when(state_column.isin("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"), "Nordeste")
        .when(state_column.isin("DF", "GO", "MS", "MT"), "Centro-Oeste")
        .when(state_column.isin("ES", "MG", "RJ", "SP"), "Sudeste")
        .when(state_column.isin("PR", "RS", "SC"), "Sul")
        .otherwise("Unknown")
    )

def process_customers(glueContext):
    # Lê dados da camada Bronze
    df_customers = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_customers_dataset_csv")
    df_orders = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_orders_dataset_csv")
    df_payments = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_order_payments_dataset_csv")

    # Tratamentos e enriquecimentos
    df_customers_cleaned = df_customers.dropDuplicates(["customer_id"]).fillna({
        "customer_city": "unknown",
        "customer_state": "unknown"
    })

    df_customers_orders = df_customers_cleaned.join(
        df_orders.select("customer_id", "order_id", "order_status"),
        on="customer_id",
        how="left"
    )

    df_customers_orders = df_customers_orders.groupBy(
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    ).agg(
        countDistinct("order_id").alias("total_orders"),
        count(when(col("order_status") == "delivered", True)).alias("delivered_orders")
    )

    df_orders_payments = df_orders.select("order_id", "customer_id").join(
        df_payments.select("order_id", "payment_type", "payment_value"),
        on="order_id",
        how="left"
    )

    df_payments_agg = df_orders_payments.groupBy("customer_id").agg(
        round(avg("payment_value"), 2).alias("average_ticket"),
        count(when(col("payment_type") == "credit_card", True)).alias("payments_credit_card"),
        count(when(col("payment_type") == "boleto", True)).alias("payments_boleto"),
        count(when(col("payment_type") == "voucher", True)).alias("payments_voucher"),
        count(when(col("payment_type") == "debit_card", True)).alias("payments_debit_card")
    )

    df_customers_cleaned = df_customers_orders.join(
        df_payments_agg,
        on="customer_id",
        how="left"
    ).fillna(0).withColumnRenamed("customer_city", "city") \
                .withColumnRenamed("customer_state", "state")

    df_customers_cleaned = df_customers_cleaned.withColumn("region", map_region(col("state")))

    # Seleciona e ordena colunas
    ordered_columns = [
        "customer_id", "customer_unique_id", "city", "state", "region",
        "total_orders", "delivered_orders", "average_ticket", 
        "payments_credit_card", "payments_boleto", "payments_voucher", "payments_debit_card"
    ]
    df_customers_cleaned = df_customers_cleaned.select(*ordered_columns)

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_customers_cleaned"
    write_data(df_customers_cleaned, output_path, partitionKeys=["region", "state"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_customers(glueContext)
