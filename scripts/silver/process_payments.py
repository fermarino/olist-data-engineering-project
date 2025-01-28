from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def process_payments(glueContext):
    # Lê dados da camada Bronze
    df_payments = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_order_payments_dataset_csv")

    # Tratamentos e enriquecimentos
    df_payments_cleaned = df_payments.dropDuplicates(["order_id", "payment_sequential"]) \
        .fillna({
            "payment_type": "unknown",
            "payment_installments": 0,
            "payment_value": 0.0
        }).withColumn(
            "payment_type",
            when(col("payment_type") == "credit_card", "Credit Card")
            .when(col("payment_type") == "boleto", "Boleto")
            .when(col("payment_type") == "voucher", "Voucher")
            .when(col("payment_type") == "debit_card", "Debit Card")
            .otherwise("Other")
        ).withColumn(
            "payment_amount", round(col("payment_value"), 2)
        ).drop("payment_sequential").withColumnRenamed("payment_type", "payment_method")

    # Seleciona e ordena colunas
    ordered_columns = [
        "order_id", "payment_method", "payment_amount", "payment_installments"
    ]
    df_payments_cleaned = df_payments_cleaned.select(*ordered_columns)

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_payments_cleaned"
    write_data(df_payments_cleaned, output_path, partitionKeys=["payment_method"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_payments(glueContext)
