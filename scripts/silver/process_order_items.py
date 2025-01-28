from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def process_order_items(glueContext):
    # Lê dados da camada Bronze
    df_order_items = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_order_items_dataset_csv")

    # Tratamentos e enriquecimentos
    df_order_items_cleaned = df_order_items.dropDuplicates(["order_id", "order_item_id"]) \
        .fillna({
            "price": 0.0,
            "freight_value": 0.0
        }).withColumn(
            "total_value", round(col("price") + col("freight_value"), 2)
        ).withColumnRenamed("order_item_id", "item_id") \
         .withColumnRenamed("shipping_limit_date", "shipping_deadline") \
         .withColumnRenamed("freight_value", "freight_cost")

    # Seleciona e ordena colunas
    ordered_columns = [
        "order_id", "item_id", "product_id", "seller_id", 
        "shipping_deadline", "price", "freight_cost", "total_value"
    ]
    df_order_items_cleaned = df_order_items_cleaned.select(*ordered_columns)

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_order_items_cleaned"
    write_data(df_order_items_cleaned, output_path, partitionKeys=["seller_id"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_order_items(glueContext)
