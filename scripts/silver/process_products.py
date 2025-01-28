from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

# Importa funções compartilhadas
from utils.shared_functions import read_data, write_data

def process_products(glueContext):
    # Lê dados da camada Bronze
    df_products = read_data(glueContext, "olist_project_data_bronze", "bronze_olist_products_dataset_csv")

    # Tratamentos e enriquecimentos
    df_products_cleaned = df_products.dropDuplicates(["product_id"]).drop("product_photos_qty") \
        .fillna({
            "product_weight_g": 0,
            "product_length_cm": 0,
            "product_height_cm": 0,
            "product_width_cm": 0
        }).withColumn(
            "product_weight_g", when(col("product_weight_g") < 0, 0).otherwise(col("product_weight_g"))
        ).withColumn(
            "product_volume_cm3",
            round(col("product_length_cm") * col("product_height_cm") * col("product_width_cm"), 2)
        ).withColumn(
            "product_size_category",
            when(col("product_volume_cm3") <= 1000, "Small")
            .when((col("product_volume_cm3") > 1000) & (col("product_volume_cm3") <= 10000), "Medium")
            .when(col("product_volume_cm3") > 10000, "Large")
            .otherwise("Unknown")
        )

    # Seleciona e ordena colunas
    ordered_columns = [
        "product_id", "product_category_name", "product_weight_g",
        "product_volume_cm3", "product_size_category"
    ]
    df_products_cleaned = df_products_cleaned.select(*ordered_columns)

    # Escreve na camada Silver
    output_path = "s3://olist-data-project/silver/parquet/olist_products_cleaned"
    write_data(df_products_cleaned, output_path, partitionKeys=["product_category_name"])

# Configuração inicial do Glue Context
glueContext = GlueContext(SparkSession.builder.getOrCreate())
process_products(glueContext)
