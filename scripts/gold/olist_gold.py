import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round, avg, count, when, lit, year, month

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver_base_path = "s3://olist-data-project/silver/parquet/"
gold_output_path = "s3://olist-data-project/gold/parquet/"

# Função para ler dados diretamente do S3
def read_data_from_s3(path):
    try:
        print(f"Lendo dados do caminho {path}")
        return spark.read.parquet(path)
    except Exception as e:
        print(f"Erro ao ler os dados do caminho {path}: {e}")
        raise

df_customers = read_data_from_s3(f"{silver_base_path}olist_customers_cleaned")
df_orders = read_data_from_s3(f"{silver_base_path}olist_orders_cleaned")
df_order_items = read_data_from_s3(f"{silver_base_path}olist_order_items_cleaned")
df_products = read_data_from_s3(f"{silver_base_path}olist_products_cleaned")
df_reviews = read_data_from_s3(f"{silver_base_path}olist_reviews_cleaned")
df_sellers = read_data_from_s3(f"{silver_base_path}olist_sellers_cleaned")
df_payments = read_data_from_s3(f"{silver_base_path}olist_payments_cleaned")

def write_data(df, path, partition_cols=None):
    if partition_cols:
        df.write.mode("overwrite").partitionBy(partition_cols).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)

# Adicionando colunas year e month na tabela df_orders
df_orders = df_orders.withColumn("year", year(col("order_purchase_timestamp")).cast("string")) \
                     .withColumn("month", month(col("order_purchase_timestamp")).cast("string"))

# 1. Frete Médio por Região, Estado e Cidade
df_frete_medio = (
    df_order_items
    .join(df_orders, "order_id")
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "year", "month")
    .agg(round(avg("freight_cost"), 2).alias("frete_medio"))
)
write_data(df_frete_medio, f"{gold_output_path}/frete_medio", partition_cols=["year", "month"])

# 2. Categorias mais vendidas por Região, Estado e Cidade
df_categorias_vendidas = (
    df_order_items
    .join(df_products, "product_id")
    .join(df_orders, "order_id")
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "product_category_name", "year", "month")
    .agg(count("product_id").alias("total_vendas"))
    .orderBy(col("total_vendas").desc())
)
write_data(df_categorias_vendidas, f"{gold_output_path}/categorias_vendidas", partition_cols=["year", "month"])

# 3. Ticket Médio por Localidade
df_ticket_medio = (
    df_orders
    .join(df_order_items, "order_id")
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "year", "month")
    .agg(round(avg("price"), 2).alias("ticket_medio"))
)
write_data(df_ticket_medio, f"{gold_output_path}/ticket_medio", partition_cols=["year", "month"])

# 4. Avaliações por Produto e Localidade
df_avaliacoes = (
    df_reviews
    .join(df_orders, "order_id")
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "review_score", "year", "month")
    .agg(count("review_id").alias("total_avaliacoes"))
)
write_data(df_avaliacoes, f"{gold_output_path}/avaliacoes", partition_cols=["year", "month"])

# 5. Número de vendas por Região, Estado, Cidade, Ano e Mês
df_numero_vendas = (
    df_orders
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "year", "month")
    .agg(count("order_id").alias("numero_vendas"))
)
write_data(df_numero_vendas, f"{gold_output_path}/numero_vendas", partition_cols=["year", "month"])

# 6. Métodos de Pagamento por Localidade
df_metodos_pagamento = (
    df_payments
    .join(df_orders, "order_id")
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "payment_method", "year", "month")
    .agg(count("order_id").alias("payment_count"))
    .orderBy(col("payment_count").desc())
)
write_data(df_metodos_pagamento, f"{gold_output_path}/metodos_pagamento", partition_cols=["year", "month"])

# 7. Pontualidade de Entregas por Localidade
df_pontualidade = (
    df_orders
    .join(df_customers, "customer_id")
    .groupBy("region", "state", "city", "year", "month")
    .agg(
        count(when(col("order_status") == "delivered", True)).alias("entregas_no_prazo"),
        count(when(col("order_status") == "late_delivery", True)).alias("entregas_atrasadas"),
        count(when(col("order_status") == "early_delivery", True)).alias("entregas_antes_do_prazo"),
        count("order_id").alias("total_entregas")
    )
    .withColumn("percentual_no_prazo", round((col("entregas_no_prazo") / col("total_entregas")) * 100, 2))
    .withColumn("percentual_atrasadas", round((col("entregas_atrasadas") / col("total_entregas")) * 100, 2))
    .withColumn("percentual_antes_do_prazo", round((col("entregas_antes_do_prazo") / col("total_entregas")) * 100, 2))
)
write_data(df_pontualidade, f"{gold_output_path}/pontualidade", partition_cols=["year", "month"])

job.commit()
print("### Todos os insights gerados e salvos na camada Gold ###")
