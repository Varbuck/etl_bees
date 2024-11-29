# Importação de bibliotecas essenciais para o processamento e logging
import logging
from delta import *
import pyspark
import func as dt

# Configuração básica de logging para monitoramento do processo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurando Spark com suporte a Delta Lake
try:
    builder = pyspark.sql.SparkSession.builder.appName("Validacao - Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
except Exception as e:
    logging.error(f"Erro ao configurar Spark. Detalhes: {str(e)}")
    raise

# Colunas-chave para validação de dados
key_columns = ["id"]

# Tentativa de leitura dos dados da camada silver
try:
    df_silver = spark.read.format("delta").load('/datalake/silver/breweries/')
except Exception as e:
    logging.error(f"Erro ao tentar ler dados da camada Silver. Detalhes: {str(e)}")
    raise

# Função para rodar verificações de qualidade de dados
def executar_validacoes_dados(df):
    resultados = {
        "dataframe_vazio": dt.check_empty_df(df),  # Verifica se o DataFrame está vazio
        "linhas_unicas": dt.check_only_unique_rows(df, key_columns)  # Verifica se as linhas são únicas
    }
    
    if all(resultados.values()):
        logging.info("Todas as validações de qualidade de dados passaram com sucesso.")
    else:
        logging.error("As validações de qualidade de dados falharam.")
        raise

# Chamando a função de validação
executar_validacoes_dados(df_silver)
