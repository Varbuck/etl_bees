"""
### Spark Submit Example DAG
Esta DAG demonstra como submeter um job Spark usando o SparkSubmitOperator.
"""

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Definir argumentos padrão
default_args = {
    "owner": "airflow",
    "retries": 0,
}

# Instanciar a DAG
with DAG(
    dag_id="ppl_brewery",
    default_args=default_args,
    description="DAG de exemplo para submeter um job Spark",
    schedule_interval=None,  # Desativando agendamento automático
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["brewerys", "spark"],
) as dag:
    dag.doc_md = __doc__


    # Raw
    ingestao_raw = SparkSubmitOperator(
        task_id="ingestao_raw",
        conn_id="spark-con",
        application='/opt/bitnami/spark/jobs/raw.py',
        name="ingestao_raw",
        verbose=True,
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1",
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "SparkSubmitExample"
        }
    )
    
    # Silver
    ingestao_silver = SparkSubmitOperator(
        task_id="ingestao_silver",
        conn_id="spark-con",
        application='/opt/bitnami/spark/jobs/silver-ingestion.py',
        name="ingestao_silver",
        verbose=True,
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1",
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "SparkSubmitExample"
        }
    )

    validacao_silver = SparkSubmitOperator(
        task_id="validacao_silver",
        conn_id="spark-con",
        application='/opt/bitnami/spark/jobs/silver-validation.py',
        name="validacao_silver",
        verbose=True,
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1",
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "SparkSubmitExample"
        }
    )

    # Gold
    agregacao_gold = SparkSubmitOperator(
        task_id="agregacao_gold",
        conn_id="spark-con",
        application='/opt/bitnami/spark/jobs/gold.py',
        name="agregacao_gold",
        verbose=True,
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1",
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "SparkSubmitExample"
        }
    )

    # Definir a ordem das tarefas
    ingestao_raw >> ingestao_silver >> validacao_silver >> agregacao_gold
