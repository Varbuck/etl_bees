# Bees
user airflow:admin   
password:admin   
airflow user interface localhost:8081   

se o postgres nao subir alterar as permissões no host 
1. Ajustar Permissões no Host Antes de Subir o Container
No host, corrija o dono e as permissões do diretório antes de iniciar o container:
sudo chown -R 1001:1001 /home/blueshift/testegit/Bees/postgresql_data
sudo chmod -R 775 /home/blueshift/testegit/Bees/postgresql_data


sudo chown -R 1001:1001 /data


/opt/bitnami/airflow/venv/bin/python -c "import airflow.providers.apache.spark.operators.spark_submit"