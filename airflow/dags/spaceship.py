from airflow import DAG
import pandas as pd
import pendulum 

from airflow.operators.python import PythonOperator

with DAG(
    'spaceship',
    default_args={'retries':2},
    description="spaceship tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021,1,1,tz="UTC"),
    catchup=False,
    tags=['tutorial', 'ingest']
) as dag:

    def baixar_arquivo():
        url = "https://raw.githubusercontent.com/professortavares/airflow/main/spaceship_train.csv"
        nome = "spaceship"

        df = pd.read_csv(url)
        df.to_parquet(f"./airflow/dags/spaceship.parquet")
        print(df.head())
    
    def contar_passageiros():
        df = pd.read_parquet("./airflow/dags/spaceship.parquet")
        print(df['PassengerId'].nunique())
    
    def contar_passageiros_por_planeta():
        df = pd.read_parquet("./airflow/dags/spaceship.parquet")
        df['c'] = 1
        print(df.groupby(['HomePlanet']).agg(['count'])['c'])
    
    def contar_passageiros_por_desfecho():
        df = pd.read_parquet("./airflow/dags/spaceship.parquet")
        df['c'] = 1
        print(df.groupby(['Transported']).agg(['count'])['c'])
    
    baixar_arquivo_task = PythonOperator(
        task_id = 'baixar_arquivo_task',
        python_callable=baixar_arquivo
    )

    contar_passageiros_task = PythonOperator(
        task_id = 'contar_passageiros_task',
        python_callable=contar_passageiros
    )

    contar_passageiros_por_planeta_task = PythonOperator(
        task_id = 'contar_passageiros_por_planeta_task',
        python_callable=contar_passageiros_por_planeta
    )        

    contar_passageiros_por_desfecho_task = PythonOperator(
        task_id = 'contar_passageiros_por_desfecho_task',
        python_callable=contar_passageiros_por_desfecho
    )  

    baixar_arquivo_task >> [contar_passageiros_task, contar_passageiros_por_planeta_task, contar_passageiros_por_desfecho_task]