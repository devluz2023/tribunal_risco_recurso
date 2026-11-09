from datetime import datetime, timedelta
import random
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def popular_banco_com_faker():
    fake = Faker('pt_BR')
    conn = psycopg2.connect(
        dbname="tribunal_db",
        user="airflow",
        password="airflow_password",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    
    tipos_processo = ['civel', 'trabalhista', 'tributario']
    riscos = ['baixo', 'medio', 'alto']
    pesos_risco = [0.85, 0.12, 0.03]

    batch = []
    # Inserção de um lote simulado de 5.000 registros por execução da DAG
    for _ in range(5000):
        num_processo = f"PROC-{fake.unique.random_number(digits=6)}-{random.randint(10,99)}"
        cpf = fake.cpf()
        nome = fake.name()
        valor = round(float(np.random.exponential(scale=50000)), 2)
        tempo = int(np.random.poisson(lam=18) + 1)
        tipo = random.choice(tipos_processo)
        recursos = int(np.random.poisson(lam=0.8))
        risco = random.choices(riscos, weights=pesos_risco)[0]

        batch.append((num_processo, cpf, nome, valor, tempo, tipo, recursos, risco))

    cursor.executemany("""
        INSERT INTO processos_juridicos (
            numero_processo, cpf_autor, nome_autor, valor_causa, 
            tempo_tramitacao, tipo_processo, numero_recursos_anteriores, risco_recurso
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (numero_processo) DO NOTHING;
    """, batch)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("5.000 registros gerados via Faker e persistidos no PostgreSQL com sucesso!")

with DAG(
    'popular_processos_faker_dag',
    default_args=default_args,
    description='DAG do Airflow para popular a base PostgreSQL com Faker',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    task_popular_dados = PythonOperator(
        task_id='inserir_dados_faker_postgres',
        python_callable=popular_banco_com_faker,
    )