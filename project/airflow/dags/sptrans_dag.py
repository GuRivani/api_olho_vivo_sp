from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sptrans_client import authenticate, fetch_data, save_json_temp
from kafka_producer import send_to_kafka
from minio_client import upload_to_minio
from config import TOPIC_LINHA, TOPIC_PARADA, TOPIC_PREVISAO
import json

def process_linhas():
    session = authenticate()
    data = fetch_data(session, 'Linha/Buscar?termosBusca=8000')
    if data:
        json_path = save_json_temp(data)
        upload_to_minio(f"Linhas/Linhas_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json", json_path)
        send_to_kafka(TOPIC_LINHA, json.dumps(data))

def process_paradas():
    session = authenticate()
    linhas = fetch_data(session, 'Linha/Buscar?termosBusca=8000')
    if linhas:
        for linha in linhas:
            data = fetch_data(session, f'Parada/BuscarParadasPorLinha?codigoLinha={linha["cl"]}')
            if data:
                json_path = save_json_temp(data)
                upload_to_minio(f"Parada/ParadaPorLinha_{linha['cl']}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json", json_path)
                send_to_kafka(TOPIC_PARADA, json.dumps(data))

def process_previsao():
    session = authenticate()
    linhas = fetch_data(session, 'Linha/Buscar?termosBusca=8000')
    if linhas:
        for linha in linhas:
            paradas = fetch_data(session, f'Parada/BuscarParadasPorLinha?codigoLinha={linha["cl"]}')
            if paradas:
                for parada in paradas:
                    data = fetch_data(session, f'Previsao?codigoParada={parada["cp"]}&codigoLinha={linha["cl"]}')
                    if data:
                        json_path = save_json_temp(data)
                        upload_to_minio(f"Previsao/Previsao_{linha['cl']}_{parada['cp']}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json", json_path)
                        send_to_kafka(TOPIC_PREVISAO, json.dumps(data))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sptrans_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

process_linhas_task = PythonOperator(task_id='process_linhas', python_callable=process_linhas, dag=dag)
process_paradas_task = PythonOperator(task_id='process_paradas', python_callable=process_paradas, dag=dag)
process_previsao_task = PythonOperator(task_id='process_previsao', python_callable=process_previsao, dag=dag)

process_linhas_task >> process_paradas_task >> process_previsao_task