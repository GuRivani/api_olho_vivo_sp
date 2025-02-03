from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import tempfile
from minio import Minio
import requests
from confluent_kafka import Producer

# Configurações globais
TOKEN = 'ef901a1e2d90324f90106392d4df1ed341f9cf6c40a3a2eaf4203ff074ce9a29'
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
BROKER = 'kafka-broker:9092'
CLIENT = Minio("minio:9000", access_key="datalake", secret_key="datalake", secure=False)
BUCKET_NAME = "raw"

# Criar sessão global autenticada
SESSION = requests.Session()

def autenticar():
    """Autentica na API e mantém a sessão."""
    req = SESSION.post(f'http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={TOKEN}', headers=HEADERS)
    if req.status_code == 200 and req.json():
        print("Autenticação bem-sucedida!")
    else:
        raise Exception("Falha na autenticação da API SPTrans")

def buscar_linhas(ti):
    linhas = ["8000"]
    cl_values_json = []

    for linha in linhas:
        req = SESSION.get(f'http://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={linha}')
        if req.status_code == 200:
            data = req.json()
            for d in data:
                cl_values_json.append(d["cl"])  # Pegando apenas o código da linha
    
    ti.xcom_push(key="cl_values_json", value=cl_values_json)

def buscar_paradas(ti):
    """Busca as paradas por linha."""
    cl_values_json = ti.xcom_pull(key="cl_values_json", task_ids="buscar_linhas")

    if not cl_values_json:
        print("Nenhuma linha encontrada.")
        return
    
    paradas = []

    for cl in cl_values_json:
        req = SESSION.get(f'http://api.olhovivo.sptrans.com.br/v2.1/Parada/BuscarParadasPorLinha?codigoLinha={cl}')
        if req.status_code == 200:
            data = req.json()
            paradas.extend(data)  
    
    ti.xcom_push(key="parada_values", value=paradas)

def previsao_chegada(ti):
    """Busca previsões de chegada e armazena no MinIO."""
    parada_values = ti.xcom_pull(key="parada_values", task_ids="buscar_paradas")

    if not parada_values:
        print("Nenhuma parada encontrada.")
        return
    
    for parada in parada_values:
        req = SESSION.get(f'http://api.olhovivo.sptrans.com.br/v2.1/Previsao?codigoParada={parada["cp"]}&codigoLinha={parada["cl"]}')
        if req.status_code == 200:
            data = req.json()
            
            with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as tmpfile:
                json.dump(data, tmpfile, ensure_ascii=False)
                tmpfile_path = tmpfile.name

            object_name = f"Previsao/Previsao_{parada['cl']}_{parada['cp']}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
            
            CLIENT.fput_object(BUCKET_NAME, object_name, tmpfile_path)
            print(f"Arquivo enviado para MinIO: {object_name}")

def enviar_kafka(ti):
    """Envia dados para o Kafka."""
    parada_values = ti.xcom_pull(key="parada_values", task_ids="buscar_paradas")

    if not parada_values:
        print("Sem dados para enviar ao Kafka.")
        return
    
    producer = Producer({'bootstrap.servers': BROKER})
    
    for parada in parada_values:
        producer.produce("api.paradaOnibus", json.dumps(parada))
    
    producer.flush()
    print("Dados enviados para o Kafka.")

def enviar_minio(ti):
    """Envia os dados das linhas para o MinIO."""
    cl_values_json = ti.xcom_pull(key="cl_values_json", task_ids="buscar_linhas")

    if not cl_values_json:
        print("Sem dados de linha para enviar ao MinIO.")
        return

    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as tmpfile:
        json.dump(cl_values_json, tmpfile, ensure_ascii=False)
        tmpfile_path = tmpfile.name
    
    object_name = f"Linhas/Linhas_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    
    CLIENT.fput_object(BUCKET_NAME, object_name, tmpfile_path)
    print(f"Arquivo '{object_name}' enviado para MinIO.")

# Definição da DAG
dag = DAG(
    'processamento_dados',
    description='Processamento de dados de linhas e previsões',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 3),
    catchup=False
)

# Definição das tarefas
task_autenticar = PythonOperator(
    task_id='autenticar',
    python_callable=autenticar,
    dag=dag
)

task_buscar_linhas = PythonOperator(
    task_id='buscar_linhas',
    python_callable=buscar_linhas,
    dag=dag
)

task_buscar_paradas = PythonOperator(
    task_id='buscar_paradas',
    python_callable=buscar_paradas,
    dag=dag
)

task_previsao_chegada = PythonOperator(
    task_id='previsao_chegada',
    python_callable=previsao_chegada,
    dag=dag
)

task_enviar_kafka = PythonOperator(
    task_id='enviar_kafka',
    python_callable=enviar_kafka,
    dag=dag
)

task_enviar_minio = PythonOperator(
    task_id='enviar_minio',
    python_callable=enviar_minio,
    dag=dag
)

# Definição da ordem de execução
task_autenticar >> task_buscar_linhas >> task_buscar_paradas >> task_previsao_chegada >> task_enviar_kafka >> task_enviar_minio
