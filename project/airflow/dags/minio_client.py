from minio import Minio
from config import MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, MINIO_BUCKET

def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

def upload_to_minio(object_name, file_path):
    client = get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    client.fput_object(MINIO_BUCKET, object_name, file_path)
    print(f"Arquivo '{file_path}' enviado para o bucket '{MINIO_BUCKET}' como '{object_name}'.")
