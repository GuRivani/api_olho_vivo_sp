import requests
import json
import tempfile
from datetime import datetime
from config import API_BASE_URL, TOKEN, HEADERS

def authenticate():
    session = requests.Session()
    session.post(f'{API_BASE_URL}/Login/Autenticar?token={TOKEN}', headers=HEADERS)
    return session

def fetch_data(session, endpoint):
    response = session.get(f'{API_BASE_URL}/{endpoint}')
    return response.json() if response.status_code == 200 else None

def save_json_temp(data):
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as tmpfile:
        json.dump(data, tmpfile, ensure_ascii=False)
        return tmpfile.name