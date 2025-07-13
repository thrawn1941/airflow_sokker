from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import sqlite3
import sys
import pandas as pd
sys.path.append('/opt/airflow')
from helpers.utils import get_auth, last_thursday, get_teamid

# Ścieżki wewnątrz kontenera
DATA_DIR = "/opt/airflow/data"
DB_PATH = "/opt/airflow/db/sokker.db"
SQL_DIR = "/opt/airflow/sql"

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag = DAG(
    'sokker_pipeline',
    schedule_interval=None,
    default_args=default_args,
    description='Pipeline for Sokker API → CSV → SQLite',
)

def fetch_from_api():
    print("🔁 Start fetching data from Sokker API")
    auth = get_auth()
    
    url = r'https://sokker.org/api/auth/login'
    team_id = get_teamid()
    
    players_url = f'https://sokker.org/api/team/{team_id}/player'
    
    with requests.Session() as session:
        login_resp = session.post(url, json = auth)
        print(login_resp.json())
    
        players_data = session.get(players_url).json()
        players = players_data.get('players', [])
      
        players_info = [{ 'id': player['id'],
                          'name': player['info']['name']['name'],
                          'surname': player['info']['name']['surname'],
                          'age': player['info']['characteristics']['age'],
                          'height': player['info']['characteristics']['height'],
                          'country': player['info']['country']['name'],
                          'stamina': player['info']['skills']['stamina'],
                          'pace': player['info']['skills']['pace'],
                          'technique': player['info']['skills']['technique'],
                          'passing': player['info']['skills']['passing'],
                          'keeper': player['info']['skills']['keeper'],
                          'defending': player['info']['skills']['defending'],
                          'playmaking': player['info']['skills']['playmaking'],
                          'striker': player['info']['skills']['striker'],
                          'experience': player['info']['skills']['experience'],
                          'teamwork': player['info']['skills']['teamwork'],
                          'tacticalDiscipline': player['info']['skills']['tacticalDiscipline'],
                          'currency': player['info']['wage']['currency'],
                          'wage': player['info']['wage']['value']
                        } for player in players]
        
        players_df = pd.DataFrame(players_info)
    
    last_thursday_date = last_thursday()
    
    players_df.to_csv(f'{DATA_DIR}/{last_thursday_date}.csv', index=False)
    print("✅ Data fetched successfully, saving CSV")

def create_sqlite_table():
    print("📦 Creating SQLite table")
    # Tworzenie połączenia z bazą (lub utworzenie nowej bazy jeśli nie istnieje)
    conn = sqlite3.connect(DB_PATH)
    
    # Tworzenie kursora do wykonywania zapytań
    cursor = conn.cursor()
    
    with open(f'{SQL_DIR}/players_table.sql', 'r') as f:
        create_players_table = f.read()
    
    # Tworzenie tabeli players
    cursor.execute(create_players_table)
    
    # Zatwierdzenie i zamknięcie
    conn.commit()
    conn.close()
    print("✅ SQLite table created")

def load_players_to_sqlite():
    print("📥 Loading players data into SQLite")
    # Tworzenie połączenia z bazą (lub utworzenie nowej bazy jeśli nie istnieje)
    conn = sqlite3.connect(DB_PATH)
    
    # Tworzenie kursora do wykonywania zapytań
    cursor = conn.cursor()
    
    with open(f'{SQL_DIR}/all_players_records.sql', 'r') as f:
        all_player_records_insert = f.read()
    
    # Wykonanie zapytania zapełniającego tabelę players
    cursor.execute(all_player_records_insert)
    
    conn.commit()
    conn.close()
    print("✅ Players data loaded into SQLite")

fetching_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_from_api,
    dag=dag,
)

creating_players_table = PythonOperator(
    task_id='create_players_table',
    python_callable=create_sqlite_table,
    dag=dag,
)

loading_players_to_sqlite = PythonOperator(
    task_id='load_players_to_sqlite',
    python_callable=load_players_to_sqlite,
    dag=dag,
)

[fetching_data, creating_players_table] >> loading_players_to_sqlite
