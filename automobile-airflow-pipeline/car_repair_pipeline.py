from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import random

# Configuration des chemins
BASE_PATH = '/home/ubuntu/airflow_car_repair'
RAW_DATA_PATH = os.path.join(BASE_PATH, 'data/raw')
STAGING_DATA_PATH = os.path.join(BASE_PATH, 'data/staging')
PROCESSED_DATA_PATH = os.path.join(BASE_PATH, 'data/processed')
QUARANTINE_DATA_PATH = os.path.join(BASE_PATH, 'data/quarantine')

# Création des répertoires si nécessaire
for path in [STAGING_DATA_PATH, PROCESSED_DATA_PATH, QUARANTINE_DATA_PATH]:
    os.makedirs(path, exist_ok=True)

default_args = {
    'owner': 'manus_ai',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_data(source_type, **kwargs):
    print(f"Ingestion des données de type: {source_type}")
    file_name = f"{source_type}_data.json" if source_type != 'sensor' else f"{source_type}_data.csv"
    raw_file = os.path.join(RAW_DATA_PATH, file_name)
    staging_file = os.path.join(STAGING_DATA_PATH, file_name)
    
    if os.path.exists(raw_file):
        # Simulation d'un déplacement vers staging
        import shutil
        shutil.copy(raw_file, staging_file)
        return staging_file
    else:
        raise FileNotFoundError(f"Fichier source non trouvé: {raw_file}")

def validate_data(source_type, **kwargs):
    ti = kwargs['ti']
    staging_file = ti.xcom_pull(task_ids=f'ingestion_group.ingest_{source_type}')
    print(f"Validation des données: {staging_file}")
    
    # Simulation de validation
    # On décide aléatoirement si les données sont valides ou non pour démontrer le branchement
    is_valid = random.random() > 0.1
    
    if is_valid:
        return f'validation_group.process_{source_type}'
    else:
        return f'validation_group.quarantine_{source_type}'

def process_data(source_type, **kwargs):
    print(f"Traitement des données: {source_type}")
    # Logique de transformation ici (simulée)
    return f"Processed {source_type}"

def quarantine_data(source_type, **kwargs):
    print(f"Mise en quarantaine des données: {source_type}")
    # Logique de mise en quarantaine ici (simulée)
    return f"Quarantined {source_type}"

def enrich_and_join(**kwargs):
    print("Enrichissement et jointure des données")
    # Logique de jointure complexe ici
    return "Enriched Data"

def detect_anomalies(**kwargs):
    print("Détection d'anomalies et de fraudes")
    # Simulation de détection d'anomalies
    anomalies_found = random.random() > 0.7
    if anomalies_found:
        return 'anomaly_alert'
    else:
        return 'prepare_insurance_report'

def generate_report(**kwargs):
    print("Génération du rapport d'assurance")
    return "Insurance Report Generated"

def send_alert(**kwargs):
    print("ALERTE: Anomalies détectées dans les données de réparation !")
    return "Alert Sent"

with DAG(
    'car_repair_data_pipeline_v1',
    default_args=default_args,
    description='Pipeline complexe de traitement des données de réparation automobile',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['insurance', 'iot', 'automotive'],
) as dag:

    start = EmptyOperator(task_id='start_pipeline')

    with TaskGroup("ingestion_group") as ingestion_group:
        for source in ['machine', 'sensor', 'repair']:
            PythonOperator(
                task_id=f'ingest_{source}',
                python_callable=ingest_data,
                op_kwargs={'source_type': source},
            )

    with TaskGroup("validation_group") as validation_group:
        for source in ['machine', 'sensor', 'repair']:
            branch = BranchPythonOperator(
                task_id=f'validate_{source}',
                python_callable=validate_data,
                op_kwargs={'source_type': source},
            )
            
            process = PythonOperator(
                task_id=f'process_{source}',
                python_callable=process_data,
                op_kwargs={'source_type': source},
            )
            
            quarantine = PythonOperator(
                task_id=f'quarantine_{source}',
                python_callable=quarantine_data,
                op_kwargs={'source_type': source},
            )
            
            branch >> [process, quarantine]

    enrich_join = PythonOperator(
        task_id='enrich_and_join_data',
        python_callable=enrich_and_join,
        trigger_rule=TriggerRule.ALL_SUCCESS, # On attend que toutes les validations réussissent
    )

    anomaly_branch = BranchPythonOperator(
        task_id='check_for_anomalies',
        python_callable=detect_anomalies,
    )

    anomaly_alert = PythonOperator(
        task_id='anomaly_alert',
        python_callable=send_alert,
    )

    insurance_report = PythonOperator(
        task_id='prepare_insurance_report',
        python_callable=generate_report,
    )

    archive_data = EmptyOperator(task_id='archive_processed_data')

    end = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.ONE_SUCCESS, # Soit après rapport, soit après alerte
    )

    # Définition des dépendances principales
    start >> ingestion_group >> validation_group >> enrich_join >> anomaly_branch
    anomaly_branch >> [anomaly_alert, insurance_report]
    insurance_report >> archive_data >> end
    anomaly_alert >> end
