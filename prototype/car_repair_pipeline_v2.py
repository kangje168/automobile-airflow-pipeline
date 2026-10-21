from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import random
import logging
import sys

# Ajout du chemin des modules au sys.path
sys.path.append('/home/ubuntu/airflow_car_repair/modules')

from processors import DataProcessor, InsuranceEnricher
from validators import DataValidator, BusinessRuleValidator
from analytics import AnomalyDetector

# Configuration des chemins
BASE_PATH = '/home/ubuntu/airflow_car_repair'
RAW_DATA_PATH = os.path.join(BASE_PATH, 'data/raw')
STAGING_DATA_PATH = os.path.join(BASE_PATH, 'data/staging')
PROCESSED_DATA_PATH = os.path.join(BASE_PATH, 'data/processed')
QUARANTINE_DATA_PATH = os.path.join(BASE_PATH, 'data/quarantine')

# Création des répertoires si nécessaire
for path in [STAGING_DATA_PATH, PROCESSED_DATA_PATH, QUARANTINE_DATA_PATH]:
    os.makedirs(path, exist_ok=True)

# Arguments par défaut
default_args = {
    'owner': 'manus_ai',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# --- Classes de Traitement (Simulant des Opérateurs Personnalisés) ---

class CarRepairProcessor:
    @staticmethod
    def validate_schema(df, schema_type):
        logging.info(f"Validation du schéma pour {schema_type}")
        # Simulation de validation de schéma
        required_cols = {
            'machine': ['machine_id', 'timestamp', 'odometer_km'],
            'sensor': ['sensor_id', 'machine_id', 'timestamp', 'value'],
            'repair': ['repair_id', 'machine_id', 'repair_date', 'total_cost']
        }
        cols = required_cols.get(schema_type, [])
        return all(col in df.columns for col in cols)

    @staticmethod
    def detect_fraud(df):
        logging.info("Exécution de l'algorithme de détection de fraude")
        # Simulation: Coût de réparation > 1000 et kilométrage < 5000 est suspect
        if 'total_cost' in df.columns and 'odometer_km' in df.columns:
            suspects = df[(df['total_cost'] > 1000) & (df['odometer_km'] < 5000)]
            return not suspects.empty
        return False

# --- Fonctions de Tâches ---

def check_system_health(**kwargs):
    logging.info("Vérification de la santé du système de fichiers et de la base de données")
    # Simulation de vérification
    return True

def get_file_list(source_type, **kwargs):
    logging.info(f"Récupération de la liste des fichiers pour {source_type}")
    files = [f for f in os.listdir(RAW_DATA_PATH) if source_type in f]
    if not files:
        raise AirflowException(f"Aucun fichier trouvé pour {source_type}")
    return files

def process_file_batch(source_type, **kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids=f'ingestion_group.get_{source_type}_files')
    logging.info(f"Traitement du lot de fichiers: {files}")
    
    all_data = []
    for file_name in files:
        file_path = os.path.join(RAW_DATA_PATH, file_name)
        if file_name.endswith('.json'):
            with open(file_path, 'r') as f:
                data = json.load(f)
                all_data.extend(data if isinstance(data, list) else [data])
        elif file_name.endswith('.csv'):
            df = pd.read_csv(file_path)
            all_data.extend(df.to_dict('records'))
    
    df_batch = pd.DataFrame(all_data)
    
    # Validation de schéma via le module dédié
    if not DataValidator.validate_schema(df_batch, source_type):
        return f'validation_group.quarantine_{source_type}'
    
    # Vérification de la qualité des données
    if not DataValidator.check_data_quality(df_batch):
        return f'validation_group.quarantine_{source_type}'
    
    # Sauvegarde en staging
    staging_file = os.path.join(STAGING_DATA_PATH, f"{source_type}_batch.parquet")
    df_batch.to_parquet(staging_file, index=False)
    return f'validation_group.transform_{source_type}'

def transform_data(source_type, **kwargs):
    logging.info(f"Transformation des données {source_type}")
    ti = kwargs['ti']
    # Récupération du fichier staging
    staging_file = os.path.join(STAGING_DATA_PATH, f"{source_type}_batch.parquet")
    df = pd.read_parquet(staging_file)
    
    if source_type == 'sensor':
        df = DataProcessor.normalize_sensor_data(df)
        df = DataProcessor.aggregate_daily_sensor_stats(df)
    
    # Sauvegarde des données transformées
    processed_file = os.path.join(PROCESSED_DATA_PATH, f"{source_type}_processed.parquet")
    df.to_parquet(processed_file, index=False)
    return f"Transformed {source_type}"

def quarantine_data(source_type, **kwargs):
    logging.info(f"Mise en quarantaine des données {source_type}")
    return f"Quarantined {source_type}"

def aggregate_and_enrich(**kwargs):
    logging.info("Agrégation et enrichissement multi-sources")
    # Simulation de jointure complexe entre machines, capteurs et réparations
    return "Enriched Master Dataset"

def anomaly_detection_branch(**kwargs):
    logging.info("Analyse des anomalies et détection de fraude")
    # Chargement des données enrichies (simulé ici)
    # Dans un vrai cas, on lirait le fichier produit par aggregate_and_enrich
    
    # Utilisation du module d'analyse
    # fraud_suspects = AnomalyDetector.detect_fraud_patterns(df_enriched)
    
    # Simulation de branchement basé sur les résultats de l'analyse
    fraud_detected = random.random() > 0.85
    if fraud_detected:
        return 'fraud_investigation_group.notify_fraud_team'
    else:
        return 'compliance_check_group.verify_policy_coverage'

def generate_final_report(**kwargs):
    logging.info("Génération du rapport final pour l'assurance")
    return "Final Insurance Claim Report"

# --- Définition du DAG ---

with DAG(
    'car_repair_sophisticated_pipeline_v2',
    default_args=default_args,
    description='Pipeline de données automobile ultra-sophistiqué pour l\'assurance',
    schedule_interval='@daily',
    catchup=False,
    tags=['enterprise', 'automotive', 'insurance', 'complex'],
) as dag:

    # 1. Setup & Health Check
    setup_task = PythonOperator(
        task_id='system_health_check',
        python_callable=check_system_health,
    )

    # 2. Ingestion Group (Parallel & Dynamic)
    with TaskGroup("ingestion_group") as ingestion_group:
        for source in ['machine', 'sensor', 'repair']:
            get_files = PythonOperator(
                task_id=f'get_{source}_files',
                python_callable=get_file_list,
                op_kwargs={'source_type': source},
            )
            
            # Simulation d'un capteur de fichier (FileSensor)
            # Dans un vrai environnement, il attendrait l'arrivée des fichiers
            wait_for_file = EmptyOperator(task_id=f'wait_for_{source}_data')
            
            get_files >> wait_for_file

    # 3. Validation & Transformation Group (Branching)
    with TaskGroup("validation_group") as validation_group:
        for source in ['machine', 'sensor', 'repair']:
            validate_branch = BranchPythonOperator(
                task_id=f'validate_and_route_{source}',
                python_callable=process_file_batch,
                op_kwargs={'source_type': source},
            )
            
            transform = PythonOperator(
                task_id=f'transform_{source}',
                python_callable=transform_data,
                op_kwargs={'source_type': source},
            )
            
            quarantine = PythonOperator(
                task_id=f'quarantine_{source}',
                python_callable=quarantine_data,
                op_kwargs={'source_type': source},
            )
            
            validate_branch >> [transform, quarantine]

    # 4. Data Integration
    enrich_task = PythonOperator(
        task_id='aggregate_and_enrich',
        python_callable=aggregate_and_enrich,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 5. Advanced Analytics Branching
    analytics_branch = BranchPythonOperator(
        task_id='advanced_analytics_routing',
        python_callable=anomaly_detection_branch,
    )

    # 6. Fraud Investigation Group
    with TaskGroup("fraud_investigation_group") as fraud_group:
        notify_team = PythonOperator(
            task_id='notify_fraud_team',
            python_callable=lambda: "Fraud Team Notified",
        )
        flag_claim = EmptyOperator(task_id='flag_claim_for_manual_review')
        notify_team >> flag_claim

    # 7. Compliance & Policy Check Group
    with TaskGroup("compliance_check_group") as compliance_group:
        verify_policy = PythonOperator(
            task_id='verify_policy_coverage',
            python_callable=lambda: "Policy Verified",
        )
        calculate_payout = PythonOperator(
            task_id='calculate_estimated_payout',
            python_callable=lambda: "Payout Calculated",
        )
        verify_policy >> calculate_payout

    # 8. Finalization
    final_report = PythonOperator(
        task_id='generate_final_insurance_report',
        python_callable=generate_final_report,
        trigger_rule=TriggerRule.ONE_SUCCESS, # Soit après compliance, soit après investigation (si applicable)
    )

    archive_results = EmptyOperator(task_id='archive_to_data_lake')
    
    cleanup_task = EmptyOperator(
        task_id='cleanup_temporary_files',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Définition des dépendances de haut niveau
    setup_task >> ingestion_group >> validation_group >> enrich_task >> analytics_branch
    analytics_branch >> [fraud_group, compliance_group]
    [fraud_group, compliance_group] >> final_report >> archive_results >> cleanup_task
