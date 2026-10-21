import pandas as pd
import logging

class DataValidator:
    """Classe pour la validation des données de réparation automobile."""
    
    @staticmethod
    def validate_schema(df, schema_type):
        """Vérifie que le DataFrame contient toutes les colonnes requises."""
        required_cols = {
            'machine': ['machine_id', 'timestamp', 'odometer_km', 'engine_status'],
            'sensor': ['sensor_id', 'machine_id', 'timestamp', 'sensor_type', 'value'],
            'repair': ['repair_id', 'machine_id', 'repair_date', 'total_cost']
        }
        cols = required_cols.get(schema_type, [])
        missing_cols = [col for col in cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Colonnes manquantes pour {schema_type}: {missing_cols}")
            return False
        return True

    @staticmethod
    def check_data_quality(df):
        """Vérifie la qualité des données (valeurs nulles, types, plages)."""
        # Vérification des valeurs nulles dans les colonnes critiques
        critical_cols = ['machine_id', 'timestamp', 'value', 'total_cost']
        for col in critical_cols:
            if col in df.columns and df[col].isnull().any():
                logging.warning(f"Valeurs nulles détectées dans la colonne {col}")
                return False
        
        # Vérification des plages de valeurs (ex: coût de réparation positif)
        if 'total_cost' in df.columns and (df['total_cost'] < 0).any():
            logging.warning("Coût de réparation négatif détecté")
            return False
            
        return True

class BusinessRuleValidator:
    """Classe pour la validation des règles métier d'assurance."""
    
    @staticmethod
    def verify_policy_coverage(repair_record, policy_data):
        """Vérifie si une réparation est couverte par la police d'assurance."""
        # Simulation de vérification de police
        # Exemple: Si la réparation est due à l'usure normale, elle n'est pas couverte
        if 'description' in repair_record and 'routine' in repair_record['description'].lower():
            return False
        return True
