import pandas as pd
import numpy as np
import logging

class AnomalyDetector:
    """Classe pour la détection d'anomalies et de fraudes dans les données de réparation."""
    
    @staticmethod
    def detect_outlier_costs(df, threshold_std=3):
        """Détecte les coûts de réparation aberrants (outliers) par rapport à la moyenne."""
        logging.info("Détection des coûts de réparation aberrants")
        if 'total_cost' in df.columns:
            mean_cost = df['total_cost'].mean()
            std_cost = df['total_cost'].std()
            
            # Définition du seuil d'anomalie
            upper_bound = mean_cost + (threshold_std * std_cost)
            
            # Identification des anomalies
            anomalies = df[df['total_cost'] > upper_bound]
            return anomalies
        return pd.DataFrame()

    @staticmethod
    def detect_sensor_anomalies(df):
        """Détecte les anomalies dans les données de capteurs (ex: valeurs hors seuils)."""
        logging.info("Détection des anomalies de capteurs")
        if 'value' in df.columns and 'threshold_min' in df.columns and 'threshold_max' in df.columns:
            anomalies = df[(df['value'] < df['threshold_min']) | (df['value'] > df['threshold_max'])]
            return anomalies
        return pd.DataFrame()

    @staticmethod
    def detect_fraud_patterns(df):
        """Détecte des motifs de fraude potentiels (ex: réparations fréquentes sur la même pièce)."""
        logging.info("Détection des motifs de fraude potentiels")
        # Exemple: Plus de 3 réparations sur la même pièce pour le même véhicule en moins de 30 jours
        if 'machine_id' in df.columns and 'repaired_parts' in df.columns and 'repair_date' in df.columns:
            # Explosion de la liste des pièces réparées
            df_exploded = df.explode('repaired_parts')
            df_exploded['repair_date'] = pd.to_datetime(df_exploded['repair_date'])
            
            # Groupement par véhicule et pièce, puis comptage sur une fenêtre glissante
            # (Simplifié ici pour la démonstration)
            counts = df_exploded.groupby(['machine_id', 'repaired_parts']).size()
            fraud_suspects = counts[counts > 3]
            return fraud_suspects
        return pd.Series()
