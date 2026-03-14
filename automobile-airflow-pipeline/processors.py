import pandas as pd
import numpy as np
import logging

class DataProcessor:
    """Classe pour le traitement et la transformation des données de réparation automobile."""
    
    @staticmethod
    def normalize_sensor_data(df):
        """Normalise les valeurs des capteurs (ex: conversion d'unités)."""
        logging.info("Normalisation des données de capteurs")
        # Exemple: Conversion PSI en Bar si nécessaire
        if 'unit' in df.columns and 'value' in df.columns:
            mask = df['unit'] == 'PSI'
            df.loc[mask, 'value'] = df.loc[mask, 'value'] * 0.0689476
            df.loc[mask, 'unit'] = 'Bar'
        return df

    @staticmethod
    def calculate_vehicle_age(df, current_year=2023):
        """Calcule l'âge du véhicule à partir de l'année de fabrication (si disponible)."""
        if 'manufacture_year' in df.columns:
            df['vehicle_age'] = current_year - df['manufacture_year']
        return df

    @staticmethod
    def aggregate_daily_sensor_stats(df):
        """Agrège les données de capteurs par jour et par véhicule."""
        logging.info("Agrégation des statistiques quotidiennes des capteurs")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        agg_df = df.groupby(['machine_id', 'date', 'sensor_type']).agg({
            'value': ['mean', 'min', 'max', 'std']
        }).reset_index()
        
        # Aplatir les colonnes multi-index
        agg_df.columns = ['_'.join(col).strip('_') for col in agg_df.columns.values]
        return agg_df

class InsuranceEnricher:
    """Classe pour enrichir les données avec des informations d'assurance."""
    
    @staticmethod
    def join_repair_and_machine(repair_df, machine_df):
        """Joint les données de réparation avec les données machine les plus proches en date."""
        logging.info("Jointure des données de réparation et machine")
        # Logique de jointure temporelle complexe (ASOF join)
        repair_df['repair_date'] = pd.to_datetime(repair_df['repair_date'])
        machine_df['timestamp'] = pd.to_datetime(machine_df['timestamp'])
        
        # Tri pour asof join
        repair_df = repair_df.sort_values('repair_date')
        machine_df = machine_df.sort_values('timestamp')
        
        enriched_df = pd.merge_asof(
            repair_df, 
            machine_df, 
            left_on='repair_date', 
            right_on='timestamp', 
            by='machine_id',
            direction='backward'
        )
        return enriched_df
