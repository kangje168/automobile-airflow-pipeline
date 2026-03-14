import pandas as pd
import numpy as np
import json
import os
import random
from datetime import datetime, timedelta

# Configuration
NUM_VEHICLES = 10
NUM_DAYS = 30
START_DATE = datetime(2023, 3, 1)
OUTPUT_DIR = '/home/ubuntu/airflow_car_repair/data/raw'

# Création du répertoire de sortie si nécessaire
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_machine_data():
    data = []
    for i in range(NUM_VEHICLES):
        vehicle_id = f"VEH{i+1:03d}"
        for day in range(NUM_DAYS):
            timestamp = START_DATE + timedelta(days=day, hours=random.randint(8, 18))
            record = {
                "machine_id": vehicle_id,
                "timestamp": timestamp.isoformat(),
                "odometer_km": 10000 + (day * 50) + random.randint(0, 20),
                "engine_status": random.choice(['OK', 'OK', 'OK', 'Warning', 'Critical']),
                "battery_volt": round(random.uniform(11.5, 14.5), 2),
                "fuel_level": round(random.uniform(10.0, 100.0), 1),
                "error_codes": random.sample(['P0101', 'P0300', 'P0420', 'B1234'], random.randint(0, 2)) if random.random() > 0.8 else [],
                "gps_latitude": 48.8566 + random.uniform(-0.1, 0.1),
                "gps_longitude": 2.3522 + random.uniform(-0.1, 0.1)
            }
            data.append(record)
    
    # Sauvegarde en JSON
    with open(os.path.join(OUTPUT_DIR, 'machine_data.json'), 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Généré {len(data)} enregistrements de données machine.")

def generate_sensor_data():
    data = []
    sensor_types = [
        ('tire_pressure', 'PSI', 30.0, 35.0),
        ('brake_temp', '°C', 20.0, 150.0),
        ('oil_pressure', 'kPa', 200.0, 500.0)
    ]
    
    for i in range(NUM_VEHICLES):
        vehicle_id = f"VEH{i+1:03d}"
        for day in range(NUM_DAYS):
            for sensor_type, unit, t_min, t_max in sensor_types:
                timestamp = START_DATE + timedelta(days=day, hours=random.randint(8, 18), minutes=random.randint(0, 59))
                value = round(random.uniform(t_min - 5, t_max + 20), 2)
                record = {
                    "sensor_id": f"SEN_{vehicle_id}_{sensor_type[:3].upper()}",
                    "machine_id": vehicle_id,
                    "timestamp": timestamp.isoformat(),
                    "sensor_type": sensor_type,
                    "value": value,
                    "unit": unit,
                    "threshold_min": t_min,
                    "threshold_max": t_max
                }
                data.append(record)
    
    # Sauvegarde en CSV
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, 'sensor_data.csv'), index=False)
    print(f"Généré {len(data)} enregistrements de données de capteurs.")

def generate_repair_data():
    data = []
    repair_shops = ['SHOP001', 'SHOP002', 'SHOP003']
    parts = ['brake_pads', 'oil_filter', 'air_filter', 'spark_plugs', 'battery', 'tires']
    
    for i in range(NUM_VEHICLES):
        vehicle_id = f"VEH{i+1:03d}"
        # Simuler 1 à 3 réparations par véhicule sur la période
        num_repairs = random.randint(1, 3)
        for r in range(num_repairs):
            repair_date = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-1))
            parts_cost = round(random.uniform(50, 500), 2)
            labor_cost = round(random.uniform(40, 300), 2)
            record = {
                "repair_id": f"REP_{vehicle_id}_{r+1:02d}",
                "machine_id": vehicle_id,
                "repair_date": repair_date.strftime('%Y-%m-%d'),
                "repair_shop_id": random.choice(repair_shops),
                "description": f"Réparation de routine pour {vehicle_id}",
                "parts_cost": parts_cost,
                "labor_cost": labor_cost,
                "total_cost": round(parts_cost + labor_cost, 2),
                "repaired_parts": random.sample(parts, random.randint(1, 3)),
                "warranty_status": random.choice([True, False])
            }
            data.append(record)
            
    # Sauvegarde en JSON
    with open(os.path.join(OUTPUT_DIR, 'repair_data.json'), 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Généré {len(data)} enregistrements de données de réparation.")

if __name__ == "__main__":
    generate_machine_data()
    generate_sensor_data()
    generate_repair_data()
    print("Génération des données terminée.")
