# timescale_to_json_extractor.py

import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

import psycopg2
import pandas as pd

# --- Configuration ---
# Define where to save the output JSON files
PROJECT_DIR = Path(__file__).resolve().parent
JSON_OUTPUT_PATH = PROJECT_DIR / "data"

# Ensure the output directory exists
os.makedirs(JSON_OUTPUT_PATH, exist_ok=True)

# --- Helper Function: Logger ---
def setup_logger(name, level=logging.INFO):
    """A simple helper function to configure a logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

# --- Data Extraction Functions for Each Table ---

def extract_vessels_data(db_conn) -> list:
    """
    Queries TimescaleDB to get a list of all vessels, their IDs, and names.
    Returns a list of dictionaries.
    """
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for all vessel IDs and names...")

    query = """
        SELECT x_studio_vessel_id, x_name 
        FROM odoo_x_vessel
        WHERE x_studio_vessel_id IS NOT NULL AND x_studio_vessel_id != '';
    """
    
    try:
        df = pd.read_sql(query, db_conn)
        df['vehicle_id'] = pd.to_numeric(df['x_studio_vessel_id'].str.lstrip('V0'), errors='coerce')
        df.rename(columns={'x_name': 'vessel_name'}, inplace=True)
        df.dropna(subset=['vehicle_id'], inplace=True)
        df['vehicle_id'] = df['vehicle_id'].astype(int).astype(str)
        
        logger.info(f"  -> Found {len(df)} vessels.")
        return df[['vehicle_id', 'vessel_name']].to_dict('records')
            
    except Exception as e:
        logger.error(f"Failed to query vessel data: {e}")
        return []

def extract_incidents_data(db_conn) -> list:
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for latest incidents data...")
    
    query = """
        SELECT x_name, x_studio_vessels, x_studio_date, x_studio_priority, x_studio_human_error, x_studio_authorities, x_studio_type, x_studio_html_field_QMBdm, x_studio_technical_error_1
        FROM odoo_x_incident_reporting; 
    """
    
    try:
        df = pd.read_sql(query, db_conn)

        df["x_studio_date"] = pd.to_datetime(df["x_studio_date"], errors="coerce")
        df["x_studio_date"] = df["x_studio_date"].dt.strftime('%d/%m/%Y').fillna('')

        
        logger.info(f"  -> Found {len(df)} incidents.")
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Failed to query incidents data: {e}")
        return []

def extract_control_data(db_conn) -> list:
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for control data...")

    query = """
        SELECT message_sent_time, system_running_state_id, vehicle_id
        FROM pm010_messages
        WHERE message_sent_time >= '01/01/2025'
        ORDER BY message_sent_time;
    """

    try:
        df = pd.read_sql(query, db_conn)
        df["message_sent_time"] = df["message_sent_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        logger.info(f"  -> Found {len(df)} control messages.")
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Failed to query control data: {e}")
        return []

def extract_navigational_reports_data(db_conn) -> list:
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for control data...")

    query = """
        SELECT id, vessel_id, remote_operator_id, boat_master, departure_place, 
                departure_date_time, arrival_place, arrival_date_time, traffic_intensity, 
                loaded_condition, sailing_had_problems, main_problem_cause, specify_other_cause,
                why_control_was_handed_over, was_e_stop_pushed, ship_connectivity, seafar_equipment_needed_improvement,
                which_equipment_notes, track_pilot_was_used, how_track_pilot_was_used, sailing_assessment_remarks
        FROM odoo_navigation_report
        WHERE vessel_id = 5
        ORDER BY departure_date_time;
    """
    # vessel_id = 5 -> Privilege

    try:
        df = pd.read_sql(query, db_conn)
        # Ensure it's a datetime column
        df["departure_date_time"] = pd.to_datetime(df["departure_date_time"], errors="coerce")
        df["departure_date_time"] = df["departure_date_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        df["arrival_date_time"] = pd.to_datetime(df["arrival_date_time"], errors="coerce")
        df["arrival_date_time"] = df["arrival_date_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        logger.info(f"  -> Found {len(df)} navigational reports.")
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Failed to query control data: {e}")
        return []

def extract_location_data(db_conn) -> list:
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for location data...")

    query = """
        SELECT message_sent_time, location, vehicle_id, latitude, longitude
        FROM gga_messages
        WHERE message_sent_time >= '01/01/2025'
        ORDER BY message_sent_time;
    """

    try:
        df = pd.read_sql(query, db_conn)
        df["message_sent_time"] = df["message_sent_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        logger.info(f"  -> Found {len(df)} location messages.")
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Failed to query location data: {e}")
        return []
    
def extract_employees_data(db_conn) -> list:
    logger = logging.getLogger('TimescaleExtractor')
    logger.info("Querying for employees data...")

    query = """
        SELECT id, name
        FROM odoo_hr_employee;
    """

    try:
        df = pd.read_sql(query, db_conn)
        
        logger.info(f"  -> Found {len(df)} employees.")
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Failed to query employees data: {e}")
        return []

# --- Main Configuration for Extraction Tasks ---

# This dictionary maps a task key to its extraction function and output file.
# To add a new table, add a new entry here.
TABLE_CONFIG = {
    "vessels": {
        "extractor_function": extract_vessels_data,
        "output_filename": "vessels.json"
    },
    "incidents": {
        "extractor_function": extract_incidents_data,
        "output_filename": "incidents.json"
    },
    # Add another table here in the future
    # "another_table": {
    #     "extractor_function": extract_another_table_data,
    #     "output_filename": "another_table.json"
    # },
    "control": {
        "extractor_function": extract_control_data,
        "output_filename": "control.json"
    },
    "location": {
        "extractor_function": extract_location_data,
        "output_filename": "location.json"
    },
    "navigational_reports": {
        "extractor_function": extract_navigational_reports_data,
        "output_filename": "navigation_reports.json"
    },
    "employees": {
        "extractor_function": extract_employees_data,
        "output_filename": "employees.json"
    }
}

# --- Generic JSON Saving Function ---
def save_data_to_json(data, filename: str, output_path: Path):
    """Saves a list of dictionaries to a JSON file."""
    if not data:
        logging.getLogger('TimescaleExtractor').warning(f"No data provided for {filename}, skipping save.")
        return False
        
    file_path = output_path / filename
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logging.getLogger('TimescaleExtractor').error(f"Could not write to file {file_path}: {e}")
        return False

def main():
    """
    Main function to connect to the database and run all configured extraction tasks.
    """
    logger = setup_logger('TimescaleExtractor')
    logger.info("--- Starting TimescaleDB Data Extraction Script ---")
    
    load_dotenv(find_dotenv())
    timescale_url = os.environ.get("TIMESCALE_SERVICE_URL")
    if not timescale_url:
        logger.error("FATAL: TIMESCALE_SERVICE_URL not found in .env file. Aborting.")
        return

    db_conn = None
    try:
        # 1. Connect to the database (once for all tasks)
        logger.info("Connecting to TimescaleDB...")
        db_conn = psycopg2.connect(dsn=timescale_url, connect_timeout=30)
        logger.info("✅ Successfully connected to TimescaleDB.")

        # 2. Iterate through configured tables and extract data
        for task_name, config in TABLE_CONFIG.items():
            logger.info(f"\n----- Starting task: '{task_name}' -----")
            
            # Get the correct function and filename from the config
            extractor_func = config["extractor_function"]
            output_filename = config["output_filename"]
            
            # Execute the extraction
            data = extractor_func(db_conn)
            
            # Save the data to its specific JSON file
            if save_data_to_json(data, output_filename, JSON_OUTPUT_PATH):
                logger.info(f"✅ Successfully saved '{task_name}' data to: {JSON_OUTPUT_PATH / output_filename}")
            else:
                logger.error(f"❌ Failed to save '{task_name}' data.")
            
            logger.info(f"----- Finished task: '{task_name}' -----")

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main process: {e}", exc_info=True)
    finally:
        # 4. Always ensure the connection is closed
        if db_conn:
            db_conn.close()
            logger.info("\nDatabase connection closed.")
            
    logger.info("--- All extraction tasks finished. ---")

if __name__ == "__main__":
    main()
