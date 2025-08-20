from __future__ import annotations
import pendulum
import pandas as pd

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

def extract_and_load_raw_data(**kwargs):
    """
    Extrait les données brutes des tables source et les charge dans des tables de staging.
    """
    try:
        print("Starting raw data extraction and loading...")
        
        # Connexion à la base de données source
        source_hook = PostgresHook(postgres_conn_id='administration_recette')
        companies_sql = "SELECT id, siret, tva_number FROM companies;"
        addresses_sql = "SELECT id, address, city, country, region, zip_code FROM addresses;"
        
        companies_df = source_hook.get_pandas_df(companies_sql)
        addresses_df = source_hook.get_pandas_df(addresses_sql)

        # Connexion à la base de données cible (le datamart)
        target_hook = PostgresHook(postgres_conn_id='bdd_neon_recette')
        
        # Charger les données brutes dans des tables de staging
        target_hook.insert_rows(
            table='raw_companies',
            rows=companies_df.values.tolist(),
            target_fields=companies_df.columns.tolist()
        )
        target_hook.insert_rows(
            table='raw_addresses',
            rows=addresses_df.values.tolist(),
            target_fields=addresses_df.columns.tolist()
        )
        print("Raw data loaded successfully. Ready for dbt transformation.")
        
    except Exception as e:
        print(f"Error in extract_and_load_raw_data: {str(e)}")
        raise

# Définition du DAG
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id="elt_dim_entreprise_with_dbt_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["elt", "dbt", "datamart", "entreprise"],
    description="ELT pipeline for dim_entreprise using dbt for transformation",
    max_active_runs=1,
) as dag:
    
    # Étape 1: Extraction et chargement des données brutes
    extract_and_load_task = PythonOperator(
        task_id="extract_and_load_raw_data",
        python_callable=extract_and_load_raw_data,
    )
    
    # Étape 2: Transformation des données avec dbt
    dbt_run_task = BashOperator(
        task_id="dbt_run_models",
        bash_command='cd /path/to/your/dbt/project && dbt run',
    )
    
    # Définition de l'ordre des tâches
    extract_and_load_task >> dbt_run_task