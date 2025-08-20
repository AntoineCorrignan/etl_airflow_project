from __future__ import annotations
import pendulum
import pandas as pd
import psycopg2
from io import StringIO

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_table_if_not_exists(**kwargs):
    """
    Crée la table dim_site si elle n'existe pas.
    """
    try:
        print("Creating table if not exists...")
        
        # Connexion à la base de données Neon
        neon_hook = PostgresHook(postgres_conn_id='bdd_neon_recette')
        
        # Requête SQL pour créer la table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_site (
                site VARCHAR(255),
                id_site VARCHAR(255),
                adresse VARCHAR(255),
                ville VARCHAR(255),
                code_postal VARCHAR(255),
                region VARCHAR(255),
                pays VARCHAR(255)
            );
        """
        
        # Exécution de la requête
        neon_hook.run(create_table_sql)
        print("Table dim_site created or already exists")
        return "Table creation successful"
        
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise

def extract_and_transform_data(**kwargs):
    """
    Extrait, transforme et retourne le DataFrame résultant.
    """
    try:
        print("Starting data extraction and transformation...")
        # 1. Connexion à la base de données source
        source_hook = PostgresHook(postgres_conn_id='administration_recette')
        
        # 2. Requête SQL pour joindre les données
        joined_sql = """
            SELECT
                s.name AS site,
                s.id AS id_site,
                a.address AS adresse,
                a.city AS ville,
                a.zip_code AS code_postal,
                a.region AS region,
                a.country AS pays
            FROM sites AS s
            LEFT JOIN addresses AS a ON a.id = s.address_id;
        """
        
        # 3. Exécution et récupération des données directement dans un DataFrame
        joined_df = source_hook.get_pandas_df(joined_sql)
        print(f"Data extracted and joined: {len(joined_df)} rows")
        
        # 4. Nettoyage des données (version simplifiée)
        # S'assurer que les colonnes sont des strings pour éviter les erreurs de type
        for col in joined_df.columns:
            if joined_df[col].dtype == 'object':
                joined_df[col] = joined_df[col].astype(str).str.encode('utf-8', errors='ignore').str.decode('utf-8')
        
        # 5. Stockage des données pour la tâche suivante
        kwargs['ti'].xcom_push(key='transformed_data_df', value=joined_df.to_json(orient='records'))
        print(f"Data transformation completed. Final dataset: {len(joined_df)} rows")
        return f"Successfully processed {len(joined_df)} rows"
    except Exception as e:
        print(f"Error in extract_and_transform_data: {str(e)}")
        raise

def load_data_to_neon(**kwargs):
    """
    Charge les données transformées dans la table 'dim_site' du datamart Neon.
    """
    try:
        print("Starting data loading...")
        
        # 1. Récupération des données passées par la tâche précédente
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='extract_and_transform_data', key='transformed_data_df')
        
        if not json_data:
            raise ValueError("No data received from previous task")
            
        df_transformed = pd.read_json(json_data, orient='records')
        print(f"Data received for loading: {len(df_transformed)} rows")
        
        if len(df_transformed) == 0:
            print("No data to load")
            return "No data to load"
        
        # 2. Connexion à la base de données cible Neon
        neon_hook = PostgresHook(postgres_conn_id='bdd_neon_recette')
        
        # 3. Vider la table avant insertion
        neon_hook.run("TRUNCATE TABLE dim_site;")
        print("Table truncated")
        
        # 4. Préparation des données pour le chargement
        output = StringIO()
        df_transformed.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        # 5. Chargement des données
        conn = neon_hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Insérer les nouvelles données
            cur.copy_from(
                output, 
                'dim_site', 
                sep='\t', 
                columns=list(df_transformed.columns),
                null='\\N'
            )
            conn.commit()
            print(f"Successfully loaded {len(df_transformed)} rows into dim_site")
            return f"Successfully loaded {len(df_transformed)} rows"
            
        except (Exception, psycopg2.Error) as error:
            print(f"Error loading data into Neon: {error}")
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        print(f"Error in load_data_to_neon: {str(e)}")
        raise

# Configuration par défaut
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Définition du DAG
with DAG(
    dag_id="etl_dim_site_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Planification manuelle
    catchup=False,
    tags=["etl", "datamart", "site"],
    description="ETL pipeline for dim_site table from sites and addresses",
    max_active_runs=1,
) as dag:
    
    # Tâche 1 : Création de la table si elle n'existe pas
    create_table_task = PythonOperator(
        task_id="create_dim_site_table",
        python_callable=create_table_if_not_exists,
    )
    
    # Tâche 2 : Extraction et transformation des données
    extract_and_transform_task = PythonOperator(
        task_id="extract_and_transform_data",
        python_callable=extract_and_transform_data,
    )
    
    # Tâche 3 : Chargement des données dans Neon
    load_data_to_neon_task = PythonOperator(
        task_id="load_data_to_neon",
        python_callable=load_data_to_neon,
    )
    
    # Définition de l'ordre d'exécution
    create_table_task >> extract_and_transform_task >> load_data_to_neon_task