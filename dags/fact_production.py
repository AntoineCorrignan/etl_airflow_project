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
    Crée la table fact_production si elle n'existe pas.
    """
    try:
        print("Creating table if not exists...")
        
        # Connexion à la base de données Neon
        neon_hook = PostgresHook(postgres_conn_id='bdd_neon_recette')
        
        # Requête SQL pour créer la table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS fact_production (
                variable VARCHAR(255),
                id_variable VARCHAR(255),
                valeur DOUBLE PRECISION,
                unit VARCHAR(255),
                date_entree TIMESTAMP,
                id_site VARCHAR(255),
                type VARCHAR(255),
                equipement VARCHAR(255),
                CONSTRAINT unique_production UNIQUE (id_variable, date_entree)
            );
        """
        
        # Exécution de la requête
        neon_hook.run(create_table_sql)
        print("Table fact_production created or already exists")
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
        source_hook = PostgresHook(postgres_conn_id='core_recette')
        
        # 2. Requête SQL pour joindre les données
        joined_sql = """
            SELECT
                v.name AS variable,
                e.id_variable AS id_variable,
                e.value AS valeur,
                v.unit AS unit,
                e.created_date AS date_entree,
                e.id_site AS id_site,
                v.type AS type,
                v.equipment AS equipement
            FROM public.variables AS v
            LEFT JOIN public.entries AS e ON e.id_variable = v.id
            ORDER BY e.id_variable;
        """
        
        # 3. Exécution et récupération des données directement dans un DataFrame
        joined_df = source_hook.get_pandas_df(joined_sql)
        print(f"Data extracted and joined: {len(joined_df)} rows")
        
        # 4. Nettoyage et conversion des types

        # suppression des id_variable null
        joined_df.dropna(subset=['id_variable'], inplace=True)
        print(f"Data after filtering NULL id_variable: {len(joined_df)} rows")

        # Convertir les valeurs non numériques en NaN
        joined_df['valeur'] = pd.to_numeric(joined_df['valeur'], errors='coerce')
        # Supprimer les lignes où 'valeur' est NaN (ce sont les lignes qui ne sont pas des nombres)
        joined_df.dropna(subset=['valeur'], inplace=True)
        print(f"Data after filtering non-numeric values: {len(joined_df)} rows")
        
        # Convertir d'abord les colonnes en type str, excepté date_entree
        for col in ['variable', 'id_variable', 'unit', 'id_site', 'type', 'equipement']:
            if col in joined_df.columns:
                joined_df[col] = joined_df[col].astype(str)
        
        # Correction pour la colonne 'date_entree'
        # Convertir en numeric d'abord, puis en datetime avec l'unité 'ms'
        joined_df['date_entree'] = pd.to_datetime(joined_df['date_entree'], unit='ms')

        # 5. Stockage des données pour la tâche suivante
        # On utilise date_format='iso' pour garantir que le format JSON est bien lisible par la tâche de chargement
        kwargs['ti'].xcom_push(key='transformed_data_df', value=joined_df.to_json(orient='records', date_format='iso'))
        print(f"Data transformation completed. Final dataset: {len(joined_df)} rows")
        return f"Successfully processed {len(joined_df)} rows"
    except Exception as e:
        print(f"Error in extract_and_transform_data: {str(e)}")
        raise

def load_data_to_neon(**kwargs):
    """
    Charge les données transformées dans la table 'fact_production' du datamart Neon.
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
        conn = neon_hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Créer une table temporaire
            create_temp_table = """
                CREATE TEMP TABLE IF NOT EXISTS temp_fact_production (
                    variable VARCHAR(255),
                    id_variable VARCHAR(255),
                    valeur DOUBLE PRECISION,
                    unit VARCHAR(255),
                    date_entree TIMESTAMP,
                    id_site VARCHAR(255),
                    type VARCHAR(255),
                    equipement VARCHAR(255)
                );
            """
            cur.execute(create_temp_table)

            # Charger les données dans la table temporaire
            output = StringIO()

            # Convertir les colonnes nécessaires en types appropriés
            df_transformed['valeur'] = pd.to_numeric(df_transformed['valeur'], errors='coerce')
            df_transformed['date_entree'] = pd.to_datetime(df_transformed['date_entree'], errors='coerce')

            df_transformed.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
            output.seek(0)
            cur.copy_from(
                output, 
                'temp_fact_production', 
                sep='\t', 
                columns=list(df_transformed.columns),
                null='\\N'
            )
            
            # Utiliser ON CONFLICT pour insérer ou mettre à jour
            upsert_sql = """
                INSERT INTO fact_production (
                    variable, id_variable, valeur, unit, date_entree, id_site, type, equipement
                )
                SELECT
                    variable, id_variable, valeur, unit, date_entree, id_site, type, equipement
                FROM temp_fact_production
                ON CONFLICT (id_variable, date_entree) DO UPDATE
                SET
                    valeur = EXCLUDED.valeur,
                    unit = EXCLUDED.unit,
                    type = EXCLUDED.type,
                    equipement = EXCLUDED.equipement;
            """
            cur.execute(upsert_sql)
            
            conn.commit()
            print(f"Successfully loaded {len(df_transformed)} rows into fact_production with UPSERT")
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
    dag_id="etl_fact_production_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 7,14,16 * * *",  # Planification pour 7h et 14h
    catchup=False,
    tags=["etl", "datamart", "production"],
    description="ETL pipeline for fact_production table from variables and entries",
    max_active_runs=1,
) as dag:
    
    # Tâche 1 : Création de la table si elle n'existe pas
    create_table_task = PythonOperator(
        task_id="create_fact_production_table",
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