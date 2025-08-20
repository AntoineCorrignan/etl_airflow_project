from __future__ import annotations
import pendulum
import pandas as pd
import psycopg2
from io import StringIO

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def extract_and_transform_data(**kwargs):
    """
    Extrait les données des tables 'companies' et 'addresses', les joint,
    et retourne le DataFrame résultant.
    """
    # 1. Connexion à la base de données source
    source_hook = PostgresHook(postgres_conn_id='administration_recette')
    
    # 2. Requêtes SQL pour extraire les données
    companies_sql = "SELECT id, siret, tva_number FROM companies;"
    addresses_sql = "SELECT address, city, country, region, zip_code FROM addresses;"
    
    # 3. Exécution des requêtes et récupération des données
    companies_df = source_hook.get_pandas_df(companies_sql)
    addresses_df = source_hook.get_pandas_df(addresses_sql)
    
    # 4. Jointure des deux DataFrames
    # Note : Il nous faudrait une clé de jointure. J'ajoute une colonne 'id' temporaire pour l'exemple
    # S'il y a une colonne 'company_id' dans 'addresses', utilisez-la pour une vraie jointure
    # Exemple de jointure sur une clé commune si elle existe
    # joined_df = pd.merge(companies_df, addresses_df, left_on='id', right_on='company_id')
    
    # Pour l'exemple, nous allons faire une simple concaténation si l'association est 1:1 et dans le même ordre
    # Cela ne fonctionnera que si vous êtes certain que le nombre de lignes est identique et dans le bon ordre.
    # Dans un vrai projet, une clé de jointure serait obligatoire.
    joined_df = pd.concat([companies_df, addresses_df], axis=1)

    # 5. Stockage des données pour les passer à la tâche suivante via XCom
    kwargs['ti'].xcom_push(key='transformed_data_df', value=joined_df.to_json())

def load_data_to_neon(**kwargs):
    """
    Charge les données transformées dans la table 'dim_entreprise' du datamart Neon.
    """
    # 1. Récupération des données passées par la tâche précédente
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='extract_and_transform_data', key='transformed_data_df')
    df_transformed = pd.read_json(json_data)
    
    # 2. Connexion à la base de données cible Neon
    neon_hook = PostgresHook(postgres_conn_id='bdd_neon_recette')
    
    # 3. Chargement des données dans la table 'dim_entreprise'
    # La méthode 'copy_from' est la plus efficace pour le chargement en masse.
    
    table_name = 'dim_entreprise'
    
    # Préparation des données pour le chargement en masse (format CSV in-memory)
    from io import StringIO
    output = StringIO()
    df_transformed.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    
    # Utilisation du Hook pour se connecter et charger les données
    conn = neon_hook.get_conn()
    cur = conn.cursor()
    
    try:
        cur.copy_from(output, table_name, sep='\t', columns=df_transformed.columns)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Erreur lors du chargement des données dans Neon :", error)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="etl_dim_entreprise_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Planification manuelle pour le moment
    catchup=False,
    tags=["etl", "datamart"],
) as dag:
    
    # Tâche 1 : Création de la table si elle n'existe pas
    create_table_if_not_exists = PostgresOperator(
        task_id="create_dim_entreprise_table",
        postgres_conn_id="bdd_neon_recette",
        sql="""
            CREATE TABLE IF NOT EXISTS dim_entreprise (
                id VARCHAR(255),
                siret VARCHAR(255),
                tva_number VARCHAR(255),
                address VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                region VARCHAR(255),
                zip_code VARCHAR(255)
            );
        """
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
    create_table_if_not_exists >> extract_and_transform_task >> load_data_to_neon_task

# C'est une modification pour forcer le rechargement du DAG