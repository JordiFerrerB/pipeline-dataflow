import base64
import json
import time
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery

PROJECT_ID = "preving-cap"
ETL_DATASET = "CAP_BQ_ETL"
CTRL_DATASET = "CAP_CTRL_ETL"

""" PROJECT_ID = "preving-personas"
ETL_DATASET = "PERSONAS_BQ_ETL"
CTRL_DATASET = "PERSONAS_CTRL_ETL" """

credentials, _ = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

bq_client = bigquery.Client(credentials=credentials)

def run_etl(event, context):
    pubsub_message = json.loads(
        base64.b64decode(event['data']).decode('utf-8'))

    dataset, table = get_jobConfiguration(pubsub_message)

    set_updated_table(table)
    get_etl_state()

def get_jobConfiguration(pubsub_message):
    event = pubsub_message['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']
    data = {}
    if 'load' in event:
        data = event['load']
    elif 'query' in event:
        data = event['query']

    return data['destinationTable']['datasetId'], data['destinationTable']['tableId']

def get_etl_state():
    groups = run_query("""SELECT * FROM `{}.{}.Grupos` ORDER BY Grupo ASC """.format(PROJECT_ID, CTRL_DATASET))

    for row in groups:
        group = row['Grupo']
        state = row['Estado']
        if state == 'ESPERA':
            # Si el grupo está en espera, pero sus dependiencias están actualizadas, ejecuta sus vistas
            is_ready = check_group_updates(group)
            if is_ready:
                run_group_views(group)
                set_group_state(group, 'ACTUALIZADO')
            return

    # Si todos los grupos están actualizados, devuelve el dataset de control al estado inicial
    print('SE HA TERMINADO EL PROCESO DE ETL')
    set_initial_state()
    return


# Cuando se recibe que se ha terminado de actualizar una tabla, se indica en la tabla de Dependencias
def set_updated_table(table):
    # Espera a que no haya trabajos ejecutándose en la tabla de Dependencias
    while table_has_jobs('Dependencias'):
        print('Esperando a trabajo pendiente...')
        time.sleep(2)

    query = "UPDATE {}.{}.Dependencias SET Estado = 'ACTUALIZADO' WHERE LOWER(Tabla) like '{}%'".format(PROJECT_ID, CTRL_DATASET, table.lower())
    result = run_query(query)

    print('Tabla actualizada', table)

def table_has_jobs(table):
    curr_jobs = bq_client.list_jobs(project=PROJECT_ID, state_filter='running')
    for job in curr_jobs:
        # Si hay un trabajo pendiente en la tabla especificada, espera unos segundos y vuelve a probar
        if(table in job.query):
            return True

    return False

# Comprueba si se han actualizado todas las tablas de las que depende un grupo
def check_group_updates(group):
    query = "SELECT Estado FROM {}.{}.Dependencias WHERE Grupo BETWEEN 1 and {}".format(PROJECT_ID, CTRL_DATASET, group)
    result = run_query(query)

    is_ready = False
    for row in result:
        # Si alguna de las dependencias está en espera, devuelve falso
        if row['Estado'] == 'ESPERA':
            print('El grupo ' + str(group) + ' tiene dependencias en espera.')
            return False
        is_ready = True

    return is_ready

# Ejecuta las vistas asociadas con el grupo indicado
def run_group_views(group):
    #Selecciona las vistas del grupo
    query = "SELECT Vista_ETL, Dataset_Destino, Tabla_Destino FROM {}.{}.Vistas WHERE Grupo = {}".format(PROJECT_ID, CTRL_DATASET, group)
    result = run_query(query)

    for row in result:
        print('Ejecutando vista del etl: ' + row['Vista_ETL'])
        destination_table = "{}.{}.{}".format(PROJECT_ID, row['Dataset_Destino'], row['Tabla_Destino'])
        job_config = bigquery.QueryJobConfig(destination=destination_table, write_disposition="WRITE_TRUNCATE")

        # Ejecuta la vista con tabla de destino según corresponda
        view_query = "SELECT * FROM {}.{}.{}".format(PROJECT_ID, ETL_DATASET, row['Vista_ETL'])
        query_job = bq_client.query(view_query, job_config=job_config, retry=bigquery.DEFAULT_RETRY)

def set_group_state(group, state):
    query = "UPDATE {}.{}.Grupos SET Estado = '{}' Where Grupo = {}".format(PROJECT_ID, CTRL_DATASET, state, group)
    run_query(query)

def set_initial_state():
    reset_groups = "UPDATE {}.{}.Grupos SET Estado = 'ESPERA' Where True".format(PROJECT_ID, CTRL_DATASET)
    reset_dependences = "UPDATE {}.{}.Dependencias SET Estado = 'ESPERA' Where True".format(PROJECT_ID, CTRL_DATASET)
    run_query(reset_groups)
    run_query(reset_dependences)

def run_query(query):
    query_job = bq_client.query(query, retry=bigquery.DEFAULT_RETRY)
    result = query_job.result()

    return result