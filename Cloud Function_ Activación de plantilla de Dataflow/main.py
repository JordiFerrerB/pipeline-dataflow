import json
import time
from google.cloud import storage
from google.cloud.storage import Blob
from googleapiclient.discovery import build

PROJECT_ID        = 'preving-bajas-301512'
SCHEMAS_LOCATION  = 'Bajas/schemas/'
TEMPLATE_LOCATION = 'gs://preving-bajas/templates/extract-csv-template'
OUTPUT_DATASET    = 'BAJAS_BQ_STG.'
CSV_SEPARATOR     = '{'

def csv_to_template(event, context):
    storage_bucket = event['bucket']
    file_name = event['name']
    file_extension = file_name[file_name.rindex('.'):]
    print(file_extension)
    if file_extension == '.csv':
        print('Se ha detectado un cambio en ' + file_name)
        entity = file_name[file_name.rindex('/')+1:file_name.index(file_extension)] 

        # Especifica la tabla de salida
        output_table = ''
        try:
            output_table = OUTPUT_DATASET + entity[entity.rindex('.') + 1:] # Quita los puntos del nombre de la tabla final
        except:
            output_table = OUTPUT_DATASET + entity

        json_file = SCHEMAS_LOCATION + entity + '.json'
        input_file = 'gs://' + storage_bucket + '/' + file_name
        print('El esquema de la tabla está en ' + json_file)

        # Lee el JSON del storage
        client = storage.Client()
        gcs_bucket = client.get_bucket(storage_bucket)
        blob = gcs_bucket.get_blob(json_file)
        if blob is not None:
            blob_content = blob.download_as_string().decode('utf-8')
            print('Esquema leído: ', blob_content)
        else:
            print('No se ha encontrado esquema para ', entity, ', generando...')
            blob_content = csv_header_to_schema(gcs_bucket, file_name)
            
        schema_json = json.loads(blob_content)

        # Esquema a string del tipo: 'campo1:tipo1,campo2:tipo2'
        bq_schema = ''
        for field in schema_json:
            bq_schema += field['name']
            bq_schema += ':'
            bq_schema += field['type']
            bq_schema += ','

        bq_schema = bq_schema[:len(bq_schema) - 1]

        print('TABLE SCHEMA', bq_schema)

        # Activa plantilla: CSV a Dataflow
        print('Lanzando tarea en Dataflow...')
        dataflow = build('dataflow', 'v1b3')
        request = dataflow.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location='europe-west1',
            gcsPath=TEMPLATE_LOCATION,
            body={
                'jobName': 'csv-a-bigquery-' + str(int(time.time())),
                'parameters': {
                    'input': input_file,
                    'output': output_table,
                    'schema': bq_schema
                }
            }
        )

        response = request.execute()
        print('RESPUESTA', response)


# A partir de la cabecera del CSV, genera el esquema JSON que se utiliza en la pipeline de Dataflow
def csv_header_to_schema(gcs_bucket, csv_file):
    csv_blob = gcs_bucket.get_blob(csv_file)
    csv_header = csv_blob.download_as_string().decode('utf-8').split('\n')[:1]
    print('CSV HEADER: ', csv_header)
    csv_fields = csv_header[0].split(CSV_SEPARATOR)
    csv_schema = []
    for field in csv_fields:
        schema_field = dict()
        schema_field['name'] = field
        schema_field['type'] = 'STRING'
        csv_schema.append(schema_field)

    blob_content = json.dumps(csv_schema)
    schema_blob = Blob(SCHEMAS_LOCATION + entity + '.json', gcs_bucket)
    schema_blob.upload_from_string(blob_content, content_type="application/json")

    return blob_content