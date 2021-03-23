import base64
from google.cloud import bigquery

def query_hechos_por_agente(event, context):
     partitioning = bigquery.RangePartitioning(
          field="id_particion",
          range_=bigquery.PartitionRange(start=1, end=10000, interval=1),
     )


    # Ejecuta la vista y guarda en DWH
     job_config = bigquery.QueryJobConfig(destination="gestionfinanciera.HECHOS_GestionFinanciera.Hechos_x_Agente", write_disposition="WRITE_TRUNCATE", range_partitioning=partitioning)
     
     client = bigquery.Client()
     query = "SELECT * FROM gestionfinanciera.ETL_GestionFinanciera.Hechos_x_Agente"
     client.query(query,job_config=job_config)
