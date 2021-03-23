# coding=utf-8
import logging
import os
import re
import io
import json
import csv
import apache_beam as beam
from datetime import datetime
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.value_provider import RuntimeValueProvider, StaticValueProvider
from apache_beam.io.gcp.internal.clients import bigquery

STORAGE_BUCKET = 'productividad-facturacion'
CSV_FIELD_DELIMITER = '{'

# Formato de entrada de las fechas
DATE_FORMAT = '%m-%d-%Y'
DATETIME_FORMAT = '%m-%d-%Y %H:%M:%S'

#Define los parámetros de entrada
class Parameters(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str,required=False, default='gs://' + STORAGE_BUCKET + '/templates/Provincia.csv')
        parser.add_value_provider_argument('--output', type=str,required=False, default='createTemplate.Provincia')
        parser.add_value_provider_argument('--schema', type=str,required=False, default='CodProvincias:STRING,NomProvincias:STRING')

class ParseLine(beam.DoFn):
    columns = []
    schema = ''

    def __init__(self, schema=''):
        self.schema = schema

    # Convierte un valor al tipo dado
    def value_to_type(self, value, type):
        type = type.lower()
        if value:
            if type == 'integer':
                return int(value)
            elif type == 'float':
                return float(value)
            elif type == 'bytes':
                return value.encode()
            elif type == 'date':
                return datetime.strptime(value, DATE_FORMAT).date()
            elif type == 'datetime':
                return datetime.strptime(value, DATETIME_FORMAT)
            elif type == 'boolean':
                return value.lower() in ['true', '1']
            else:
                return value
        else:
            return None

    # Procesa cada línea del CSV leído
    def process(self, string_input, columns):
        column_list = []
        parsed_values = []
        values = re.split(CSV_FIELD_DELIMITER, re.sub('\r\n', '', re.sub('"', '', string_input)))
        fields = columns['fields']
        for i in range(len(values)):
            field = fields[i]
            value = values[i]
            column_list.append(field['name'])

            parsed_value = self.value_to_type(value, field['type'])
            parsed_values.append(parsed_value)

        row = dict(zip(column_list, parsed_values))
        yield row

# Esquema a diccionario para BigQuery {'fields':[{'name': 'nombre1', 'type': 'tipo1'},{...}]}
class ParseSchema(beam.DoFn):
    def process(self, schema):
        fields = schema.get().split(',')
        table_schema = {'fields': []}
        for field in fields:
            new_field = {}
            new_field['name'] = field[:field.index(':')]
            new_field['type'] = field[field.index(':')+1:]
            new_field['mode'] = 'NULLABLE'
            table_schema['fields'].append(new_field)

        print(table_schema)
        yield table_schema

def run(argv=None):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    params = pipeline_options.view_as(Parameters)

    with beam.Pipeline(options=pipeline_options) as p:
        table_schema = (p | beam.Create([params.schema])
        | "Esquema a diccionario" >> beam.ParDo(ParseSchema()))

        table_schema_dict = beam.pvalue.AsSingleton(table_schema)

        (p | "Leee CSV" >> beam.io.ReadFromText(params.input,skip_header_lines=1)
        | "Datos a diccionario" >> beam.ParDo(ParseLine(), table_schema_dict)
#        | beam.Map(print))
        | "Datos a BigQuery" >> 
            beam.io.WriteToBigQuery(
                params.output,
                schema=lambda dest, schema: schema,
                schema_side_inputs=(table_schema_dict,),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))

    p.run()

# logging.getLogger().setLevel(logging.DEBUG)
run()