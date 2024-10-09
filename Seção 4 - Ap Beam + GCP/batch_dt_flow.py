import apache_beam as beam
import os

from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

sa =  os.getenv("SA")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options = {
    'project': os.getenv("PROJECT"),
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://curso-ap-beam/temp',
    'temp_location': 'gs://curso-ap-beam/temp',
    'template_location': 'gs://curso-ap-beam/template/batch_job_df_gcs_voos'
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText(r"gs://curso-ap-beam/RAW/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro())
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
#  | "Mostrar Resultados" >> beam.Map(print)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText(r"gs://curso-ap-beam/RAW/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro())
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
#  | "Mostrar Resultados QTD" >> beam.Map(print)
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | "Group By" >> beam.CoGroupByKey()
    | "Saida para GCP" >> beam.io.WriteToText(r"gs://curso-ap-beam/REFINED/voos_atrasados_qtd.csv")
)

p1.run()