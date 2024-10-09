import apache_beam as beam
import os

from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

sa = os.getenv("SA")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options = {
    'project': os.getenv("PROJECT"),
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://curso-ap-beam/temp',
    'temp_location': 'gs://curso-ap-beam/temp',
    'template_location': 'gs://curso-ap-beam/template/batch_job_df_gcs_big_query',
    'save_main_session': True
    
   }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

def criar_dict_nivel1(record):
    """
    Cria um dicionário de nível 1 a partir de um registro.
    
    Args:
    - record (list): Lista contendo os dados. Espera-se que o primeiro elemento seja o nome do aeroporto e o segundo seja uma lista de dados associados.

    Returns:
    - dict: Dicionário contendo o nome do aeroporto ('airport') e uma lista de valores ('lista').
    """
    dict_ = {}  # Inicializa um dicionário vazio
    dict_['airport'] = record[0]  # Adiciona o nome do aeroporto ao dicionário
    dict_['lista'] = record[1]  # Adiciona a lista associada ao aeroporto
    return dict_  # Retorna o dicionário

def desaninhar_dict(record):
    """
    Desaninha um dicionário aninhado, transformando suas chaves internas em chaves de nível superior, concatenando os nomes das chaves.
    
    Args:
    - record (dict): Um dicionário possivelmente aninhado.

    Returns:
    - dict: Um novo dicionário com todas as chaves em nível superior.
    """
    def expand(key, value):
        if isinstance(value, dict):  # Verifica se o valor é outro dicionário
            # Expande o dicionário interno concatenando as chaves
            return [(key + '_' + k, v) for k, v in desaninhar_dict(value).items()]
        else:
            return [(key, value)]  # Se não for um dicionário, mantém a chave e o valor
    
    # Constrói uma lista de pares chave-valor para todas as entradas do dicionário
    items = [item for k, v in record.items() for item in expand(k, v)]
    return dict(items)  # Converte a lista de pares de volta para um dicionário

def criar_dict_nivel0(record):
    """
    Cria um dicionário de nível superior (nível 0) a partir de um registro com chaves específicas.
    
    Args:
    - record (dict): Um dicionário contendo as chaves 'airport', 'lista_Qtd_Atrasos', e 'lista_Tempo_Atrasos'.

    Returns:
    - dict: Dicionário contendo o nome do aeroporto e os primeiros valores de 'Qtd_Atrasos' e 'Tempo_Atrasos'.
    """
    dict_ = {}  # Inicializa um dicionário vazio
    dict_['airport'] = record['airport']  # Adiciona o nome do aeroporto
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]  # Pega o primeiro valor de Qtd_Atrasos
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]  # Pega o primeiro valor de Tempo_Atrasos
    return dict_  # Retorna o dicionário


table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = os.getenv("TABELA")

Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText(r"gs://curso-ap-beam/RAW/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro())
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText(r"gs://curso-ap-beam/RAW/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro())
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | beam.CoGroupByKey()
    | beam.Map(lambda record: criar_dict_nivel1(record))
    | beam.Map(lambda record: desaninhar_dict(record))
    | beam.Map(lambda record: criar_dict_nivel0(record)) 
    | beam.io.WriteToBigQuery(
                              tabela,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://curso-ap-beam/temp'
                              )

)

p1.run()