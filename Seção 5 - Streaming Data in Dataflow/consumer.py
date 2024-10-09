import csv
import time
import os

from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

sa = os.getenv("SA")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

subscription = os.getenv("SUBS_VOOS")
subscriber = pubsub_v1.SubscriberClient()

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack()

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)