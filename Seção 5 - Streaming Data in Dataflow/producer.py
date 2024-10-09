import csv
import time
import os

from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

sa = os.getenv("SA")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

topico = os.getenv("TOPIC_VOOS")
publisher = pubsub_v1.PublisherClient()

entrada = r"data/voos_sample.csv"     

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico,row)
        time.sleep(2)