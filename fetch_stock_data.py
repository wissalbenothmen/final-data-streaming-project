import requests
from kafka import KafkaProducer
import json
import time

API_KEY = "FXMGLC46AQCWXVE2"
KAFKA_TOPIC = "stock-stream"
KAFKA_SERVER = "localhost:9092"

def fetch_data():
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        print(response.json())  # Print the entire response
        return response.json().get('data', [])  # Safely get 'data' or return an empty list if 'data' is not present
    else:
        print("Error fetching data", response.status_code)
        return []


def produce_data():
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        news_data = fetch_data()
        for article in news_data:
            producer.send(KAFKA_TOPIC, article)
            time.sleep(1)  
        time.sleep(60)  

if __name__ == "__main__":
    produce_data()
