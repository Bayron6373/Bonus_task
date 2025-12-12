#Import libraries for scrapping
import requests
from bs4 import BeautifulSoup
import pandas as pd

#Kafka
from kafka import KafkaProducer,KafkaConsumer
import json
import time


url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"}
response = requests.get(url,headers = headers)

soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table", {"class": "sortable"})

headers = [th.get_text(strip=True) for th in table.find_all("th")]

data = []
for row in table.find("tbody").find_all("tr"):
    cells = [td.get_text(strip=True) for td in row.find_all("td")]
    data.append(cells)
    
df = pd.DataFrame(data,columns = headers)

#2.Clean the data
#--Remove rows with missing or invalid values
df = df.dropna(subset=['Date', 'Population'])
df = df.drop(columns=["Notes"])
#--convert numbers / dates
df["Population"]=df["Population"].str.replace(",","").astype(int)
df['% ofworld'] = df['% ofworld'].str.replace('%', '').astype(float)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

#--rename columns
df = df.rename(columns={
    'Location': 'country',
    'Population': 'population',
    '% ofworld': 'percent_world',
    'Date': 'date',
    'Source (official or fromtheUnited Nations)':'source',
})

#--trim strings, lowercase
df['country'] = df['country'].str.strip()

df = df.reset_index(drop=True)

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer =lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "bonus_22B030147"

try:
    for index, row in df.iterrows():
        country = row['country']
        population = row['population']
        percent_world = row['percent_world']
        date = row['date'].strftime("%Y-%m-%d")
        source = row['source']
        message = {"id":index,
                   "country":country,
                   "population":population,
                   "percent_world":percent_world,
                   "date":date,
                   "source":source,
                   }
        producer.send(topic_name,value=message)
        print(f"Send: {message}")
        time.sleep(2)
except Exception:
    producer.close()
    
df.to_csv("cleaned_data.csv",index=False)