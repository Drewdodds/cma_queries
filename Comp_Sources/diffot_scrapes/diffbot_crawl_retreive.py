import json

import requests

import pandas as pd

url = "https://api.diffbot.com/v3/crawl/data?token=0cbf79f52f77ebd1749cdad8b93f0462&name=prolampsales-crawl-2023"

headers = {
    "accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}

response = requests.get(url, headers=headers)

data = response.json()

df = pd.DataFrame.from_records(data)

print(df)

# need to build an extract, transform, load function for diffbot
