import json

import requests

url = "https://api.diffbot.com/v3/crawl/data?token=0cbf79f52f77ebd1749cdad8b93f0462&name=prolampsales-crawl-2023"

headers = {
    "accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}

response = requests.get(url, headers=headers)

print(json.loads(response))
