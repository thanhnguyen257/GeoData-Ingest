import requests
import json
import os

class DataIngestionTask:
    def __init__(self, web_service_url, file_path):
        self.web_service_url = web_service_url
        self.file_path = file_path

    def __call__(self):
        try:
            response = requests.get(self.web_service_url)
            response.raise_for_status()

            data = response.json()

            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

            with open(self.file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"Error processing {self.web_service_url}: {e}")
