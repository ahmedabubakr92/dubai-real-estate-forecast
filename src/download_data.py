import requests
import os

# Directory setup for raw data
raw_data_dir = 'data/raw'
os.makedirs(raw_data_dir, exist_ok=True)

# URLs for datasets
urls = {
    "transactions.csv": "https://www.dubaipulse.gov.ae/dataset/3b25a6f5-9077-49d7-8a1e-bc6d5dea88fd/resource/a37511b0-ea36-485d-bccd-2d6cb24507e7/download/transactions.csv",
    "rent_contracts.csv": "https://www.dubaipulse.gov.ae/dataset/00768c45-f014-4cc6-937d-2b17dcab53fb/resource/765b5a69-ca16-4bfd-9852-74612f3c4ea6/download/rent_contracts.csv"
}

# Downloading datasets
for file_name, url in urls.items():
    file_path = os.path.join(raw_data_dir, file_name)
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"{file_name} downloaded successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file_name}: {e}")