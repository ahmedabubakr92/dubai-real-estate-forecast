import os
import requests

# Define folder paths
raw_data_dir = "data/raw"

# Make directories if not exist
os.makedirs(raw_data_dir, exist_ok=True)

# URLs for the datasets (replace these with the actual URLs or paths)
dataset_urls = {
    'transactions': 'https://www.dubaipulse.gov.ae/dataset/3b25a6f5-9077-49d7-8a1e-bc6d5dea88fd/resource/a37511b0-ea36-485d-bccd-2d6cb24507e7/download/transactions.csv',
    'rent_contracts': 'https://www.dubaipulse.gov.ae/dataset/00768c45-f014-4cc6-937d-2b17dcab53fb/resource/765b5a69-ca16-4bfd-9852-74612f3c4ea6/download/rent_contracts.csv',
    'valuations': 'https://www.dubaipulse.gov.ae/dataset/ff09ccad-6047-4793-a776-9d282abb5cdb/resource/5921b912-d938-4d04-a4d1-a391b125a459/download/valuation.csv',
    'projects': "https://www.dubaipulse.gov.ae/dataset/0b782e64-5950-4507-8f6e-02a0c30c7054/resource/db35b0cd-d291-4dde-b176-9b8d5765c7d9/download/projects.csv",
    "units": "https://www.dubaipulse.gov.ae/dataset/85462a5b-08dc-4325-9242-676a0de4afc4/resource/7d4deadf-c9bc-47a4-85de-998d0ce38bf3/download/units.csv",
    'buildings': "https://www.dubaipulse.gov.ae/dataset/04168da5-9d10-4fe3-bd29-6dba97751435/resource/6450b4a5-f9c5-4aca-8e64-b38aa8db4c5a/download/buildings.csv"
}

# Download function
def download_dataset(url, filename, output_dir):
    response = requests.get(url)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'wb') as f:
        f.write(response.content)
    print(f'Download complete: {filename}')

# Download datasets
for dataset, url in dataset_urls.items():
    download_dataset(url, f'{dataset}.csv', raw_data_dir)