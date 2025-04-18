from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

import os
# se seteaza calea catre cheia json
# Obține calea completă către folderul în care se află acest script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Calea către fișierul de autentificare (în același folder)
KEY_PATH = os.path.join(BASE_DIR, "marketingdatahub-452910-cb633ca91402.json")


nume_tabel='marketingdatahub-452910.Google_Ads_Data.ads_AccountConversionStats_5693305474'

# folder unde se salveaza graficele (facut sa functioneze pe orice device)
folder_output = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Grafice')

# Creează folderul dacă nu există
if not os.path.exists(folder_output):
    os.makedirs(folder_output)



# Inițializează clientul BigQuery (autentificarea trebuie să fie configurată cu cheia JSON)
client = bigquery.Client.from_service_account_json(KEY_PATH)



print(f"Procesare: {nume_tabel}")

# query necesar
# pun acest f pt ca am nevoie de formatarea de tip string pt cand adaug nume_tabel
query = f"""
  SELECT
    FORMAT_DATE('%Y-%m', segments_date) AS luna,
    customer_id,
    SUM(metrics_conversions_value) AS cost_total
  FROM
    `{nume_tabel}`
  WHERE
    segments_date BETWEEN
      DATE('2020-01-01') AND
      LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  GROUP BY
    luna, customer_id
  ORDER BY
    customer_id, luna;
"""
# am ales un interval mare de timp, din 2020 pana in prezent
# daca am luni absente, ele nu vor fi luate in seama ca date
# last day se ocupa sa mi seteze ultima zi a ultimei luni pe care am avut o
# e becesar

# execut query-ul si sustrag datele in tabel
df = client.query(query).to_dataframe()

# pt fiecare client o sa afisez cate un tabel
for customer in df['customer_id'].unique():
    df_customer = df[df['customer_id'] == customer]

    plt.figure(figsize=(10, 6))
    plt.plot(df_customer['luna'], df_customer['cost_total'], marker='o', linestyle='-', color='teal')
    plt.xticks(rotation=45)
    plt.title(f'Cheltuieli lunare - Customer ID {customer}')
    plt.xlabel('Lună')
    plt.ylabel('Cost total (mil. unitate monetară)')
    plt.grid(True)
    plt.tight_layout()

    output_path = os.path.join(folder_output, f"customer_{customer}.png")
    plt.savefig(output_path)
    plt.close()

    print(f"✅ Grafic salvat pentru Customer ID {customer}: {output_path}")
