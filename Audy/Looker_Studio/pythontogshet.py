import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import pygsheets

def fetch_table_data(url, payload, date):
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raise HTTPError for bad responses
    except requests.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if table:
        rows = table.find_all('tr')
        data = []
        for row in rows[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) >= 7:
                no = cols[0].text.strip()
                nama = cols[1].text.strip()
                satuan = cols[2].text.strip()
                harga_kemarin = cols[3].text.strip()
                harga_sekarang = cols[4].text.strip()
                perubahan_rp = cols[5].text.strip()
                perubahan_persen = cols[6].text.strip()
                data.append([no, date, 'Kab Malang', nama, satuan, harga_kemarin, harga_sekarang, perubahan_rp, perubahan_persen])
        return data
    else:
        print(f"Tabel tidak ditemukan untuk {date}")
        return []

def fetch_data_for_date(date):
    headers = ["NO", "Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN", "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    tanggal = date.strftime("%Y-%m-%d")
    payload = {
        'tanggal': tanggal,
        'kabkota': 'malangkab',
        'pasar': ''
    }

    endpoint_url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    data = fetch_table_data(endpoint_url, payload, tanggal)
    if data:
        df = pd.DataFrame(data, columns=headers)
        return df
    else:
        print(f"Tidak ada data yang ditemukan untuk tanggal {tanggal}")
        return pd.DataFrame(columns=headers)

def preprocess_data(df):
    df = df[df['SATUAN'] != '']  # Filter out rows where 'SATUAN' is empty
    df['NAMA BAHAN POKOK'] = df['NAMA BAHAN POKOK'].str.replace('-', '', regex=False)  # Remove hyphens

    # Convert columns to appropriate types
    df['HARGA KEMARIN'] = df['HARGA KEMARIN'].str.replace('.', '', regex=False).replace('-', '0').astype(int)
    df['HARGA SEKARANG'] = df['HARGA SEKARANG'].str.replace('.', '', regex=False).replace('-', '0').astype(int)
    df['PERUBAHAN (Rp)'] = df['PERUBAHAN (Rp)'].str.replace('.', '', regex=False).replace('-', '0').astype(int)
    df['PERUBAHAN (%)'] = df['PERUBAHAN (%)'].str.replace('%', '', regex=False).str.replace(',', '.', regex=False).replace('-', '0').astype(float) / 100
    df['Tanggal'] = pd.to_datetime(df['Tanggal'])

    # Normalize 'HARGA KEMARIN'
    harga_kemarin_min = df['HARGA KEMARIN'].min()
    harga_kemarin_max = df['HARGA KEMARIN'].max()
    df['normalize_harga_kemarin'] = (df['HARGA KEMARIN'] - harga_kemarin_min) / (harga_kemarin_max - harga_kemarin_min)

    # Map 'Kabupaten/Kota' to 'id_kab' using the provided dictionary
    id_kab_dict = {
        "Kab Lumajang": "3508",
        "Kab Madiun": "3519",
        "Kab Magetan": "3520",
        "Kab Mojokerto": "3516",
        "Kab Malang": "3507",
        "Kab Nganjuk": "3518",
        "Kab Pacitan": "3501",
        "Kab Pamekasan": "3528"
    }
    df['id_kab'] = df['Kabupaten/Kota'].map(id_kab_dict).fillna('-')

    return df

def upload_to_google_sheets(df, sheet_name, worksheet_index):
    try:
        gc = pygsheets.authorize(service_account_file="D:\Coding\magang-423902-f0e642da9b61.json")
        gs = gc.open(sheet_name)
        worksheet = gs.worksheet('index', worksheet_index)
        worksheet.clear('A1')
        
        # Ensure all numeric columns are properly formatted before uploading
        numeric_cols = ['HARGA KEMARIN', 'HARGA SEKARANG', 'PERUBAHAN (Rp)', 'PERUBAHAN (%)', 'normalize_harga_kemarin']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

        # Convert the DataFrame to strings to avoid issues with large numbers being auto-formatted
        df = df.applymap(lambda x: str(x) if isinstance(x, (int, float)) else x)

        worksheet.set_dataframe(df, 'A1', extend=True, copy_index=True)
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")

def main():
    start_date = datetime(2023, 4, 24)
    end_date = datetime.now()
    all_data = []

    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_date = {executor.submit(fetch_data_for_date, date): date for date in date_range}
        for future in concurrent.futures.as_completed(future_to_date):
            date = future_to_date[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data.append(df)
            except Exception as exc:
                print(f"Exception occurred while fetching data for date {date}: {exc}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = preprocess_data(final_df)

        output_file = 'output_harga_kabMalang.parquet'
        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, output_file)

        upload_to_google_sheets(final_df, 'Data harga', 0)

        print("Gabungan Data untuk Semua Tanggal:")
        print(final_df)
    else:
        print("Tidak ada data yang dapat diproses.")

if __name__ == "__main__":
    main()


df = pd.read_parquet('D:/Coding/Python/magang24/Audy/Looker_Studio/output_harga_kabMalang.parquet')
gc = pygsheets.authorize(service_account_file="D:\Coding\magang-423902-f0e642da9b61.json")
gs = gc.open("Data harga")
worksheet = gc.open("Data harga").worksheet('index', 0)
gc.open("Data harga").worksheet('index', 0).clear('A1')
gc.open("Data harga").worksheet('index', 0).set_dataframe(df, 'A1', extend=True, copy_index=True)
df.round(5).loc[0, 'normalize_harga_kemarin']
import math
df['normalize_harga_kemarin'] = round(df['normalize_harga_kemarin'], 5)
