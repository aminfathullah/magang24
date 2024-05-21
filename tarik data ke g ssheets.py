import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import pygsheets

def fetch_table_data(url, payload, date):
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')
            data = []
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 7:  
                    no = cols[0].text.strip()
                    nama = cols[1].text.strip()
                    satuan = cols[2].text.strip()
                    harga_kemarin = cols[3].text.strip()
                    harga_sekarang = cols[4].text.strip()
                    perubahan_rp = cols[5].text.strip()
                    perubahan_persen = cols[6].text.strip()
                    data.append([no, date, 'Kota Batu', nama, satuan, harga_kemarin, harga_sekarang, perubahan_rp, perubahan_persen])
            return data
        else:
            print("Tabel tidak ditemukan")
    else:
        print("Gagal mengambil data dari endpoint")
    return []

def fetch_data_for_date(date):
    headers = ["NO", "Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN", "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    all_data = [] 
    tanggal = date.strftime("%Y-%m-%d")
    payload = {
        'tanggal': tanggal,
        'kabkota': 'batukota',
        'pasar': ''
    }

    endpoint_url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    data = fetch_table_data(endpoint_url, payload, tanggal)
    if data:
        print(f" Multi Threading Harga Rata-Rata Kabupaten Batu di Tingkat Konsumen Tanggal {tanggal}")
        print("Pasar : Pasar Senenan, Pasar Ki Lemah Duwur, Pasar Baru Bancaran")
        df = pd.DataFrame(data, columns=headers)
        print(df.to_string(index=False))
        print()  
        all_data.append(df)  
    else:
        print(f"Tidak ada data yang ditemukan untuk tanggal {tanggal}")
        print()
    
    return all_data

def main():
    start_date = datetime(2023, 4, 24)
    end_date = datetime(2024, 4, 24)
    headers = ["NO", "Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN", "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    all_data = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        future_to_date = {executor.submit(fetch_data_for_date, date): date for date in date_range}
        for future in concurrent.futures.as_completed(future_to_date):
            date = future_to_date[future]
            try:
                data_for_date = future.result()
                if data_for_date:
                    all_data.extend(data_for_date)
            except Exception as exc:
                print(f"Exception occurred while fetching data for date {date}: {exc}")

    final_df = pd.concat(all_data, ignore_index=True)
    
    harga_clean = pd.read_parquet('harga.parquet')
    harga_clean[['HARGA SEKARANG','Tanggal','NAMA BAHAN POKOK']].groupby(['Tanggal', 'NAMA BAHAN POKOK']).mean()

    gc = pygsheets.authorize(service_account_file='magang-423902-f0e642da9b61.json')
    
    
    gs = gc.open('Data harga')
    gs.worksheet('index', 0).clear('A1')
    gs.worksheet('index', 0).set_dataframe(harga_clean[['HARGA SEKARANG','Tanggal','NAMA BAHAN POKOK']].groupby(['Tanggal', 'NAMA BAHAN POKOK']).mean(), [1,1], extend = True, copy_index= True)
    
    
    
    print("Gabungan Data untuk Semua Tanggal:")
    
    print(final_df)

    output_file = 'multithreading_kotabatu.parquet'
    table = pa.Table.from_pandas(final_df)
    pq.write_table(table, output_file)

if __name__ == "__main__":
    main()
