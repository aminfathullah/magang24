import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import pygsheets

def fetch_table_data(url, payload, date, kabkota):
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
                    data.append([no, date, kabkota, nama, satuan, harga_kemarin, harga_sekarang, perubahan_rp, perubahan_persen])
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
    kabkota_mapping = {
        'ponorogokab': 'Ponorogo',
        'trenggalekkab': 'Trenggalek'
    }

    endpoint_url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    
    for kabkota, kabupaten_kota in kabkota_mapping.items():
        payload = {
            'tanggal': tanggal,
            'kabkota': kabkota,
            'pasar': ''
        }
        data = fetch_table_data(endpoint_url, payload, tanggal, kabupaten_kota)  # Menggunakan nama kabupaten/kota dari mapping
        if data:
            print(f" Multi Threading Harga Rata-Rata Kabupaten {kabupaten_kota} di Tingkat Konsumen Tanggal {tanggal}")
            df = pd.DataFrame(data, columns=headers)
            print(df.to_string(index=False))
            print()
            all_data.append(df)
        else:
            print(f"Tidak ada data yang ditemukan untuk tanggal {tanggal} di {kabupaten_kota}")
            print()

    return all_data

def clean_and_transform_data(df):
    # Menghapus baris yang kolom "SATUAN" tidak memiliki isi
    df = df[df['SATUAN'] != ''].copy()

    # Menghilangkan tanda '-' dari 'NAMA BAHAN POKOK'
    df.loc[:, 'NAMA BAHAN POKOK'] = df['NAMA BAHAN POKOK'].str.replace('-', '', regex=False)

    # Menghilangkan tanda '.' dari 'HARGA KEMARIN' dan menangani nilai '-'
    df.loc[:, 'HARGA KEMARIN'] = df['HARGA KEMARIN'].str.replace('.', '', regex=False)
    df.loc[:, 'HARGA KEMARIN'] = df['HARGA KEMARIN'].replace('-', '0')
    df.loc[:, 'HARGA KEMARIN'] = pd.to_numeric(df['HARGA KEMARIN'], errors='coerce').fillna(0).astype(int)

    # Menghilangkan tanda '.' dari 'HARGA SEKARANG' dan menangani nilai '-'
    df.loc[:, 'HARGA SEKARANG'] = df['HARGA SEKARANG'].str.replace('.', '', regex=False)
    df.loc[:, 'HARGA SEKARANG'] = df['HARGA SEKARANG'].replace('-', '0')
    df.loc[:, 'HARGA SEKARANG'] = pd.to_numeric(df['HARGA SEKARANG'], errors='coerce').fillna(0).astype(int)

    # Menghilangkan tanda '.' dari 'PERUBAHAN (Rp)' dan menangani nilai '-'
    df.loc[:, 'PERUBAHAN (Rp)'] = df['PERUBAHAN (Rp)'].replace('-', '0')
    df.loc[:, 'PERUBAHAN (Rp)'] = df['PERUBAHAN (Rp)'].str.replace('.', '', regex=False)
    df.loc[:, 'PERUBAHAN (Rp)'] = pd.to_numeric(df['PERUBAHAN (Rp)'], errors='coerce').fillna(0).astype(int)

    # Menghilangkan tanda '%' dan mengganti ',' dengan '.' pada 'PERUBAHAN (%)'
    df.loc[:, 'PERUBAHAN (%)'] = df['PERUBAHAN (%)'].str.replace('%', '', regex=False)
    df.loc[:, 'PERUBAHAN (%)'] = df['PERUBAHAN (%)'].str.replace(',', '.', regex=False)
    df.loc[:, 'PERUBAHAN (%)'] = df['PERUBAHAN (%)'].replace('-', '0')
    df.loc[:, 'PERUBAHAN (%)'] = pd.to_numeric(df['PERUBAHAN (%)'], errors='coerce').fillna(0) / 100

    df.loc[:, 'ID_WILAYAH'] = df['Kabupaten/Kota'].map({
        "Pasuruan": 3514,
        "Ponorogo": 3502,
        "Probolinggo": 3513,
        "Sampang": 3527,
        "Sidoarjo": 3515,
        "Situbondo": 3512,
        "Sumenep": 3529,
        "Trenggalek": 3503,
        "Tuban": 3523,
        "Tulungagung": 3504
    }).astype('Int64')

    df.loc[:, 'NO'] = df.index + 1
    df.fillna(0, inplace=True)

    return df

def main():
    start_date = datetime(2023, 4, 24)
    end_date = datetime(2024, 4, 24)
    headers = ["NO", "Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN", "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    all_data = []

    current_date = start_date
    while current_date <= end_date:
        daily_data = fetch_data_for_date(current_date)
        if daily_data:
            all_data.extend(daily_data)
        current_date += timedelta(days=1)

    # Gabungkan semua data ke dalam satu DataFrame
    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)

        # transformasi dan pembersihan data
        df_cleaned = clean_and_transform_data(df_all)

        # filter data untuk Beras Premium dan Beras Medium
        df_filtered = df_cleaned[df_cleaned['NAMA BAHAN POKOK'].isin([" Beras Premium", " Beras Medium"])]

        # simpan data ke file Parquet
        pq.write_table(pa.Table.from_pandas(df_filtered), 'data_looker.parquet')
        print("Data berhasil disimpan ke file Parquet.")

        # autentikasi ke Google Sheets menggunakan service account
        try:
            gc = pygsheets.authorize(service_account_file="magang-423902-f0e642da9b61.json")
            print("Berhasil terautentikasi ke Google Sheets.")
        except Exception as e:
            print(f"Gagal terautentikasi ke Google Sheets: {e}")
            return

        # membuka spreadsheet dan worksheet yang sesuai 
        try:
            gs = gc.open("data looker")
            worksheet = gs.worksheet('index', 0)
            print("Worksheet berhasil dibuka.")
        except Exception as e:
            print(f"Gagal membuka worksheet: {e}")
            return

        # bersihkan worksheet
        try:
            worksheet.clear('A1')
            print("Worksheet berhasil dibersihkan.")
        except Exception as e:
            print(f"Gagal membersihkan worksheet: {e}")
            return

        # menulis dataframe ke Google Sheets
        try:
            worksheet.set_dataframe(df_filtered, (1, 1), extend=True, copy_index=True)
            print("Data berhasil ditulis ke Google Sheets.")
        except Exception as e:
            print(f"Gagal menulis data ke Google Sheets: {e}")

if __name__ == "__main__":
    main()
