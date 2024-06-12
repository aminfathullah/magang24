import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import pygsheets

# Define a function to fetch table data from the specified URL with given payload and date.
def fetch_table_data(url, payload, date, kabkota_name, retries=3):
    for _ in range(retries):
        try:
            response = requests.post(url, data=payload, timeout=10)  # Set timeout to 10 seconds
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                table = soup.find('table')
                if table:
                    rows = table.find_all('tr')
                    data = []
                    for row in rows[1:]:
                        cols = row.find_all('td')
                        if len(cols) >= 7:  
                            nama = cols[1].text.strip()
                            satuan = cols[2].text.strip()
                            harga_kemarin = cols[3].text.strip()
                            harga_sekarang = cols[4].text.strip()
                            perubahan_rp = cols[5].text.strip()
                            perubahan_persen = cols[6].text.strip()
                            data.append([date, kabkota_name, nama, satuan, harga_kemarin, harga_sekarang, perubahan_rp, perubahan_persen])
                    return data
                else:
                    print("Tabel tidak ditemukan")
            else:
                print("Gagal mengambil data dari endpoint, status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}. Retrying...")
    return []

# Define a function to transform the fetched data into the desired format.
def transform_data(df):
    df['NAMA BAHAN POKOK'] = df['NAMA BAHAN POKOK'].str.replace('-', '', regex=False)
    df['HARGA KEMARIN'] = df['HARGA KEMARIN'].str.replace('.', '', regex=False)
    df['HARGA KEMARIN'] = df['HARGA KEMARIN'].replace('-', '0')
    df['HARGA KEMARIN'] = pd.to_numeric(df['HARGA KEMARIN'], errors='coerce').fillna(0).astype(int)
    df['HARGA SEKARANG'] = df['HARGA SEKARANG'].str.replace('.', '', regex=False)
    df['HARGA SEKARANG'] = df['HARGA SEKARANG'].replace('-', '0')
    df['HARGA SEKARANG'] = pd.to_numeric(df['HARGA SEKARANG'], errors='coerce').fillna(0).astype(int)
    df['PERUBAHAN (Rp)'] = df['PERUBAHAN (Rp)'].replace('-', '0')
    df['PERUBAHAN (Rp)'] = df['PERUBAHAN (Rp)'].str.replace('.', '', regex=False)
    df['PERUBAHAN (Rp)'] = pd.to_numeric(df['PERUBAHAN (Rp)'], errors='coerce').fillna(0).astype(int)
    df['PERUBAHAN (%)'] = df['PERUBAHAN (%)'].str.replace('%', '', regex=False)
    df['PERUBAHAN (%)'] = df['PERUBAHAN (%)'].str.replace(',', '.', regex=False)
    df['PERUBAHAN (%)'] = df['PERUBAHAN (%)'].replace('-', '0')
    df['PERUBAHAN (%)'] = pd.to_numeric(df['PERUBAHAN (%)'], errors='coerce').fillna(0) / 100

    df = df[df['SATUAN'] != '']
    return df

# Define a function to fetch data for a specific date and all specified regions.
def fetch_data_for_date(date):
    headers = ["Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN", "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    all_data = [] 
    tanggal = date.strftime("%Y-%m-%d")
    
    kabupaten_kota_list = {
        'bangkalankab': 'Kab. Bangkalan',
        'banyuwangikab': 'Kab. Banyuwangi',
        'blitarkab': 'Kab. Blitar',
        'bojonegorokab': 'Kab. Bojonegoro',
        'bondowosokab': 'Kab. Bondowoso',
        'gresikkab': 'Kab. Gresik',
        'jemberkab': 'Kab. Jember',
        'jombangkab': 'Kab. Jombang',
        'kedirikab': 'Kab. Kediri',
        'lamongankab': 'Kab. Lamongan',
        'lumajangkab': 'Kab. Lumajang',
        'madiunkab': 'Kab. Madiun',
        'magetankab': 'Kab. Magetan',
        'malangkab': 'Kab. Malang',
        'mojokertokab': 'Kab. Mojokerto',
        'nganjukkab': 'Kab. Nganjuk',
        'ngawikab': 'Kab. Ngawi',
        'pacitankab': 'Kab. Pacitan',
        'pamekasankab': 'Kab. Pamekasan',
        'pasuruankab': 'Kab. Pasuruan',
        'ponorogokab': 'Kab. Ponorogo',
        'probolinggokab': 'Kab. Probolinggo',
        'sampangkab': 'Kab. Sampang',
        'sidoarjokab': 'Kab. Sidoarjo',
        'situbondokab': 'Kab. Situbondo',
        'sumenepkab': 'Kab. Sumenep',
        'trenggalekkab': 'Kab. Trenggalek',
        'tubankab': 'Kab. Tuban',
        'tulungagungkab': 'Kab. Tulungagung',
        'batukota': 'Kota Batu',
        'blitarkota': 'Kota Blitar',
        'kedirikota': 'Kota Kediri',
        'madiunkota': 'Kota Madiun',
        'malangkota': 'Kota Malang',
        'mojokertokota': 'Kota Mojokerto',
        'pasuruankota': 'Kota Pasuruan',
        'probolinggokota': 'Kota Probolinggo',
        'surabayakota': 'Kota Surabaya'
    }

    endpoint_url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    
    for kabkota_code, kabkota_name in kabupaten_kota_list.items():
        payload = {
            'tanggal': tanggal,
            'kabkota': kabkota_code,
            'pasar': ''
        }
        data = fetch_table_data(endpoint_url, payload, tanggal, kabkota_name)
        if data:
            print(f" Multi Threading Harga Rata-Rata Kabupaten {kabkota_name} di Tingkat Konsumen Tanggal {tanggal}")
            df = pd.DataFrame(data, columns=headers)
            df = transform_data(df)
            
            # Filter data for "Daging Ayam Ras" and "Daging Ayam Kampung"
            df_filtered = df[df['NAMA BAHAN POKOK'].isin([" Daging Ayam Ras", " Daging Ayam Kampung"])]
            print(df_filtered.to_string(index=False))
            print()  
            all_data.append(df_filtered)  # Append only the filtered data
        else:
            print(f"Tidak ada data yang ditemukan untuk tanggal {tanggal} dan kabupaten/kota {kabkota_name}")
            print()
    
    return all_data

def main():
    start_date = datetime(2024, 5, 12)
    end_date = datetime(2024, 5, 12)
    headers = ["Kabupaten/Kota", "NAMA BAHAN POKOK", "HARGA KEMARIN", "HARGA SEKARANG"]
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

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Periksa kolom yang ada dalam final_df
        print("Kolom dalam final_df:", final_df.columns)

        # Buat kolom baru untuk "Ayam Ras" dan "Ayam Kampung"
        final_df['Ayam Ras'] = final_df.apply(lambda x: x['HARGA SEKARANG'] if x['NAMA BAHAN POKOK'] == " Daging Ayam Ras" else None, axis=1)
        final_df['Ayam Kampung'] = final_df.apply(lambda x: x['HARGA SEKARANG'] if x['NAMA BAHAN POKOK'] == " Daging Ayam Kampung" else None, axis=1)

        # Pivot tabel tanpa menggunakan kolom "NO"
        pivot_df = final_df.pivot_table(
            index=['Tanggal', 'Kabupaten/Kota'],
            columns='NAMA BAHAN POKOK',
            values='HARGA SEKARANG',
            aggfunc='first'
        ).reset_index()

        # Rename columns
        pivot_df.columns = ['Tanggal', 'Kabupaten/Kota', 'Ayam Kampung', 'Ayam Ras']

        # Fill NaNs with 0s
        pivot_df['Ayam Ras'].fillna(0, inplace=True)
        pivot_df['Ayam Kampung'].fillna(0, inplace=True)

        print("Gabungan Data untuk Semua Tanggal yang berisi Daging Ayam Ras dan Daging Ayam Kampung:")
        print(pivot_df)

        print("Current Working Directory:", os.getcwd())

        file_path = os.path.abspath('harga.parquet')
        print("Path ke file Parquet:", file_path)
        
        try:
            gc = pygsheets.authorize(service_account_file='magang-423902-f0e642da9b61.json')
            gs = gc.open('looker latihan')
            worksheet = gs.worksheet('index', 0)
            worksheet.clear('A1')
            worksheet.set_dataframe(pivot_df, (1, 1), extend=True, copy_index=False)
            output_file = 'bangkalan_looker.parquet'
            table = pa.Table.from_pandas(pivot_df)
            pq.write_table(table, output_file)
        except Exception as e:
            print(f"Error reading or processing the Parquet file: {e}")
    else:
        print("No data was collected for any date. Exiting.")

if __name__ == "__main__":
    main()
