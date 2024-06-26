import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import pygsheets
from prophet import Prophet

try:
    import plotly.graph_objs as go
    import plotly.offline as py
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Importing plotly failed. Interactive plots will not work.")

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


def fetch_table_data(url, payload, date):
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return []

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
                data.append([no, date, kabupaten_kota_list[payload['kabkota']], nama,
                            satuan, harga_kemarin, harga_sekarang, perubahan_rp, perubahan_persen])
        return data
    else:
        print(f"Tabel tidak ditemukan untuk {date}")
        return []


def fetch_data_for_date(date, kabkota):
    headers = ["NO", "Tanggal", "Kabupaten/Kota", "NAMA BAHAN POKOK", "SATUAN",
               "HARGA KEMARIN", "HARGA SEKARANG", "PERUBAHAN (Rp)", "PERUBAHAN (%)"]
    tanggal = date.strftime("%Y-%m-%d")
    payload = {
        'tanggal': tanggal,
        'kabkota': kabkota,
        'pasar': ''
    }

    endpoint_url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    data = fetch_table_data(endpoint_url, payload, tanggal)
    if data:
        df = pd.DataFrame(data, columns=headers)
        return df
    else:
        print(
            f"Tidak ada data yang ditemukan untuk tanggal {tanggal} dan kabupaten/kota {kabkota}")
        return pd.DataFrame(columns=headers)


def preprocess_data(df):
    filtered_df = df[df['NAMA BAHAN POKOK'].isin(
        ['- Cabe Merah Keriting', '- Cabe Merah Besar', '- Cabe Rawit Merah'])]
    filtered_df = filtered_df[filtered_df['SATUAN'] != '']
    filtered_df['NAMA BAHAN POKOK'] = filtered_df['NAMA BAHAN POKOK'].str.replace(
        '-', '', regex=False)

    filtered_df['HARGA KEMARIN'] = filtered_df['HARGA KEMARIN'].str.replace(
        '.', '', regex=False).replace('-', '0').astype(int)
    filtered_df['HARGA SEKARANG'] = filtered_df['HARGA SEKARANG'].str.replace(
        '.', '', regex=False).replace('-', '0').astype(int)
    filtered_df['PERUBAHAN (Rp)'] = filtered_df['PERUBAHAN (Rp)'].str.replace(
        '.', '', regex=False).replace('-', '0').astype(int)
    filtered_df['PERUBAHAN (%)'] = filtered_df['PERUBAHAN (%)'].str.replace(
        '%', '', regex=False).str.replace(',', '.', regex=False).replace('-', '0').astype(float) / 100
    filtered_df['Tanggal'] = pd.to_datetime(filtered_df['Tanggal'])

    harga_kemarin_min = filtered_df['HARGA KEMARIN'].min()
    harga_kemarin_max = filtered_df['HARGA KEMARIN'].max()
    filtered_df['normalize_harga_kemarin'] = (
        filtered_df['HARGA KEMARIN'] - harga_kemarin_min) / (harga_kemarin_max - harga_kemarin_min)

    id_kab_dict = {v: k for k, v in kabupaten_kota_list.items()}
    filtered_df['id_kab'] = filtered_df['Kabupaten/Kota'].map(
        id_kab_dict).fillna('-')

    return filtered_df


def add_forecast(df):
    forecast_df = pd.DataFrame()

    for name, group in df.groupby(['Kabupaten/Kota', 'NAMA BAHAN POKOK']):
        group = group[['Tanggal', 'HARGA SEKARANG']].rename(
            columns={'Tanggal': 'ds', 'HARGA SEKARANG': 'y'})
        model = Prophet()
        model.fit(group)
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)

        forecast = forecast[['ds', 'yhat']].rename(
            columns={'ds': 'Tanggal', 'yhat': 'Forecast'})
        forecast['Kabupaten/Kota'] = name[0]
        forecast['NAMA BAHAN POKOK'] = name[1]

        forecast_df = pd.concat([forecast_df, forecast], ignore_index=True)

    forecast_df['Tanggal'] = pd.to_datetime(forecast_df['Tanggal'])
    return pd.merge(df, forecast_df, on=['Tanggal', 'Kabupaten/Kota', 'NAMA BAHAN POKOK'], how='outer')


def upload_to_google_sheets(df, sheet_name, worksheet_index):
    try:
        gc = pygsheets.authorize(
            service_account_file="D:\Coding\magang-423902-f0e642da9b61.json")
        gs = gc.open(sheet_name)
        worksheet = gs.worksheet('index', worksheet_index)
        worksheet.clear('A1')

        numeric_cols = ['HARGA KEMARIN', 'HARGA SEKARANG',
                        'PERUBAHAN (Rp)', 'PERUBAHAN (%)', 'normalize_harga_kemarin']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
        df = df.applymap(lambda x: str(
            x) if isinstance(x, (int, float)) else x)
        worksheet.set_dataframe(df, 'A1', extend=True, copy_index=True)
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")


def main():
    start_date = datetime(2023, 4, 24)
    end_date = datetime.now()
    all_data = []

    date_range = [start_date + timedelta(days=i)
                  for i in range((end_date - start_date).days + 1)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_date_kabkota = {
            executor.submit(fetch_data_for_date, date, kabkota): (date, kabkota)
            for date in date_range for kabkota in kabupaten_kota_list.keys()
        }
        for future in concurrent.futures.as_completed(future_to_date_kabkota):
            date, kabkota = future_to_date_kabkota[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data.append(df)
            except Exception as exc:
                print(
                    f"Exception occurred while fetching data for date {date} and kabupaten/kota {kabkota}: {exc}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = preprocess_data(final_df)
        final_df = add_forecast(final_df)
        upload_to_google_sheets(final_df, 'loker-coba', 0)
        print("Gabungan Data untuk Semua Tanggal dan Kabupaten/Kota dengan Prediksi:")
        print(final_df)
    else:
        print("Tidak ada data yang dapat diproses.")


if __name__ == "__main__":
    main()
