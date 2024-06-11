import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
file_path = 'D:\\Coding\\Python\\magang24\\audy\\crawling\\Output_Crawling_Harga\\datafinal.parquet'
data = pd.read_parquet(file_path)

# Convert 'Tanggal' column to datetime format
data['Tanggal'] = pd.to_datetime(data['Tanggal'], errors='coerce')

# Filter out rows with invalid dates
data = data.dropna(subset=['Tanggal'])

# Plotting trends of 'HARGA SEKARANG' over time for a specific commodity
def plot_price_trend(commodity):
    df = data[data['NAMA BAHAN POKOK'] == commodity]
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='Tanggal', y='HARGA SEKARANG', hue='Kabupaten/Kota', marker='o')
    plt.title(f'Trend Harga {commodity} dari Waktu ke Waktu')
    plt.xlabel('Tanggal')
    plt.ylabel('Harga Sekarang')
    plt.legend(loc='upper right', bbox_to_anchor=(1.25, 1))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Example: Plotting price trend for 'Beras Medium'
plot_price_trend('Beras Medium')

# Plotting histogram of price changes
plt.figure(figsize=(10, 6))
sns.histplot(data['PERUBAHAN (Rp)'], bins=50, kde=True)
plt.title('Distribusi Perubahan Harga (Rp)')
plt.xlabel('Perubahan Harga (Rp)')
plt.ylabel('Frekuensi')
plt.tight_layout()
plt.show()

# Aggregating and plotting mean price change by region
region_price_change = data.groupby('Kabupaten/Kota')['PERUBAHAN (%)'].mean().reset_index()

plt.figure(figsize=(14, 8))
sns.barplot(data=region_price_change, x='Kabupaten/Kota', y='PERUBAHAN (%)')
plt.title('Rata-rata Perubahan Harga (%) Berdasarkan Wilayah')
plt.xlabel('Kabupaten/Kota')
plt.ylabel('Perubahan Harga (%)')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()
