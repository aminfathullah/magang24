import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Memuat data dari file CSV
file_path = 'D:\Coding\Python\magang24\Audy\Crawling\Output_Crawling_Harga\datafinal.csv'
data = pd.read_csv(file_path)

# Menampilkan beberapa baris pertama data
print(data.head())

# Melakukan deskripsi statistik dasar
print(data.describe())

# Mengatur gaya seaborn
sns.set(style="whitegrid")

# Visualisasi distribusi data
plt.figure(figsize=(12, 6))
sns.histplot(data, kde=True)
plt.title('Distribusi Data')
plt.xlabel('Nilai')
plt.ylabel('Frekuensi')
plt.show()
