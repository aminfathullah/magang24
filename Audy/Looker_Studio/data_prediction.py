import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

# Contoh data harga beras premium (disederhanakan untuk contoh)
data = {
    "Tanggal": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
    "HARGA SEKARANG": [13000, 13100, 12900, 13200, 13400]
}

# Membuat DataFrame
df = pd.DataFrame(data)
df['Tanggal'] = pd.to_datetime(df['Tanggal'])
df.set_index('Tanggal', inplace=True)

# Mengatur frekuensi secara eksplisit
df = df.asfreq('D')

# Membuat dan melatih model ARIMA
model = ARIMA(df['HARGA SEKARANG'], order=(1, 1, 1))
model_fit = model.fit()

# Prediksi 5 hari ke depan
forecast = model_fit.get_forecast(steps=5)
forecast_values = forecast.predicted_mean
conf_int = forecast.conf_int()

# Membuat DataFrame untuk hasil prediksi
forecast_dates = pd.date_range(start=df.index[-1] + pd.Timedelta(days=1), periods=5)
forecast_df = pd.DataFrame(forecast_values, index=forecast_dates, columns=['Forecast'])

# Plotting hasil prediksi
plt.figure(figsize=(10, 6))
plt.plot(df, label='Harga Sekarang')
plt.plot(forecast_df, label='Prediksi', linestyle='--')
plt.fill_between(forecast_df.index, conf_int.iloc[:, 0], conf_int.iloc[:, 1], color='pink', alpha=0.3)
plt.title('Prediksi Harga Beras Premium di Kabupaten Malang')
plt.xlabel('Tanggal')
plt.ylabel('Harga (Rp)')
plt.legend()
plt.grid(True)
plt.show()
