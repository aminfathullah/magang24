library(dplyr)
library(arrow)
library(tidyr)
library(forecast)
library(partykit)
library(ggplot2)



data_harga <- arrow::open_dataset('C:/Users/User/magang24/Nasywa/crawling harga update')

kode_wilayah <- data_harga %>%
  filter(SATUAN != '') %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', NA, `HARGA KEMARIN`)) %>%
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  
  mutate(`HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE)) %>%
  mutate(`HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == '-', NA, `HARGA SEKARANG`)) %>%
  mutate(`HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE)) %>%
  mutate(`NAMA BAHAN POKOK` = as.character(`NAMA BAHAN POKOK`)) %>%
  
  mutate(`PERUBAHAN (%)` = gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = gsub(',', '.', `PERUBAHAN (%)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', NA, `PERUBAHAN (%)`)) %>%
  mutate(`PERUBAHAN (%)` = as.double(`PERUBAHAN (%)`)) %>%
  mutate(`PERUBAHAN (%)` = `PERUBAHAN (%)` / 100) %>%
  
  mutate(`PERUBAHAN (Rp)` = gsub('.', '', `PERUBAHAN (Rp)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (Rp)` = ifelse(`PERUBAHAN (Rp)` == '-', NA, `PERUBAHAN (Rp)`)) %>%
  mutate(`PERUBAHAN (Rp)` = as.integer(`PERUBAHAN (Rp)`)) %>%
  
  mutate(`Tanggal` = as.Date(Tanggal, format = "%Y-%m-%d")) %>%
  collect() %>%  
  
  mutate(ID_WILAYAH = case_when(
    `Kabupaten/Kota` == "Pasuruan" ~ 3514,
    `Kabupaten/Kota` == "Ponorogo" ~ 3502,
    `Kabupaten/Kota` == "Probolinggo" ~ 3513,
    `Kabupaten/Kota` == "Sampang" ~ 3527,
    `Kabupaten/Kota` == "Sidoarjo" ~ 3515,
    `Kabupaten/Kota` == "Situbondo" ~ 3512,
    `Kabupaten/Kota` == "Sumenep" ~ 3529,
    `Kabupaten/Kota` == "Trenggalek" ~ 3503,
    `Kabupaten/Kota` == "Tuban" ~ 3523,
    `Kabupaten/Kota` == "Tulungagung" ~ 3504,
    TRUE ~ NA_integer_
  )) %>%
  mutate(ID_WILAYAH = as.integer(ID_WILAYAH)) %>%
  mutate(NO = row_number()) %>%
  select(NO, ID_WILAYAH, everything()) %>%
  mutate_all(~replace_na(., 0)) %>%
  filter((`NAMA BAHAN POKOK` == " Beras Premium" | `NAMA BAHAN POKOK` == " Beras Medium") & 
           (`Kabupaten/Kota` == "Ponorogo" | `Kabupaten/Kota` == "Trenggalek")) %>%
  collect()

kode_wilayah_premium <- kode_wilayah %>%
  filter(`NAMA BAHAN POKOK` == " Beras Premium") %>%
  group_by(Tanggal, `NAMA BAHAN POKOK`) %>%
  summarize(`HARGA KEMARIN` = mean(`HARGA KEMARIN`, na.rm = TRUE),
            `HARGA SEKARANG` = mean(`HARGA SEKARANG`, na.rm = TRUE),
            `PERUBAHAN (Rp)` = mean(`PERUBAHAN (Rp)`, na.rm = TRUE),
            `PERUBAHAN (%)` = mean(`PERUBAHAN (%)`, na.rm = TRUE))

kode_wilayah_medium <- kode_wilayah %>%
  filter(`NAMA BAHAN POKOK` == " Beras Medium") %>%
  group_by(Tanggal, `NAMA BAHAN POKOK`) %>%
  summarize(`HARGA KEMARIN` = mean(`HARGA KEMARIN`, na.rm = TRUE),
            `HARGA SEKARANG` = mean(`HARGA SEKARANG`, na.rm = TRUE),
            `PERUBAHAN (Rp)` = mean(`PERUBAHAN (Rp)`, na.rm = TRUE),
            `PERUBAHAN (%)` = mean(`PERUBAHAN (%)`, na.rm = TRUE))

kode_wilayah_aggregated <- bind_rows(kode_wilayah_premium, kode_wilayah_medium)

write_parquet(kode_wilayah_aggregated, 'C:/Users/User/magang24/Nasywa/databeras.parquet')

# uji t-test antara HARGA KEMARIN dan HARGA SEKARANG
t_test_result <- t.test(kode_wilayah_aggregated$`HARGA KEMARIN`, kode_wilayah_aggregated$`HARGA SEKARANG`)
print("Hasil uji t-test antara HARGA KEMARIN dan HARGA SEKARANG:")
print(t_test_result)

# koefisien korelasi antara HARGA KEMARIN dan HARGA SEKARANG
correlation_result <- cor(kode_wilayah_aggregated$`HARGA KEMARIN`, kode_wilayah_aggregated$`HARGA SEKARANG`)
print("Koefisien korelasi antara HARGA KEMARIN dan HARGA SEKARANG:")
print(correlation_result)

# Model ARIMA untuk HARGA KEMARIN
ari <- arima(kode_wilayah_aggregated$`HARGA KEMARIN`)
print("Model ARIMA untuk HARGA KEMARIN:")
print(ari)

# Model ARIMA untuk harga sekarang beras premium
ari_premium <- auto.arima(kode_wilayah_aggregated$`HARGA SEKARANG_PREMIUM`)
print("Model ARIMA untuk harga sekarang beras premium:")
print(ari_premium)

# Model ARIMA untuk harga sekarang beras medium
ari_medium <- auto.arima(kode_wilayah_aggregated$`HARGA SEKARANG_MEDIUM`)
print("Model ARIMA untuk harga sekarang beras medium:")
print(ari_medium)

# Forecasting HARGA KEMARIN untuk 10 periode ke depan
forecast_result <- predict(ari, n.ahead = 10)
print("Forecasting HARGA KEMARIN untuk 10 periode ke depan:")
print(forecast_result)

# Split data untuk training dan testing
set.seed(123)  # Menetapkan seed untuk hasil yang dapat direplikasi
training_indices <- sample(1:nrow(kode_wilayah_aggregated), size = 0.8 * nrow(kode_wilayah_aggregated))
training <- kode_wilayah_aggregated[training_indices, ]
testing <- kode_wilayah_aggregated[-training_indices, ]
print("Data Training:")
print(training)
print("Data Testing:")
print(testing)

# Regresi linear
regresi1 <- lm(`HARGA SEKARANG` ~ `HARGA KEMARIN` + `PERUBAHAN (Rp)` + `PERUBAHAN (%)`, data = training)
summary(regresi1)

# Prediksi menggunakan model regresi linear
prediksi_regresi <- predict(regresi1, testing)
print("Hasil prediksi menggunakan model regresi linear:")
print(prediksi_regresi)


dtree <- ctree(`HARGA SEKARANG` ~ `HARGA KEMARIN` + `PERUBAHAN (Rp)` + `PERUBAHAN (%)`, data = kode_wilayah_aggregated)
plot(dtree)
print(dtree)


View(kode_wilayah)
