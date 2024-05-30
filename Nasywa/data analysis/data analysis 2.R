library(dplyr)
library(arrow)
library(tidyr)
library(forecast)
library(partykit)
library(ggplot2)

data_harga <- arrow::open_dataset('C:/Users/User/magang24/Nasywa/crawling harga update/')

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
  
  mutate(Tanggal = as.Date(Tanggal, format = "%Y-%m-%d")) %>%
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
  collect(kode_wilayah)

View(kode_wilayah)

kode_wilayah <- kode_wilayah %>%
  group_by(`NAMA BAHAN POKOK`) %>%
  mutate(`HARGA SEKARANG` = ifelse(is.na(`HARGA SEKARANG`), mean(`HARGA SEKARANG`, na.rm = TRUE), `HARGA SEKARANG`)) %>%
  ungroup()

y_commodity <- " Beras Premium"

kode_wilayah <- kode_wilayah %>%
  filter(`NAMA BAHAN POKOK` == y_commodity)

ari <- auto.arima(kode_wilayah$`HARGA SEKARANG`)
forecast_result <- forecast(ari, h = 10)
print("Forecasting HARGA SEKARANG untuk 10 periode ke depan:")
print(forecast_result)
print(as.data.frame(forecast_result))

set.seed(123) 
training <- kode_wilayah[1:120, ]
testing <- kode_wilayah[121:140, ]

regresi1 <- lm(`HARGA SEKARANG` ~ `PERUBAHAN (Rp)` + `PERUBAHAN (%)`, data = training)
summary(regresi1)

prediksi_regresi <- predict(regresi1, testing)
print("Hasil prediksi menggunakan model regresi linear:")
print(prediksi_regresi)

dtree <- ctree(`HARGA SEKARANG` ~ `PERUBAHAN (Rp)` + `PERUBAHAN (%)`, data = kode_wilayah)
plot(dtree)
print(dtree)
