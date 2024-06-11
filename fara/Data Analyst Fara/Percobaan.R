# Load libraries
library(dplyr)
library(arrow)
library(rpart)  # library for decision tree modeling
library(partykit)  # library for visualization of decision tree
library(forecast)
library(rpart.plot)


# Set working directory
setwd('C:/Users/ASUS/magang24/')

# Load and preprocess the data
data1 <- arrow::open_dataset('fara/Hasil Parquet Multithreading Fara/')

data_harga <- data1 %>%
  filter(SATUAN != '') %>%
  collect() %>%
  mutate(
    `NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`),
    `HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE),
    `HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == "-", 0, `HARGA KEMARIN`),
    `HARGA KEMARIN` = ifelse(is.na(`HARGA KEMARIN`), 0, `HARGA KEMARIN`),
    `HARGA KEMARIN` = as.integer(`HARGA KEMARIN`),
    `HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE),
    `HARGA SEKARANG` = ifelse(is.na(`HARGA SEKARANG`), 0, `HARGA SEKARANG`),
    `HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == "-", 0, `HARGA SEKARANG`),
    `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`),
    `PERUBAHAN (Rp)` = ifelse(is.na(`PERUBAHAN (Rp)`), 0, `PERUBAHAN (Rp)`),
    `PERUBAHAN (Rp)` = ifelse(`PERUBAHAN (Rp)` == "-", 0, `PERUBAHAN (Rp)`),
    `PERUBAHAN (Rp)` = gsub('.', '', `PERUBAHAN (Rp)`, fixed = TRUE),
    `PERUBAHAN (Rp)` = as.integer(`PERUBAHAN (Rp)`),
    `PERUBAHAN (%)` = gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE),
    `PERUBAHAN (%)` = gsub(',', '.', `PERUBAHAN (%)`, fixed = TRUE),
    `PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', NA, `PERUBAHAN (%)`),
    `PERUBAHAN (%)` = as.double(`PERUBAHAN (%)`),
    `PERUBAHAN (%)` = ifelse(is.na(`PERUBAHAN (%)`), 0, `PERUBAHAN (%)`),
    `PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == "-", 0, `PERUBAHAN (%)`),
    `PERUBAHAN (%)` = `PERUBAHAN (%)` / 100
  ) %>%
  mutate(
    ID_KABKOT = case_when(
      `Kabupaten/Kota` == "Bangkalan" ~ "3526",
      `Kabupaten/Kota` == "Banyuwangi" ~ "3510",
      `Kabupaten/Kota` == "Blitar" ~ "3505",
      `Kabupaten/Kota` == "Bojonegoro" ~ "3522",
      `Kabupaten/Kota` == "Bondowoso" ~ "3511",
      `Kabupaten/Kota` == "Gresik" ~ "3525",
      `Kabupaten/Kota` == "Jember" ~ "3509",
      `Kabupaten/Kota` == "Jombang" ~ "3517",
      `Kabupaten/Kota` == "Kediri" ~ "3506",
      `Kabupaten/Kota` == "Lamongan" ~ "3524",
      TRUE ~ "-"
    )
  ) %>%
  mutate(Tanggal = as.Date(Tanggal)) %>%
  select(-NO) %>%
  mutate(No = row_number()) %>%
  select(No, ID_KABKOT, everything())

View(data_harga)

# Mengisi NA dengan 0
data_harga <- data_harga %>%
  mutate_all(~replace_na(., 0))

# Mengelompokkan data berdasarkan 'NAMA BAHAN POKOK' dan mengisi nilai NA dengan rata-rata
data_harga <- data_harga %>%
  group_by("NAMA BAHAN POKOK") %>%
  mutate("HARGA SEKARANG" = ifelse(is.na("HARGA SEKARANG") | "HARGA SEKARANG" == 0, mean("HARGA SEKARANG", na.rm = TRUE), "HARGA SEKARANG")) %>%
  ungroup()

# Memilih komoditas tertentu
y_commodity <- " Beras Premium"
data_harga <- data_harga %>%
  filter("NAMA BAHAN POKOK" == y_commodity)

# Membuat model ARIMA
ari <- auto.arima(data_harga$"HARGA SEKARANG")
forecast_result <- forecast(ari, h = 10)
print("Forecasting HARGA SEKARANG untuk 10 periode ke depan:")
print(forecast_result)
print(as.data.frame(forecast_result))  

# Membuat model regresi linear
set.seed(123) 
training <- data_harga[1:120, ]
testing <- data_harga[121:140, ]
print(testing)

regresi1 <- lm("HARGA SEKARANG" ~ "PERUBAHAN (Rp)" + "PERUBAHAN (%)", data = training)
summary(regresi1)

prediksi_regresi <- predict(regresi1, testing)
print("Hasil prediksi menggunakan model regresi linear:")
print(prediksi_regresi)

# Mengisi nilai kosong atau 0 pada 'HARGA SEKARANG' dengan hasil prediksi
data_harga <- data_harga%>%
  mutate("HARGA SEKARANG" = ifelse("HARGA SEKARANG" == 0 | is.na("HARGA SEKARANG"), prediksi_regresi, "HARGA SEKARANG"))

# Membuat dan memplot decision tree
dtree <- ctree("HARGA SEKARANG" ~ "PERUBAHAN (Rp)" + "PERUBAHAN (%)", data = data_harga)
plot(dtree)
print(dtree)