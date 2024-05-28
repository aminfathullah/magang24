library(arrow)
library(dplyr)
library(partykit)

# Set working directory
setwd('C:/Users/ASUS/magang24/')

# Open dataset
data1 <- arrow::open_dataset('fara/Hasil Parquet Multithreading Fara/')

# Data preprocessing
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

# Menambahkan kolom kategori perubahan berdasarkan perubahan harga
data_harga <- data_harga %>%
  mutate(
    Kategori_Perubahan = case_when(
      (`HARGA SEKARANG` - `HARGA KEMARIN`) == 0 ~ "Tidak Ada Perubahan",
      (`HARGA SEKARANG` - `HARGA KEMARIN`) < 0 ~  "Perubahan Berkurang",
      (`HARGA SEKARANG` - `HARGA KEMARIN`) > 0 ~  "Perubahan Bertambah"
    )
  ) %>%
  mutate(Kategori_Perubahan = factor(Kategori_Perubahan))

# Filter data
data_filtered <- data_harga %>%
  filter(Tanggal >= as.Date("2023-12-01") & Tanggal <= as.Date("2023-12-30") & `NAMA BAHAN POKOK` == " Minyak Goreng MINYAKITA")

View(data_filtered)
collect(data_filtered)

# Uji-t
t <- t.test(data_filtered$`PERUBAHAN (Rp)`)
print(t)

# Korelasi
c <- cor(data_filtered$`PERUBAHAN (Rp)`, data_filtered$`HARGA SEKARANG`)
print(c)

# ARIMA
ari <- arima(data_filtered$`PERUBAHAN (Rp)`)
prediksi_ari <- predict(ari, n.ahead = 10)
print(prediksi_ari)

# Machine Learning
training <- data_filtered[1:130,]
testing <- data_filtered[131:150,]

# Regresi linier
regresi1 <- lm(`HARGA SEKARANG` ~ `HARGA KEMARIN` + `PERUBAHAN (Rp)`, data = data_filtered)
summary_regresi <- summary(regresi1)
print(summary_regresi)

# Prediksi menggunakan model regresi
prediksi_regresi <- predict(regresi1, testing)
print(prediksi_regresi)


# Decision tree hanya untuk PERUBAHAN (Rp)
dtree <- ctree(`PERUBAHAN (Rp)` ~ Kategori_Perubahan, data = data_filtered)
plot(dtree)
print(dtree)

