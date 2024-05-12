library(dplyr)
# install.packages('dplyr')

data1 <- arrow::open_dataset('../magang24/fara/Hasil Parquet Multithreading Fara/')

processed_data <- data1 %>%
  filter(SATUAN != '') %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', NA, `HARGA KEMARIN`)) %>%
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  mutate(PERUBAHAN_RP = ifelse(`PERUBAHAN (Rp)` == '-', NA, `PERUBAHAN (Rp)`), 
         `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  collect() %>%
  select(-`PERUBAHAN (Rp)`, -NO) %>%
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE))

processed_data <- processed_data %>%
  mutate(`PERUBAHAN (%)` = gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE))

processed_data <- processed_data %>%
  mutate(ID_KABKOT = case_when(
    `Kabupaten/Kota` == "Bangkalan" ~ 3526,
    `Kabupaten/Kota` == "Banyuwangi" ~ 3510,
    `Kabupaten/Kota` == "Blitar" ~ 3505,
    `Kabupaten/Kota` == "Bojonegoro" ~ 3522,
    `Kabupaten/Kota` == "Bondowoso" ~ 3511,
    `Kabupaten/Kota` == "Gresik" ~ 3525,
    `Kabupaten/Kota` == "Jember" ~ 3509,
    `Kabupaten/Kota` == "Jombang" ~ 3517,
    `Kabupaten/Kota` == "Kediri" ~ 3506,
    `Kabupaten/Kota` == "Lamongan" ~ 3524,
    TRUE ~ NA_integer_
  ))

processed_data <- processed_data %>%
  mutate(PERUBAHAN_RP = gsub('.', '', PERUBAHAN_RP, fixed = TRUE))

processed_data <- processed_data %>%
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', NA, `PERUBAHAN (%)`))

processed_data <- processed_data %>%
  select(ID_KABKOT, `Kabupaten/Kota`, Tanggal, `NAMA BAHAN POKOK`, SATUAN, `HARGA KEMARIN`, `HARGA SEKARANG`, PERUBAHAN_RP, everything())

processed_data <- processed_data %>%
  rename(`PERUBAHAN (Rp)` = PERUBAHAN_RP)

head(processed_data)
