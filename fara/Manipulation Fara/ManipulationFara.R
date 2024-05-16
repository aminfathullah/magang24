library(dplyr)
setwd('C:/Users/ASUS/magang24/')

data1 <- arrow::open_dataset('fara/Hasil Parquet Multithreading Fara/')

data_harga <- data1 %>%
  filter(SATUAN != '') %>%
  collect()

data_harga <- data_harga %>%
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
  select(No, ID_KABKOT, everything())%>% 
  write_parquet("harga.parquet")

View(data_harga)


