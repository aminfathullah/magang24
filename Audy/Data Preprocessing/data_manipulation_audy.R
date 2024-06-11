library(dplyr)
library(tidyverse)
library(arrow)

# Inisialisasi dataset ke dalam variabel 
data_harga <- arrow::open_dataset('D:\\Coding\\Python\\magang24\\audy\\crawling\\Output_Crawling_Harga\\')

# Kumpulkan data ke dalam memori R
data_harga_collected <- data_harga %>% 
  collect()

# Data cleaning dan transformasi
data_harga_cleaned <- data_harga_collected %>%
  filter(SATUAN != '') %>%
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', 0, `HARGA KEMARIN`)) %>%
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  mutate(`HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE)) %>%
  mutate(`HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == '-', 0, `HARGA SEKARANG`)) %>%
  mutate(`HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  mutate(`PERUBAHAN (Rp)` = gsub('.', '', `PERUBAHAN (Rp)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (Rp)` = ifelse(`PERUBAHAN (Rp)` == '-', 0, `PERUBAHAN (Rp)`)) %>%
  mutate(`PERUBAHAN (Rp)` = as.integer(`PERUBAHAN (Rp)`)) %>%
  mutate(`PERUBAHAN (%)` = gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = gsub(',', '.', `PERUBAHAN (%)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', 0, `PERUBAHAN (%)`)) %>%
  mutate(`PERUBAHAN (%)` = as.double(`PERUBAHAN (%)`)) %>%
  mutate(`PERUBAHAN (%)` = `PERUBAHAN (%)` / 100) %>%
  mutate(Tanggal = as.Date(Tanggal)) %>%
  mutate(normalize_harga_kemarin = (`HARGA KEMARIN` - min(`HARGA KEMARIN`)) / (max(`HARGA KEMARIN`) - min(`HARGA KEMARIN`))) %>%
  mutate(id_kab = case_when(
    `Kabupaten/Kota` == "Kab Lumajang" ~ "3508",
    `Kabupaten/Kota` == "Kab Madiun" ~ "3519",
    `Kabupaten/Kota` == "Kab Magetan" ~ "3520",
    `Kabupaten/Kota` == "Kab Mojokerto" ~ "3516",
    `Kabupaten/Kota` == "Kab Malang" ~ "3507",
    `Kabupaten/Kota` == "Kab Nganjuk" ~ "3518",
    `Kabupaten/Kota` == "Kab Pacitan" ~ "3501",
    `Kabupaten/Kota` == "Kab Pamekasan" ~ "3528",
    TRUE ~ "-"
  ))

# Menyimpan data yang telah dibersihkan dan ditransformasi ke dalam file Parquet
write_parquet(data_harga_cleaned, 'D:\\Coding\\Python\\magang24\\audy\\crawling\\Output_Crawling_Harga\\datafinal.parquet')

write_csv(data_harga_cleaned, 'D:\\Coding\\Python\\magang24\\audy\\crawling\\Output_Crawling_Harga\\datafinal.csv')
