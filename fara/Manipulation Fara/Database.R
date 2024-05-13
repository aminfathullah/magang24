library(dplyr)

setwd('C:/Users/ASUS/magang24/')


library(odbc)
library(DBI)

con <- DBI::dbConnect(odbc::odbc(),
                      Driver = "SQL Server",
                      Server = "localhost\\SQLEXPRESS",
                      Database = "harga_fara",
                      Trusted_connection = "True")
dbListTables(con)

data1 <- arrow::open_dataset('fara/Hasil Parquet Multithreading Fara/')


data_harga <- data1 %>%
  filter(SATUAN != '') %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', NA, `HARGA KEMARIN`)) %>%
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  mutate(PERUBAHAN_RP = ifelse(`PERUBAHAN (Rp)` == '-', NA, `PERUBAHAN (Rp)`), 
         `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  collect() %>%
  select(-`PERUBAHAN (Rp)`, -NO) %>%
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)) %>%
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
  )) %>%
  mutate(PERUBAHAN_RP = gsub('.', '', PERUBAHAN_RP, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', NA, `PERUBAHAN (%)`)) %>%
  select(ID_KABKOT, `Kabupaten/Kota`, Tanggal, `NAMA BAHAN POKOK`, SATUAN, `HARGA KEMARIN`, `HARGA SEKARANG`, PERUBAHAN_RP, everything()) %>%
  rename(`PERUBAHAN (Rp)` = PERUBAHAN_RP) %>%
  mutate(NO = row_number()) %>%
  select(NO, everything()) %>% 
  write_parquet("harga.parquet")
  dbWriteTable(con, "data_harga", data_harga, overwrite = TRUE)
  arrow::write_parquet(data_harga, "harga.parquet")

View(data_harga)