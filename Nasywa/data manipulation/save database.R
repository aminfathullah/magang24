library(dplyr)
library(arrow)
library(odbc)
library(DBI)

con <- DBI::dbConnect(odbc::odbc(),
                      Driver = "SQL Server",
                      Server = "localhost\\SQLEXPRESS",
                      Database = "DB_HARGA_PASAR",
                      Trusted_connection = "True")
dbListTables(con)

data_harga <- arrow::open_dataset('C:/Users/User/magang24/Nasywa/crawling harga update/')

kode_wilayah <- data_harga %>%
  filter(SATUAN != '') %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE)) %>%
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', NA, `HARGA KEMARIN`)) %>%
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  
  filter(SATUAN != '') %>%
  mutate(`HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE)) %>%
  mutate(`HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == '-', NA,`HARGA SEKARANG`)) %>%
  mutate(`HARGA SEKARANG` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE)) %>%
  mutate(`NAMA BAHAN POKOK` = as.character(`NAMA BAHAN POKOK`)) %>%
  collect() %>%
  
  mutate(`PERUBAHAN (%)`= gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)) %>%
  mutate(`PERUBAHAN (Rp)`= as.integer(`PERUBAHAN (Rp)`)) %>%
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
  mutate(NO = row_number()) %>%
  
  select(NO, ID_WILAYAH, everything())
dbWriteTable(con, "kode_wilayah", kode_wilayah, overwrite = TRUE)
arrow::write_parquet(kode_wilayah, "dataharga.parquet")
View(kode_wilayah)
