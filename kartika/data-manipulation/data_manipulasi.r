library('dplyr')
library('arrow')
library('odbc')
library('DBI')

con <- DBI::dbConnect(odbc::odbc(),
                      Driver = "SQL Server",
                      Server = "localhost\\SQLEXPRESS",
                      Database = "data_kota",
                      Trusted_connection = "True")
dbListTables(con)
data_kota <- arrow::open_dataset('../magang24/fara/Hasil Parquet Semua Kota/')


data_manipulasi <- data_kota %>%
  filter(SATUAN != '') %>%
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = TRUE),
         `HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-', NA, `HARGA KEMARIN`),
         `HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  
  filter(SATUAN != '') %>%
  mutate(`HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE),
         `HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == '-', NA, `HARGA SEKARANG`),
         `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  collect() %>%
  
  mutate(`NAMA BAHAN POKOK` = gsub('-', '', `NAMA BAHAN POKOK`, fixed = TRUE),
         `NAMA BAHAN POKOK` = as.character(`NAMA BAHAN POKOK`)) %>%
  collect() %>%
  
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-', NA, gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)),
         `PERUBAHAN (Rp)` = as.integer(`PERUBAHAN (Rp)`)) %>%
  collect() %>%
  
  mutate(ID_WILAYAH = case_when(
    `Kabupaten/Kota` == "Kota Kediri" ~ 3571,
    `Kabupaten/Kota` == "Kota Blitar" ~ 3572,
    `Kabupaten/Kota` == "Kota Malang" ~ 3573,
    `Kabupaten/Kota` == "Kota Probolinggo" ~ 3574,
    `Kabupaten/Kota` == "Kota Pasuruan" ~ 3575,
    `Kabupaten/Kota` == "Kota Mojokerto" ~ 3576,
    `Kabupaten/Kota` == "Kota Madiun" ~ 3577,
    `Kabupaten/Kota` == "Kota Surabaya" ~ 3578,
    `Kabupaten/Kota` == "Kota Batu" ~ 3579,
    TRUE ~ NA_integer_
  )) %>%
  mutate(NO = row_number()) %>%
  
  select(NO, ID_WILAYAH, everything())
  
dbWriteTable(con, "data_manipulasi", data_manipulasi, overwrite = TRUE)
arrow::write_parquet(data_manipulasi, "data_manipulasi.parquet")
View(data_manipulasi)
