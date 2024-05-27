library('dplyr')
library('arrow')
library('odbc')
library('DBI')
library(ggplot2)
library(scales)

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
         `HARGA KEMARIN` = ifelse(`HARGA KEMARIN` == '-' | is.na(`HARGA KEMARIN`), 0, `HARGA KEMARIN`),
         `HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  collect() %>%
  
  mutate(`HARGA SEKARANG` = gsub('.', '', `HARGA SEKARANG`, fixed = TRUE),
         `HARGA SEKARANG` = ifelse(`HARGA SEKARANG` == '-' | is.na(`HARGA SEKARANG`), 0, `HARGA SEKARANG`),
         `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>%
  collect() %>%
  
  mutate(`PERUBAHAN (Rp)` = gsub('.', '', `PERUBAHAN (Rp)`, fixed = TRUE),
         `PERUBAHAN (Rp)` = ifelse(`PERUBAHAN (Rp)` == '-' | is.na(`PERUBAHAN (Rp)`), 0, `PERUBAHAN (Rp)`),
         `PERUBAHAN (Rp)` = as.integer(`PERUBAHAN (Rp)`)) %>%
  collect() %>%
  
  mutate(`PERUBAHAN (%)` = ifelse(`PERUBAHAN (%)` == '-' | is.na(`PERUBAHAN (%)`), 0, gsub('%', '', `PERUBAHAN (%)`, fixed = TRUE)),
         `PERUBAHAN (%)` = as.double(`PERUBAHAN (%)`)) %>%
  collect() %>%
  
  mutate(`PERUBAHAN (%)` = ifelse(`HARGA KEMARIN` != 0, round((`HARGA SEKARANG` - `HARGA KEMARIN`) / `HARGA KEMARIN` * 100, 4), 0)) %>%
  collect() %>%
  
  mutate(Tanggal = as.Date(Tanggal))%>%
 
  
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

View(data_manipulasi)
#collect(data_manipulasi)

# Melihat kolom dan beberapa baris data untuk memastikan kolom yang diperlukan ada
head(data_manipulasi)
str(data_manipulasi)

# Menghitung jumlah NA di setiap kolom
colSums(is.na(data_manipulasi))

# Melihat data yang memiliki nilai NA pada kolom yang penting
data_manipulasi %>%
  filter(is.na(`NAMA BAHAN POKOK`) | is.na(`HARGA SEKARANG`)) %>%
  head()

# Visualisasi dengan ggplot2
ggplot(data = data_manipulasi, aes(x = `NAMA BAHAN POKOK`, y = `HARGA SEKARANG`)) +
  geom_bar(stat = "identity", alpha = 0.7, fill = "black", color = "lightblue") +
  theme_minimal() +
  labs(title = "Harga Bahan Pokok Sekarang", x = "Nama Bahan Pokok", y = "Harga Sekarang") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = scales::comma)

