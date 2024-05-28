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
  collect()

arrow::write_parquet(kode_wilayah, "datapasar.parquet")
View(kode_wilayah)

histogram <- ggplot(kode_wilayah, aes(x = `HARGA SEKARANG`)) + 
  geom_histogram(binwidth = 5000, alpha = 0.7, fill = "pink", color = "red", linetype = "solid", size = 0.5) +
  labs(title = "Harga Bahan Pokok Sekarang", x = "Harga Sekarang", y = "Frekuensi") +
  theme_minimal()
print(histogram)

bar <- ggplot(kode_wilayah, aes(x = `NAMA BAHAN POKOK`, y = `HARGA SEKARANG`)) + 
  geom_bar(stat = "identity", fill = "blue", color = "red") +
  labs(title = "Harga Sekarang per Nama Bahan Pokok", x = "Nama Bahan Pokok", y = "Harga Sekarang") +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme( 
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.major = element_line(color = "gray", linetype = "dotted"),
    panel.grid.minor = element_line(color = "lightgray", linetype = "dashed"),
    panel.border = element_blank(),
    axis.line = element_line(color = "black")
  )
print(bar)

qq_plot <- ggplot(kode_wilayah, aes(sample = `HARGA SEKARANG`)) +
  stat_qq(aes(color = `HARGA SEKARANG`),
          dparams = list(mean = mean(kode_wilayah$`HARGA KEMARIN`, na.rm = TRUE), 
                         sd = sd(kode_wilayah$`HARGA KEMARIN`, na.rm = TRUE)),
          alpha = 0.7, size = 1.5) +
  scale_color_gradient(low = "blue", high = "red") +
  labs(title = "Harga Sekarang dan Harga Kemarin", x = "Harga Kemarin", y = "Harga Sekarang") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"),
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(qq_plot)

area_plot <- ggplot(kode_wilayah, aes(x = `HARGA SEKARANG`)) +
  geom_area(stat = "bin", alpha = 0.5, color = "red", fill = "yellow", linetype = "solid", size = 1) +
  labs(title = "Distribusi Harga Sekarang", x = "Harga Sekarang", y = "Frekuensi") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(area_plot)

density_plot <- ggplot(kode_wilayah, aes(x = `HARGA SEKARANG`)) +
  geom_density(kernel = "gaussian", alpha = 0.7, color = "red", fill = "lightblue", linetype = "solid", size = 1) +
  labs(title = "Density Harga Sekarang", x = "Harga Sekarang", y = "Frekuensi") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(density_plot)
  
c <- ggplot(kode_wilayah, aes(x = `HARGA SEKARANG`, fill = `HARGA SEKARANG`))
dotplot <- c + geom_dotplot(binwidth = 0.1, alpha = 0.7, color = "red") +
  scale_fill_gradient(low = "lightblue", high = "red")
dotplot <- dotplot + labs(title = "Dot Plot Harga Sekarang", x = "Harga Sekarang", y = "Count") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(dotplot)
