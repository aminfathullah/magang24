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
  select(No, ID_KABKOT, everything())

View(data_harga)
collect(data_harga)

library(ggplot2)
library(RColorBrewer)

# geom histogram perbandingan harga
Histogram_Perbandingan_Harga <- ggplot(data_harga, aes(x = `HARGA SEKARANG`, y = `HARGA KEMARIN`)) + 
  geom_point(color = "blue", alpha = 0.7) + 
  labs(title = "Perbandingan Harga Sekarang dan Harga Kemarin", x = "Harga Sekarang", y = "Harga Kemarin") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(Histogram_Perbandingan_Harga)

# geom histogram harga sekarang
Histogram_Harga_Sekarang <- ggplot(data_harga, aes(x = `HARGA SEKARANG`,  fill = ..count..)) + 
  geom_histogram(binwidth = 30, alpha = 0.7, color = "purple", size = 0.5, 
                 position = "identity", show.legend = FALSE) +
  scale_fill_gradient(low = "blue", high = "red") +
  labs(title = "Distribusi Harga Sekarang", x = "Harga Sekarang", y = "Frekuensi") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(Histogram_Harga_Sekarang)

# geom qq
c <- ggplot(data_harga, aes(sample = `HARGA SEKARANG`))
c + stat_qq(aes(sample = `HARGA SEKARANG`, color = `HARGA SEKARANG`), 
            dparams = list(mean = mean(data_harga$`HARGA KEMARIN`, na.rm = TRUE), 
                           sd = sd(data_harga$`HARGA KEMARIN`, na.rm = TRUE)), 
            alpha = 0.7, size = 1.5) +
  scale_color_gradient(low = "blue", high = "red") +
  labs(title = "QQ Harga Sekarang vs. Harga Kemarin", x = "Harga Kemarin", y = "Harga Sekarang") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))

# geom area
c <- ggplot(data_harga, aes(x = `HARGA SEKARANG`))
c2 <- c + geom_area(stat = "bin", alpha = 0.5, color = "blue", fill = "lightblue", linetype = "solid", size = 1)
c2 <- c2 + labs(title = "Distribusi Harga Sekarang", x = "Harga Sekarang", y = "Frekuensi") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(c2)

# geom density
c <- ggplot(data_harga, aes(x = `HARGA SEKARANG`))
c2 <- c + geom_density(kernel = "gaussian", alpha = 0.7, color = "blue", fill = "lightblue", linetype = "solid", size = 1, weight = 1)
c2 <- c2 + labs(title = "Density Harga Sekarang", x = "Harga Sekarang", y = "Kepadatan") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(c2)

# geom dotplot
c <- ggplot(data_harga, aes(x = `HARGA SEKARANG`, fill = `HARGA SEKARANG`))
c2 <- c + geom_dotplot(binwidth = 0.1, alpha = 0.7, color = "blue") +
  scale_fill_gradient(low = "lightblue", high = "blue")
c2 <- c2 + labs(title = "Dot Plot Harga Sekarang", x = "Harga Sekarang", y = "Count") +
  theme_minimal() +
  theme(panel.grid.major = element_line(color = "black", linetype = "dotted"), 
        panel.grid.minor = element_line(color = "gray", linetype = "dashed"),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"))
print(c2)








