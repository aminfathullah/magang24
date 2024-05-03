library(dplyr)
# install.packages('dplyr')

iris %>% 
  as_tibble() %>% 
  mutate(
    b = Sepal.Length * Sepal.Width,
    c = Petal.Length * Petal.Width,
    nama = paste('bunga ', Species)
  ) %>% 
  group_by(Species) %>% 
  summarise(
    jumlah_sepal = sum(Sepal.Length)
  ) %>% 
  inner_join(iris, by='Species') %>% 
  mutate(persen_sepal = 100*Sepal.Length/jumlah_sepal)

iris %>% 
  as_tibble() %>% 
  left_join(jumlah_sepal_per_species, by = "Species")

data1 <- arrow::open_dataset('../magang24/fara/Hasil Parquet Multithreading Fara/')

data1 %>% 
  filter(SATUAN != '') %>% 
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = T)) %>% 
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN`=='-',NA,`HARGA KEMARIN`)) %>% 
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>%
  head(100) %>%
  collect()
  mutate(perubahan_int = as.integer(`PERUBAHAN (Rp)`), `HARGA SEKARANG` = as.integer(`HARGA SEKARANG`)) %>% 
  head() %>% 
  collect()

paste(1,2, 3, sep = "| ")
paste(c(1,2, 3), collapse = "| ")
paste(c(1,2, 3), c(1,2, 3), collapse = "| ", sep = ", ")


c(1,2,3, 10)


for (nama_var in colnames(iris %>% select(-Species))) {
  print(mean(iris[[nama_var]]))
}

apply(iris %>% select(-Species), 2, function(x){
  mean(x)
})
