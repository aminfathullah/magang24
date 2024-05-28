library(dplyr)

data1 <- arrow::open_dataset('../magang24/fara/Hasil Parquet Multithreading Fara/')

summari_harga_kemarin <- data1 %>% 
  filter(SATUAN != '') %>% 
  mutate(`HARGA KEMARIN` = gsub('.', '', `HARGA KEMARIN`, fixed = T)) %>% 
  mutate(`HARGA KEMARIN` = ifelse(`HARGA KEMARIN`=='-',NA,`HARGA KEMARIN`)) %>% 
  mutate(`HARGA KEMARIN` = as.integer(`HARGA KEMARIN`)) %>% 
  filter(!is.na(`HARGA KEMARIN`)) %>% 
  # filter(`HARGA KEMARIN`=='')
  summarise(mean_harga_kemarin = mean(`HARGA KEMARIN`),
            min_harga_kemarin = min(`HARGA KEMARIN`),
            q3_harga_kemarin = quantile(`HARGA KEMARIN`, 0.75),
            var_harga_kemarin = var(`HARGA KEMARIN`),
            std_harga_kemarin = var(`HARGA KEMARIN`)^0.5
            ) %>% 
  mutate(cv = std_harga_kemarin/ mean_harga_kemarin) %>% 
  collect()

hist(iris$Sepal.Length)
hist(iris$Sepal.Width)

(var(iris$Sepal.Length)^0.5)/mean(iris$Sepal.Length)
(var(iris$Sepal.Width)^0.5)/mean(iris$Sepal.Width)

library(ggplot2)
summary(iris$Sepal.Width)
histogram <- ggplot(iris, aes(Sepal.Width)) + geom_histogram(binwidth = 0.2)
histogram

grafik_batang <- ggplot(iris, aes(Species)) + geom_bar()
grafik_batang

grafik_kolom <- ggplot(iris, aes(Species, Petal.Width)) + geom_col()
grafik_kolom

boxplot <- ggplot(iris, aes(1, Petal.Width)) + geom_boxplot()
boxplot

scatter <- ggplot(iris, aes(Petal.Width, Sepal.Length)) + geom_point() + labs(title = 'Korelasi antara sepal width dan sepal length')
cor(iris$Petal.Width, iris$Sepal.Length)
cor(iris$Petal.Width, iris$Sepal.Width)
scatter


library(plotly)

fig <- plot_ly(data = iris, x = ~Sepal.Length, y = ~Petal.Length, color = ~Species)

fig
