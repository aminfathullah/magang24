iris

#Uji-t
t <- t.test(iris$Sepal.Length, iris$Petal.Width)
t
c <- cor(iris$Sepal.Length, iris$Sepal.Width)
c 

#Arima
ari <- arima(iris$Sepal.Length)

#forecasting
predict(ari, 10)

training <- iris[1:120,]
testing <- iris[131:150,]

regresi1 <- lm(Petal.Width ~ Sepal.Length+Sepal.Width+Petal.Length, data = iris)
attach(iris)
regresi1 <- lm(Petal.Width ~ Sepal.Length+Sepal.Width+Petal.Length)
summary(regresi1)

predict(regresi1, testing)

library(partykit)
dtree <- ctree(Species ~ ., data = iris)
plot(dtree)
