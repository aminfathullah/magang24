iris

t <- t.test(iris$Sepal.Length, iris$Petal.Width)
t
c <- cor(iris$Sepal.Length, iris$Sepal.Width)
c

ari <- arima(iris$Sepal.Length)

predict(ari, 10)
training <- iris[1:130,]
testing <- iris[131:150,]

regresi1 <- lm(Petal.Width ~ Sepal.Length+Sepal.Width+Petal.Length, data = iris)
attach(iris)
regresi1 <- lm(Petal.Width ~ Sepal.Length+Sepal.Width+Petal.Length)
summary(regresi1)
predict(regresi1, testing)


install.packages('partykit')
library(partykit)
dtree <- ctree(Species ~  ., data = iris)
plot(dtree)
