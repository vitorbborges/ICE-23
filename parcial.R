library("data.table")
library("tidyverse")

#-------------------------------------------------------------------------------
# Criando looping para importar os indicadores padronizados
det <- c("ACESSO A CAPITAL","AMBIENTE REGULATÓRIO","CAPITAL HUMANO","CULTURA2",
         "INFRAESTRUTURA","INOVACAO","MERCADO")
det_s <- c("ACESSO_CAPITAL","AMBIENTE_REGULATORIO","CAPITAL_HUMANO","CULTURA2",
           "INFRAESTRUTURA","INOVACAO","MERCADO")

arq <- list()
names <- list()
df <- list()

for(i in 1:7){
  # Importando a base
  arq[i] <- paste0("DETERMINANTES/det-",det[i],".csv")
  names[i] <- paste0("det_",det_s[i])
  df[[i]] <- assign(paste0("det_",det_s[i]), drop_na(fread(arq[[i]],encoding = 'UTF-8',skip=1)))
}

determinantes <- df %>% 
  reduce(full_join, by=c('V1','V2'))

colnames(determinantes)[1:2] <- c("Município","UF")
write.csv(determinantes, 'DETERMINANTES/parcial (cultura).csv')
