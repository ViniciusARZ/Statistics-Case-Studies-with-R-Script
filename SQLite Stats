install.packages("RSQLite")
install.packages("tidyverse")
install.packages("dbplyr")
install.packages("lubridate")
library("lubridate")

library(RSQLite)
library(tidyverse)
library(dbplyr)

#Creating DB
conn = dbConnect(SQLite(),"meteorologia.sqlite3")

#Creating Table "estacoes"
estacoes = read_csv("estacoes_p2.csv")
dbWriteTable(conn,"estacoes",estacoes)
dbListTables(conn)

#Creating Table "dados_metereologicos"

#Sample of microdados
sample = read_csv("microdados_p2.csv")
sample
hour()
#looking for unique values for stations
unique(sample[c("id_estacao")])
#looking for stations names
estacoes
#brasilia = A001; Oeiras = A354; Piracicaba = A726; Passo Fundo = A839


#Function to Chunk Data
Leitura = function(input,pos){
  message("Lido até linha ", pos)
  input = input %>%
    filter(id_estacao %in% c("A001","A354","A726","A839")) %>%
    mutate(Ano = as.Date(data,format = "%Y")) %>%
    mutate(Mes = as.Date(data,format = "%m")) %>%
    mutate(Dia = as.Date(data,format= "%d"))
  dbWriteTable(conn,"dados_metereologicos",input,append=TRUE)
}

#Columns to read
colunasleitura = cols_only(id_estacao = 'c', data = 'D', hora= 't', precipitacao_total = 'd', temperatura_max = 'd')
#Result of reading it
resultado = read_csv_chunked("microdados_p2.csv", chunk_size = 1e3, col_types= colunasleitura, callback = SideEffectChunkCallback$new(Leitura))


#Checking if worked
dbListTables(conn)

#ANOTAÇÃO: 
  #Professor, eu não sei porque não está formatando o Dia, Mes e Ano da forma certa...
  #Tentei procurar de diversas formas mas não consegui arrumar, conferi também se o formato de leitura está correto e aparentemente,
  #a variável "colunasleitura" está lendo a coluna 'data' como 'date', através da inspeção do elemento na seção Environment ao lado.
  #Sendo assim, por questão de tempo, irei colocar as queries em SQL como se tivesse dado certo...
  #Vou tirar a dúvida com a monitoria depois, mas espero que considere as Queries.

dbGetQuery(conn, "SELECT * FROM dados_metereologicos LIMIT 5")

#3 
dbGetQuery(conn, "SELECT id_estacao, Count (*) AS contagem FROM dados_metereologicos GROUP BY id_estacao")

#4
dbGetQuery(conn, "SELECT dia, mes, ano , id_estacao AS estacao, AVG(precipitacao_total) AS prec_media, AVG(temperatura_max) AS temp_media FROM dados_metereologicos GROUP BY dia")

#5
dbGetQuery(conn, "SELECT dia, mes, ano, id_estacao AS estacao, AVG(temperatura_max) AS temp_diurna FROM dados_metereologicos GROUP BY dia HAVING temperatura_max > 8 AND temperatura_max <18")

#6
#Resolução em SQL
  dbGetQuery(conn, "SELECT Ano, COUNT(*) AS Cont_Estacoes, AVG(temperatura_max) AS temp_media FROM dados_metereologicos GROUP BY Ano HAVING temp_media > 32")
  # Nenhum ano teve a temp_media maior que 32
estacoes_tidy = tbl(conn,"estacoes")
metereologia_tidy = tbl(conn,"dados_metereologicos")
resultado_completo_emtidy = metereologia_tidy %>% 
  group_by(id_estacao, Ano) %>%
  filter(metereologia_tidy %in% temperatura_max > 32)
  mutate(Ano = as.Date(data,format = "%Y")) %>%
  mutate(Mes = as.Date(data,format = "%m")) %>%
  mutate(Dia = as.Date(data,format= "%d"))
  summarize(contagem = count(Dia))
  arrange(contagem)
resultado_completo_emtidy
