install.packages("RSQLite")
install.packages("tidyverse")

library(RSQLite)
file.exists(('disco.db'))
path = "/cloud/project"
fname = file.path(path, "disco.db")

disco2 = copy(conn)
## Conexão com o Arquivo: dbConnect
conn = dbConnect(SQLite(),fname)
conn

#Listar todas as Tabelas
dbListTables(conn)
dbListFields(conn, "customers")

#Armazenar uma Query em uma Variável
sql1 = "SELECT * FROM customers LIMIT 5"

#Executar uma Query dentro do db
dbGetQuery(conn, sql1)

#Executar uma ação que NÃO É Query
dbExecute(conn, "CREATE TABLE Example (FirstName STRING")

#Condição de Numero de caracteres WHERE Column GLOB "?????" ? é um caracter

#Para colocar Join, use o standard "column.table"

#Para dar Mais de um Join, use TYPE JOIN table, TYPE JOIN, ON table.column = table.column AND table.column = table.column

#Inserir dados em uma tabela, INSERT INTO table VALUES ('valorcol1','valorcol2','valorcol3');

#Inserir várias linhas em uma tabela
dbWriteTable(db,"nome",dataframe)


library(tidyverse)
# armazenar uma tabela em dataframe para usar em uma criação de base de dados
conexao = dbConnect(SQLite(),"voos.sqlite3")
airlines = read_csv("airlines.csv")
airlines
dbWriteTable(conexao,"airlines",airlines)
#Conferir se a tabela foi criada
dbListTables(conexao)

airports = read_csv("airports.csv")
airports
dbWriteTable(conexao,"airports",airports)
dbListTables(conexao)


#Ler arquivos de forma chuncada para gastar menos RAM
#Função de CallBack para a leitura
LerDados = function(input, pos){
  message("A leitura do arquivo atingiu a linha ", pos)
  input = input %>%
    filter(ORIGIN_AIRPORT %in% c("BWI", "MIA", "SEA", "SFO", "JFK"),
           DESTINATION_AIRPORT %in% c("BWI", "MIA", "SEA", "SFO", "JFK"))
  dbWriteTable(conexao,"flights",input, append = TRUE)
}
colunasleitura = cols_only(YEAR = 'i', MONTH = 'i', DAY = 'i', AIRLINE = 'c', FLIGHT_NUMBER = 'i', 
                           ORIGIN_AIRPORT = 'c', DESTINATION_AIRPORT = 'c', ARRIVAL_DELAY = 'i')

amostra = read_csv("flights.csv",n_max=10)
amostra
resultado = read_csv_chunked("flights.csv", chunk_size = 1e5, col_types= colunasleitura, callback = SideEffectChunkCallback$new(LerDados))
dbListTables(conexao)
dbGetQuery(conexao,"SELECT* FROM flights")

sql_atrasomedio = paste("SELECT destination_airport, AVG(arrival_delay) AS Atraso_Medio",
                        "FROM flights",
                        "GROUP BY destination_airport",
                        "ORDER BY Atraso_Medio DESC")
resultado_medias = dbGetQuery(conexao,sql_atrasomedio)

#Transformar em Tabela para o R
resultado_medias %>% collect()
#E em formato de SQL
resultado_media %>% show_query()

install.packages("dbplyr")
library(dbplyr)

tabela = tbl(conexao, "flights")
tabela

resultado_medias_emtidy = tabela %>% 
  group_by(DESTINATION_AIRPORT) %>%
  summarise(atraso_medio = mean(ARRIVAL_DELAY, na.rm = TRUE))
resultado_medias_emtidy
#Como o R faria isso em SQL?
resultado_medias_emtidy %>% show_query()

flights_tidy = tbl(conexao,"flights")
airports_tidy = tbl(conexao,"airports")
resultado_completo_emtidy = flights_tidy %>% 
  inner_join(airports_tidy, by = c('DESTINATION_AIRPORT' = 'IATA_CODE')) %>%
    group_by(DESTINATION_AIRPORT, AIRPORT) %>%
    summarize(atraso_medio = mean(ARRIVAL_DELAY, na.rm=TRUE)) %>%
    arrange(-atraso_medio)
resultado_completo_emtidy
#Como o R faria isso em SQL?
resultado_completo_emtidy %>% show_query()