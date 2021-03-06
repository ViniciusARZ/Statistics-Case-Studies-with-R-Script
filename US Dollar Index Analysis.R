#Carregando Pacotes
library(lubridate)
library(RSQLite)
library(tidyverse)
library(dplyr)
library(ggplot2)
#Consulta do N�mero de Arquivos
dir()

### PSEUDO-CODIGO
###1. Criar uma fun��o processaDados que receba os arquvios,processa cada um deles,
  ###E deposite os dados em uma base chamada bigmac.sqlite3, assim como na pr�pria pasta.
  ###O banco dever� ter uma conex�o e conter as colunas:
      ###Cols
        ###data:date
        ###cod_pais:
        ###cod_moeda:
        ###nome_pais:
        ###preco_local:Col local_price
        ###preco_dolar: dollar_price
        ###cotacao:dolar_ex
        ###indice_dolar:usd_raw
#observa��o do arquivo antes de fazer a fun��o
fname = "indice_bigmac_UKR_Ukraine_UAH.csv"

#Fun��o para reasolver os problemas da atividade 1
processaDados <- function(fname) {
  pares = (fname %>% str_remove(".csv") %>% str_split("_") %>% unlist())[3:5]
  conteudo = read_csv(fname, show_col_types = FALSE)
  names(conteudo) = tolower(names(conteudo))
  conteudo %>% select(date,local_price,dollar_price,dollar_ex,usd_raw) %>% 
    transmute(data = date(date),
              cod_pais = pares[1],
              nome_pais = str_to_lower(pares[2]),
              cod_moeda = pares[3],
              preco_dolar = dollar_price,
              preco_local = local_price,
              cotacao = dollar_ex,
              indice_dolar = usd_raw)
}
#Conexao com a base de dados requisitada
conn = dbConnect(SQLite(),"bigmac.sqlite3")

#Fun��o para rodar a fun��o de cima na pasta de arquivos
for (arquivo in list.files(pattern = ".csv$")){
  message("Rodando em: ", arquivo)
  input = processaDados(arquivo)
  dbWriteTable(conn,"ibm",input,append=TRUE)
  message("Terminei esse arquivo: ", arquivo, " :)")
}

#Teste para ver se deu certo
dbGetQuery(conn,"SELECT * FROM ibM LIMIT 5")

###2.Construa uma fun��o denominada obsPais que conta quantas observa��es foram 
  ###obtidas para cada um:
    ###recebe 2 argumentos, conex�o e paises
    ### calcula o n�mero de observa��es para cada pa�s utilizando SQL
    ### Ordena de maneira decrescente
    ### retorna um dataframe dos pa�ses informados na chamada da fun��o

#Fun��o para pegar o n�mero de registros por pa�s
obsPais = function(conexao, paises){
  dbGetQuery(conn,"SELECT nome_pais, COUNT(cod_pais) AS Contagem_Registros FROM ibm GROUP BY nome_pais ORDER BY Contagem_Registros DESC") %>%   
    filter(nome_pais == paises)
}
paises = c("united states","thailand")
obsPais(conn, paises)

###3.Crie uma fun��o chamada coletaDados para obter as observa��es referentes a pa�ses de interesse
  ### recebe 2 argumentos, conex�o e paises
  ### retorna um df com: data, nome_pais, preco_dolar e indice_dolar
  ### coluna data deve ser formato D
  ### a sa�da deve conter apenas os pa�ses do argumento
  ### a extra��o deve ser com SQL
  ### ordena��o pode ser fora do sql (pa�s e data)
#4.Gr�fico de linha dos pa�ses Argentina, Brazil e United States
  ###Argentina - Vermelho
  ###Brazil - Verde
  ###United States - Azul

#Fun��o para coletar os dados dos pa�ses

#SQL para Resolu��o da 3
sql2 = paste("SELECT data, nome_pais, preco_dolar, indice_dolar FROM ibm ORDER BY data, nome_pais DESC")
#No caso da 3, seria s� apagar a parte do ggplot da fun��o. Para fins ilustrativos:

  #coletaDados <- function(conn,paises) {
  
    # df = dbGetQuery(conn,sql3)   
    #df %>% as_tibble() %>%
    # filter(nome_pais == paises) %>%
      #mutate(data = as.Date(data,"%d-%m-%Y")) %>%}

#SQL para resolu��o da 4 e o Gr�fico
sql3 = paste("SELECT data, nome_pais, preco_dolar, indice_dolar FROM ibm ORDER BY data, nome_pais DESC")

coletaDados <- function(conn,paises) {
  
    df = dbGetQuery(conn,sql3)   
    df %>% as_tibble() %>%
      filter(nome_pais == paises) %>%
      mutate(data = as.Date(data,"%d-%m-%Y")) %>%
      ggplot(aes(x=data, y=indice_dolar,group=nome_pais)) + geom_line(aes(color=nome_pais)) + theme_bw() + 
      labs(x='Data',y='�ndice Big Mac (n�o ajustado) em USD',title = "Evolu��o do �ndice Big Mac",
           subtitle = "Compara��o: Argentina, Brasil, Estados Unidos", )

  }

#Replicar o gr�fico
paises = c("argentina","brazil","united states")
coletaDados(conn, paises)