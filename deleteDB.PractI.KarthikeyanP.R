library(DBI)
library(RMySQL)

db_host <- "restaurant-db-cs5200-praticum1-pratoshk.c.aivencloud.com"
db_user <- "avnadmin"
db_password <- "AVNS_IChSyWGD4QyeTya62-E"
db_name <- "defaultdb"
db_port <- 12512

con <- dbConnect(RMySQL::MySQL(),
                 host = db_host,
                 user = db_user,
                 password = db_password,
                 dbname = db_name,
                 port = db_port)

execute_query <- function(query) {
  tryCatch({
    dbExecute(con, query)
  }, error = function(e) {})
}

execute_query("SET FOREIGN_KEY_CHECKS=0;")
execute_query("DROP TABLE IF EXISTS BILL;")
execute_query("DROP TABLE IF EXISTS PAYMENTMETHOD_LOOKUP;")
execute_query("DROP TABLE IF EXISTS GENDERCOUNT;")
execute_query("DROP TABLE IF EXISTS VISIT;")
execute_query("DROP TABLE IF EXISTS SERVER_RESTAURANT;")
execute_query("DROP TABLE IF EXISTS SERVER;")
execute_query("DROP TABLE IF EXISTS CUSTOMER;")
execute_query("DROP TABLE IF EXISTS MEALTYPE;")
execute_query("DROP TABLE IF EXISTS RESTAURANT;")
execute_query("SET FOREIGN_KEY_CHECKS=1;")


dbDisconnect(con)
