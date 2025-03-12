library(DBI)
library(RMySQL)

conn <- dbConnect(
  MySQL(),
  dbname = "defaultdb",  
  host = "restaurant-db-cs5200-practicum1-pratoshk.c.aivencloud.com",  
  port = 12512,           
  user = "avnadmin",     
  password = "AVNS_IChSyWGD4QyeTya62-E"  
)

print(dbListTables(conn))

dbDisconnect(conn)
print("Successfully connected to Aiven MySQL")
