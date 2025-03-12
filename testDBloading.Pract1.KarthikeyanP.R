library(DBI)
library(RMySQL)
library(dplyr)
library(readr)

db_host <- "restaurant-db-cs5200-practicum1-pratoshk.c.aivencloud.com"
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

cat("✅ Connected to MySQL\n")

csv_file <- "restaurant-visits-139874.csv"
df_csv <- read_csv(csv_file, show_col_types = FALSE)

cat("✅ CSV Data Loaded Successfully\n")

df_csv <- df_csv %>%
  mutate(across(where(is.character), trimws)) %>%
  mutate(
    ServerEmpID = as.character(ServerEmpID),  
    VisitID = as.character(VisitID),
    FoodBill = as.numeric(FoodBill),
    AlcoholBill = as.numeric(AlcoholBill),  
    TipAmount = as.numeric(TipAmount)
  )

csv_counts <- list(
  restaurants = n_distinct(df_csv$Restaurant, na.rm = TRUE),
  customers = n_distinct(df_csv$CustomerName, na.rm = TRUE),
  servers = n_distinct(df_csv$ServerEmpID, na.rm = TRUE),
  visits = n_distinct(df_csv$VisitID, na.rm = TRUE)
)

db_counts <- list(
  restaurants = dbGetQuery(con, "SELECT COUNT(*) AS count FROM RESTAURANT;")$count,
  customers = dbGetQuery(con, "SELECT COUNT(*) AS count FROM CUSTOMER;")$count,
  servers = dbGetQuery(con, "SELECT COUNT(*) AS count FROM SERVER;")$count,
  visits = dbGetQuery(con, "SELECT COUNT(*) AS count FROM VISIT;")$count
)

cat("\n **Validation Results** \n")
for (key in names(csv_counts)) {
  if (csv_counts[[key]] == db_counts[[key]]) {
    cat(sprintf("Count Matches! (CSV: %d, DB: %d)\n", key, csv_counts[[key]], db_counts[[key]]))
  } else {
    cat(sprintf("Count Mismatch! (CSV: %d, DB: %d)\n", key, csv_counts[[key]], db_counts[[key]]))
  }
}

csv_sums <- df_csv %>%
  summarize(
    total_food = sum(FoodBill, na.rm = TRUE),
    total_alcohol = sum(AlcoholBill, na.rm = TRUE),
    total_tips = sum(TipAmount, na.rm = TRUE)
  )

db_sums <- dbGetQuery(con, "SELECT 
                                SUM(FoodBill) AS total_food, 
                                SUM(AlcoholBill) AS total_alcohol, 
                                SUM(TipAmount) AS total_tips 
                            FROM BILL;")

csv_sums$total_tips <- sprintf("%.2f", csv_sums$total_tips)
db_sums$total_tips <- sprintf("%.2f", db_sums$total_tips)

csv_sums$total_alcohol <- sprintf("%.5f", csv_sums$total_alcohol)
db_sums$total_alcohol <- sprintf("%.5f", db_sums$total_alcohol)

cat("\n **Amount Validation** \n")
for (col in names(csv_sums)) {
  if (csv_sums[[col]] == db_sums[[col]]) {
    cat(sprintf(" Matches! (CSV: %s, DB: %s)\n", col, csv_sums[[col]], db_sums[[col]]))
  } else {
    cat(sprintf(" Mismatch! (CSV: %s, DB: %s)\n", col, csv_sums[[col]], db_sums[[col]]))
  }
}

dbDisconnect(con)
