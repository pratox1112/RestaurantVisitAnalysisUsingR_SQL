
load_restaurant_data <- function(con, df) {
  unique_restaurants <- df %>%
    dplyr::select(Restaurant) %>%
    dplyr::distinct() %>%
    dplyr::filter(!is.na(Restaurant) & Restaurant != "")
  
  failed_count <- 0
  
  for (i in seq_len(nrow(unique_restaurants))) {
    rest_name <- unique_restaurants$Restaurant[i]
    safe_name <- gsub("'", "\\'", rest_name)
    query <- sprintf("INSERT INTO RESTAURANT (Restaurant) VALUES ('%s')", safe_name)
    
    tryCatch({
      dbExecute(con, query)
    }, error = function(e) {
      failed_count <<- failed_count + 1
    })
  }
  
  res <- dbGetQuery(con, "SELECT COUNT(*) AS total FROM RESTAURANT;")
}

insert_server_restaurant_data <- function(con, df_raw) {
  if (nrow(df_raw) == 0) {
    return(NULL)
  }
  
  df_server_restaurant <- df_raw %>%
    select(ServerEmpID, Restaurant, StartDateHired, EndDateHired, HourlyRate) %>%
    filter(!is.na(ServerEmpID) & !is.na(Restaurant)) %>%
    distinct()
  
  restaurant_map <- dbGetQuery(con, "SELECT RestaurantID, Restaurant FROM RESTAURANT;")
  
  df_server_restaurant <- df_server_restaurant %>%
    left_join(restaurant_map, by = "Restaurant") %>%
    select(ServerEmpID, RestaurantID, StartDateHired, EndDateHired, HourlyRate) %>%
    filter(!is.na(RestaurantID))
  
  df_server_restaurant$StartDateHired <- parse_date_time(df_server_restaurant$StartDateHired, orders = c("mdy", "dmy", "ymd"))
  df_server_restaurant$StartDateHired <- format(df_server_restaurant$StartDateHired, "%Y-%m-%d")
  df_server_restaurant$StartDateHired[is.na(df_server_restaurant$StartDateHired)] <- "1900-01-01"
  
  df_server_restaurant$EndDateHired <- parse_date_time(df_server_restaurant$EndDateHired, orders = c("mdy", "dmy", "ymd"))
  df_server_restaurant$EndDateHired <- format(df_server_restaurant$EndDateHired, "%Y-%m-%d")
  df_server_restaurant$EndDateHired[is.na(df_server_restaurant$EndDateHired)] <- "1900-01-01"
  
  dbExecute(con, "SET FOREIGN_KEY_CHECKS=0;")
  
  failed_rows <- c()
  
  for (i in 1:nrow(df_server_restaurant)) {
    query <- sprintf("
      INSERT INTO SERVER_RESTAURANT (ServerEmpID, RestaurantID, StartDateHired, EndDateHired, HourlyRate)
      VALUES (%s, %s, '%s', '%s', %s)
      ON DUPLICATE KEY UPDATE StartDateHired=VALUES(StartDateHired), EndDateHired=VALUES(EndDateHired), HourlyRate=VALUES(HourlyRate);
    ",
                     df_server_restaurant$ServerEmpID[i],
                     df_server_restaurant$RestaurantID[i],
                     df_server_restaurant$StartDateHired[i],
                     df_server_restaurant$EndDateHired[i],
                     df_server_restaurant$HourlyRate[i]
    )
    
    tryCatch({
      dbExecute(con, query)
    }, error = function(e) {
      failed_rows <<- c(failed_rows, i)
    })
  }
  
  dbExecute(con, "SET FOREIGN_KEY_CHECKS=1;")
  
  if (length(failed_rows) > 0) {
    print(failed_rows)
    print(df_server_restaurant[failed_rows, ])
  }
}


prepare_and_insert_server_data <- function(con, df.orig) {
  df.server <- df.orig %>%
    dplyr::select(ServerEmpID, ServerName, ServerBirthDate, ServerTIN) %>%
    dplyr::distinct() %>%
    dplyr::filter(!is.na(ServerEmpID))
  
  df.server$ServerBirthDate <- lubridate::parse_date_time(df.server$ServerBirthDate, orders = c("mdy", "dmy", "ymd"))
  df.server$ServerBirthDate <- format(df.server$ServerBirthDate, "%Y-%m-%d")
  df.server$ServerBirthDate[is.na(df.server$ServerBirthDate)] <- "1900-01-01"
  
  missing_tins <- which(is.na(df.server$ServerTIN) | df.server$ServerTIN == "")
  if (length(missing_tins) > 0) {
    for (i in seq_along(missing_tins)) {
      df.server$ServerTIN[missing_tins[i]] <- paste0("UNKNOWN_TIN_", i)
    }
  }
  
  failed_rows <- c()
  
  for (i in seq_len(nrow(df.server))) {
    safeName <- gsub("'", "\\'", df.server$ServerName[i])
    safeTIN  <- gsub("'", "\\'", df.server$ServerTIN[i])
    
    query <- sprintf("
      INSERT INTO SERVER (ServerEmpID, ServerName, ServerBirthDate, ServerTIN)
      VALUES (%s, '%s', '%s', '%s');
    ",
                     df.server$ServerEmpID[i],
                     safeName,
                     df.server$ServerBirthDate[i],
                     safeTIN
    )
    
    tryCatch({
      DBI::dbExecute(con, query)
    }, error = function(e) {
      failed_rows <<- c(failed_rows, i)
    })
  }
  
  count_query <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS total FROM SERVER;")
  
  head_data <- DBI::dbGetQuery(con, "SELECT * FROM SERVER LIMIT 10;")
  
  csv_ids <- unique(df.server$ServerEmpID)
  db_ids  <- DBI::dbGetQuery(con, "SELECT ServerEmpID FROM SERVER;")
  db_ids$ServerEmpID <- as.numeric(db_ids$ServerEmpID)
  
  missing_ids <- setdiff(csv_ids, db_ids$ServerEmpID)
  if (length(missing_ids) > 0) {
    missing_details <- dplyr::filter(df.server, ServerEmpID %in% missing_ids)
  }
  
  if (length(failed_rows) > 0) {
    print(df.server[failed_rows, ])
  }
}

insert_mealtype_data <- function(con) {
  dbExecute(con, "SET FOREIGN_KEY_CHECKS=0;")
  dbExecute(con, "TRUNCATE TABLE MEALTYPE;")
  
  meal_types <- c("Breakfast", "Lunch", "Dinner", "Takeout")
  
  for (meal in meal_types) {
    query <- sprintf("INSERT INTO MEALTYPE (MealTypeName) VALUES ('%s');", meal)
    dbExecute(con, query)
  }
  
  dbExecute(con, "SET FOREIGN_KEY_CHECKS=1;")
}


insert_customer_data <- function(con, df_raw) {
  df_customer <- df_raw %>%
    select(CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) %>%
    filter(!is.na(CustomerName)) %>%
    distinct()
  
  df_customer$CustomerPhone[is.na(df_customer$CustomerPhone)] <- "UNKNOWN_PHONE"
  df_customer$CustomerEmail[is.na(df_customer$CustomerEmail)] <- "UNKNOWN_EMAIL"
  df_customer$LoyaltyMember[is.na(df_customer$LoyaltyMember)] <- FALSE
  
  failed_rows <- c()
  
  for (i in 1:nrow(df_customer)) {
    query <- sprintf("
      INSERT INTO CUSTOMER (CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember)
      VALUES ('%s', '%s', '%s', %s)
      ON DUPLICATE KEY UPDATE CustomerPhone=VALUES(CustomerPhone), CustomerEmail=VALUES(CustomerEmail), LoyaltyMember=VALUES(LoyaltyMember);
    ",
                     gsub("'", "''", df_customer$CustomerName[i]),
                     gsub("'", "''", df_customer$CustomerPhone[i]),
                     gsub("'", "''", df_customer$CustomerEmail[i]),
                     as.integer(df_customer$LoyaltyMember[i])
    )
    
    tryCatch({
      dbExecute(con, query)
    }, error = function(e) {
      failed_rows <<- c(failed_rows, i)
    })
  }
}

load_and_insert_visit_data <- function(con, df_raw, batch_size = 500) {
  if (!dbIsValid(con)) {
    stop("MySQL connection is not valid.")
  }
  
  df_visit <- df_raw %>%
    select(VisitID, Restaurant, ServerEmpID, CustomerName, MealType, VisitDate, VisitTime, PartySize, WaitTime) %>%
    distinct()
  
  df_visit$VisitDate <- parse_date_time(df_visit$VisitDate, orders = c("mdy", "dmy", "ymd"))
  df_visit$VisitDate <- format(df_visit$VisitDate, "%Y-%m-%d")
  
  df_visit$VisitTime <- trimws(df_visit$VisitTime)
  df_visit$VisitTime <- gsub(":00:00$", "", df_visit$VisitTime)
  df_visit$VisitTime <- ifelse(nchar(df_visit$VisitTime) == 5, paste0(df_visit$VisitTime, ":00"), df_visit$VisitTime)
  df_visit$VisitTime <- ifelse(is.na(df_visit$VisitTime) | df_visit$VisitTime == "NULL", "00:00:00", df_visit$VisitTime)
  
  restaurant_map <- dbGetQuery(con, "SELECT RestaurantID, Restaurant FROM RESTAURANT;")
  mealtype_map <- dbGetQuery(con, "SELECT MealTypeID, MealTypeName FROM MEALTYPE;")
  customer_map <- dbGetQuery(con, "SELECT CustomerID, CustomerName FROM CUSTOMER;")
  
  if (nrow(restaurant_map) == 0 | nrow(mealtype_map) == 0 | nrow(customer_map) == 0) {
    stop("Required foreign key data not found.")
  }
  
  df_visit <- df_visit %>%
    left_join(restaurant_map, by = "Restaurant") %>%
    left_join(mealtype_map, by = c("MealType" = "MealTypeName")) %>%
    left_join(customer_map, by = "CustomerName")
  
  df_visit <- df_visit %>%
    select(VisitID, RestaurantID, ServerEmpID, CustomerID, MealTypeID, VisitDate, VisitTime, PartySize, WaitTime)
  
  df_visit$RestaurantID <- ifelse(is.na(df_visit$RestaurantID), "NULL", df_visit$RestaurantID)
  df_visit$ServerEmpID <- ifelse(is.na(df_visit$ServerEmpID), "NULL", df_visit$ServerEmpID)
  df_visit$CustomerID <- ifelse(is.na(df_visit$CustomerID), "NULL", df_visit$CustomerID)
  df_visit$MealTypeID <- ifelse(is.na(df_visit$MealTypeID), "NULL", df_visit$MealTypeID)
  
  df_visit$PartySize <- ifelse(is.na(df_visit$PartySize), 0, as.integer(df_visit$PartySize))
  df_visit$WaitTime <- ifelse(is.na(df_visit$WaitTime), 0, as.integer(df_visit$WaitTime))
  
  total_batches <- ceiling(nrow(df_visit) / batch_size)
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_visit))
    
    batch_values <- character()
    
    for (i in start_idx:end_idx) {
      row <- df_visit[i, ]
      
      value_string <- sprintf("(%d, %s, %s, %s, %s, '%s', '%s', %d, %d)",
                              row$VisitID,
                              ifelse(is.na(row$RestaurantID), "NULL", row$RestaurantID),
                              ifelse(is.na(row$ServerEmpID), "NULL", row$ServerEmpID),
                              ifelse(is.na(row$CustomerID), "NULL", row$CustomerID),
                              ifelse(is.na(row$MealTypeID), "NULL", row$MealTypeID),
                              row$VisitDate, row$VisitTime,
                              row$PartySize, row$WaitTime)
      
      batch_values <- c(batch_values, value_string)
    }
    
    if (length(batch_values) > 0) {
      batch_query <- paste("INSERT INTO VISIT (VisitID, RestaurantID, ServerEmpID, CustomerID, MealTypeID, VisitDate, VisitTime, PartySize, WaitTime) VALUES",
                           paste(batch_values, collapse = ", "), ";")
      
      tryCatch({
        dbExecute(con, batch_query)
      }, error = function(e) {})
    }
  }
}


process_and_insert_gender_count <- function(con, df) {
  df_gender <- df %>%
    select(VisitID, Genders) %>%
    mutate(
      MaleCount = str_count(Genders, "m"),
      FemaleCount = str_count(Genders, "f"),
      UnspecifiedCount = str_count(Genders, "u")
    ) %>%
    group_by(VisitID) %>%
    summarise(
      MaleCount = sum(MaleCount, na.rm = TRUE),
      FemaleCount = sum(FemaleCount, na.rm = TRUE),
      UnspecifiedCount = sum(UnspecifiedCount, na.rm = TRUE)
    ) %>%
    ungroup()
  
  valid_visit_ids <- dbGetQuery(con, "SELECT VisitID FROM VISIT;")$VisitID
  df_gender <- df_gender %>% filter(VisitID %in% valid_visit_ids)
  
  batch_size <- 500
  total_batches <- ceiling(nrow(df_gender) / batch_size)
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_gender))
    
    batch_values <- character()
    
    for (i in start_idx:end_idx) {
      row <- df_gender[i, ]
      value_string <- sprintf("(%d, %d, %d, %d)",
                              row$VisitID, row$MaleCount, row$FemaleCount, row$UnspecifiedCount)
      batch_values <- c(batch_values, value_string)
    }
    
    if (length(batch_values) > 0) {
      batch_query <- paste("INSERT INTO GENDERCOUNT (VisitID, MaleCount, FemaleCount, UnspecifiedCount) VALUES",
                           paste(batch_values, collapse = ", "), ";")
      
      tryCatch({
        dbExecute(con, batch_query)
      }, error = function(e) {})
    }
  }
}


insert_payment <- function(con, df) {
  df_payment <- df %>%
    mutate(PaymentMethod = trimws(tolower(PaymentMethod))) %>%
    select(PaymentMethod) %>%
    distinct() %>%
    filter(!is.na(PaymentMethod) & PaymentMethod != "")
  
  existing_payment_methods <- dbGetQuery(con, "SELECT DISTINCT PaymentMethod FROM PAYMENTMETHOD;")$PaymentMethod
  df_payment <- df_payment %>% filter(!(PaymentMethod %in% existing_payment_methods))
  
  if (nrow(df_payment) == 0) {
    return()
  }
  
  batch_size <- 500
  total_batches <- ceiling(nrow(df_payment) / batch_size)
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_payment))
    
    batch_values <- c()
    
    for (i in start_idx:end_idx) {
      row <- df_payment[i, ]
      value_string <- sprintf("('%s')", row$PaymentMethod)
      batch_values <- c(batch_values, value_string)
    }
    
    if (length(batch_values) > 0) {
      batch_query <- paste("INSERT INTO PAYMENTMETHOD (PaymentMethod) VALUES",
                           paste(batch_values, collapse = ", "), ";")
      tryCatch({
        dbExecute(con, batch_query)
      }, error = function(e) {})
    }
  }
}



insert_bill_data <- function(con, df_bill, batch_size = 500) {
  visit_map <- dbGetQuery(con, "SELECT DISTINCT VisitID FROM VISIT;")
  df_bill <- df_bill %>%
    filter(VisitID %in% visit_map$VisitID)
  
  payment_map <- dbGetQuery(con, "SELECT PaymentMethodID, PaymentMethod FROM PAYMENTMETHOD_LOOKUP;")
  
  df_bill <- df_bill %>%
    left_join(payment_map, by = "PaymentMethod") %>%
    select(VisitID, PaymentMethodID, FoodBill, TipAmount, DiscountApplied, orderedAlcohol, AlcoholBill)
  
  df_bill <- df_bill %>%
    mutate(
      VisitID = as.integer(VisitID),
      PaymentMethodID = ifelse(is.na(PaymentMethodID), "NULL", as.integer(PaymentMethodID)),
      FoodBill = ifelse(is.na(FoodBill), 0, as.numeric(FoodBill)),
      TipAmount = ifelse(is.na(TipAmount), 0, as.numeric(TipAmount)),
      DiscountApplied = ifelse(is.na(DiscountApplied), 0, as.numeric(DiscountApplied)),
      OrderedAlcohol = ifelse(is.na(orderedAlcohol), 0, as.integer(orderedAlcohol)),
      AlcoholBill = ifelse(is.na(AlcoholBill), 0, as.numeric(AlcoholBill)),
      TotalAmount = FoodBill + TipAmount + AlcoholBill
    )
  
  total_batches <- ceiling(nrow(df_bill) / batch_size)
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_bill))
    
    batch_values <- c()
    
    for (i in start_idx:end_idx) {
      row <- df_bill[i, ]
      
      value_string <- sprintf("(%d, %s, %.2f, %.2f, %.2f, %d, %.5f, %.2f)",
                              row$VisitID,
                              ifelse(is.na(row$PaymentMethodID) | row$PaymentMethodID == "NA", "NULL", row$PaymentMethodID),
                              ifelse(is.na(row$FoodBill) | row$FoodBill == "NA", 0, row$FoodBill),
                              ifelse(is.na(row$TipAmount) | row$TipAmount == "NA", 0, row$TipAmount),
                              ifelse(is.na(row$DiscountApplied) | row$DiscountApplied == "NA", 0, row$DiscountApplied),
                              ifelse(is.na(row$OrderedAlcohol) | row$OrderedAlcohol == "NA", 0, row$OrderedAlcohol),
                              ifelse(is.na(row$AlcoholBill) | row$AlcoholBill == "NA", 0, row$AlcoholBill),
                              ifelse(is.na(row$TotalAmount) | row$TotalAmount == "NA", 0, row$TotalAmount))
      
      batch_values <- c(batch_values, value_string)
    }
    
    if (length(batch_values) > 0) {
      batch_query <- paste("INSERT INTO BILL (VisitID, PaymentMethodID, FoodBill, TipAmount, DiscountApplied, OrderedAlcohol, AlcoholBill, TotalAmount) VALUES",
                           paste(batch_values, collapse = ", "), ";")
      tryCatch({
        dbExecute(con, batch_query)
      }, error = function(e) {})
    }
  }
}

insert_payment_method_lookup <- function(con, df){
  df_payment <- df %>%
    select(PaymentMethod) %>%
    distinct() %>%
    filter(!is.na(PaymentMethod) & PaymentMethod != "")
  
  batch_size <- 500
  total_batches <- ceiling(nrow(df_payment) / batch_size)
  
  for (batch in 1:total_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(df_payment))
    
    batch_values <- c()
    
    for (i in start_idx:end_idx) {
      row <- df_payment[i, ]
      value_string <- sprintf("('%s')", row$PaymentMethod)
      batch_values <- c(batch_values, value_string)
    }
    
    if (length(batch_values) > 0) {
      batch_query <- paste("INSERT INTO PAYMENTMETHOD_LOOKUP (PaymentMethod) VALUES",
                           paste(batch_values, collapse = ", "), ";")
      tryCatch({
        dbExecute(con, batch_query)
      }, error = function(e) {})
    }
  }
}


con <- dbConnect(RMySQL::MySQL(),
                 host = "restaurant-db-cs5200-practicum1-pratoshk.c.aivencloud.com",
                 user = "avnadmin",
                 password = "AVNS_IChSyWGD4QyeTya62-E",
                 dbname = "defaultdb",
                 port = 12512)

cat("Connected to MySQL\n")

csv_file <- "restaurant-visits-139874.csv"
df_orig <- read_csv(csv_file, show_col_types = FALSE)

prepare_and_insert_server_data(con, df_orig)
load_restaurant_data(con, df_orig)
insert_server_restaurant_data(con, df_orig)
insert_customer_data(con, df_orig)
insert_mealtype_data(con)
load_and_insert_visit_data(con, df_orig)
process_and_insert_gender_count(con, df_orig)
insert_payment_method_lookup(con, df_orig)
insert_bill_data(con, df_orig)



dbDisconnect(con)


