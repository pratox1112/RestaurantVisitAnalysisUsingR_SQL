# createDB_PractI_KarthikeyanP.R
# Author: Pratosh Karthikeyan
# Semester: Spring 2025
# Description: Create table in the cloud server

library(DBI)
library(RMySQL)

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

execute_query <- function(sql) {
  dbExecute(con, sql)
}

execute_query("
  CREATE TABLE IF NOT EXISTS RESTAURANT (
    RestaurantID INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant VARCHAR(100) NOT NULL
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS SERVER (
    ServerEmpID INT PRIMARY KEY,
    ServerName VARCHAR(255) NOT NULL,
    ServerBirthDate DATE NOT NULL DEFAULT '1900-01-01',
    ServerTIN VARCHAR(50) NOT NULL UNIQUE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS SERVER_RESTAURANT (
    ServerEmpID INT NOT NULL,
    RestaurantID INT NOT NULL,
    StartDateHired DATE NOT NULL DEFAULT '1900-01-01',
    EndDateHired DATE DEFAULT NULL,
    HourlyRate DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (ServerEmpID, RestaurantID),
    FOREIGN KEY (ServerEmpID) REFERENCES SERVER(ServerEmpID) ON DELETE CASCADE,
    FOREIGN KEY (RestaurantID) REFERENCES RESTAURANT(RestaurantID) ON DELETE CASCADE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS CUSTOMER (
    CustomerID INT AUTO_INCREMENT PRIMARY KEY,
    CustomerName VARCHAR(255) NOT NULL,
    CustomerPhone VARCHAR(20),
    CustomerEmail VARCHAR(255),
    LoyaltyMember BOOLEAN DEFAULT FALSE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS MEALTYPE (
    MealTypeID INT AUTO_INCREMENT PRIMARY KEY,
    MealTypeName VARCHAR(100) NOT NULL
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS VISIT (
    VisitID INT AUTO_INCREMENT PRIMARY KEY,
    RestaurantID INT,
    ServerEmpID INT,
    CustomerID INT,
    MealTypeID INT,
    VisitDate DATE DEFAULT '1900-01-01',
    VisitTime TIME DEFAULT '00:00:00',
    PartySize INT DEFAULT 1,
    WaitTime INT DEFAULT 0,
    FOREIGN KEY (RestaurantID) REFERENCES RESTAURANT(RestaurantID) ON DELETE CASCADE,
    FOREIGN KEY (ServerEmpID) REFERENCES SERVER(ServerEmpID) ON DELETE CASCADE,
    FOREIGN KEY (CustomerID) REFERENCES CUSTOMER(CustomerID) ON DELETE CASCADE,
    FOREIGN KEY (MealTypeID) REFERENCES MEALTYPE(MealTypeID) ON DELETE CASCADE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS PAYMENTMETHOD_LOOKUP (
    PaymentMethodID INT AUTO_INCREMENT PRIMARY KEY,
    PaymentMethod VARCHAR(255) NOT NULL UNIQUE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS BILL (
    BillID INT AUTO_INCREMENT PRIMARY KEY,
    VisitID INT NOT NULL,
    PaymentMethodID INT NOT NULL,
    FoodBill DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    TipAmount DECIMAL(10,2) DEFAULT 0.00,
    DiscountApplied DECIMAL(5,2) DEFAULT 0.00,
    OrderedAlcohol BOOLEAN DEFAULT FALSE,
    AlcoholBill DECIMAL(10,5) DEFAULT 0.00000,
    TotalAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    FOREIGN KEY (VisitID) REFERENCES VISIT(VisitID) ON DELETE CASCADE,
    FOREIGN KEY (PaymentMethodID) REFERENCES PAYMENTMETHOD_LOOKUP(PaymentMethodID) ON DELETE CASCADE
  );
")

execute_query("
  CREATE TABLE IF NOT EXISTS GENDERCOUNT (
    GenderCountID INT AUTO_INCREMENT PRIMARY KEY,
    VisitID INT NOT NULL,
    MaleCount INT DEFAULT 0,
    FemaleCount INT DEFAULT 0,
    UnspecifiedCount INT DEFAULT 0,
    FOREIGN KEY (VisitID) REFERENCES VISIT(VisitID) ON DELETE CASCADE
  );
")

print("All tables created successfully")
dbDisconnect(con)

