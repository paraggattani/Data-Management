library(readr)
library(RSQLite)
#install.packages("readxl")
library(readxl)

# Create a database
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/group32.db")
RSQLite::dbListTables(connection)

# Create a new table with specified columns

## Products Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'products' (
  'product_id' VARCHAR(255) PRIMARY KEY,
  'product_name' TEXT NOT NULL,
  'in_stock' BIT NOT NULL DEFAULT 0,
  'available_units' INT NOT NULL DEFAULT 0,
  'price' MONEY NOT NULL CHECK (price > 0),
  'seller_id' VARCHAR(10) NOT NULL,
  'category_id' VARCHAR(10) NOT NULL,
  FOREIGN KEY ('seller_id') REFERENCES seller('seller_id'),
  FOREIGN KEY ('category_id') REFERENCES category('category_id')
);
")


# loading data from seller excel sheet to seller table
products_data <- read_csv("data_upload/products_df.csv")

# Use dbWriteTable to insert the data into the 'products' table
dbWriteTable(connection, "products", products_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "products")
print(products_table)
#dbExecute(connection, "DROP TABLE IF EXISTS products")

# Optionally, verify all tables have been dropped
dbListTables(connection)

## Create Seller Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'seller' (
'seller_id' VARCHAR(10) PRIMARY KEY,
'seller_name' TEXT NOT NULL,
'url' VARCHAR(255),
'description' TEXT,
'email' VARCHAR(255) UNIQUE,
'password' VARCHAR(255),
'account_number' VARCHAR(255),
'bank_name' TEXT
);
")

# loading data from seller excel sheet to seller table
seller_data <- read_csv("data_upload/sellers_df.csv")

# Use dbWriteTable to insert the data into the 'seller' table
dbWriteTable(connection, "seller", seller_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
seller_table <- dbReadTable(connection, "seller")
print(seller_table)
#dbExecute(connection, "DROP TABLE IF EXISTS seller")

## Create category Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'categories' (
'category_id' VARCHAR(255) PRIMARY KEY,
'category_name' TEXT NOT NULL 
);
")

# loading data from seller excel sheet to seller table
categories_data <- read_csv("data_upload/categories_df.csv")

# Use dbWriteTable to insert the data into the 'categories' table
dbWriteTable(connection, "categories", categories_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
categories_table <- dbReadTable(connection, "categories")
print(categories_table)
#dbExecute(connection, "DROP TABLE IF EXISTS category")

## Create buyer Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'buyer' (
  'buyer_id' VARCHAR PRIMARY KEY,
  'first_name' TEXT NOT NULL,
  'last_name' TEXT,
  'email' VARCHAR NOT NULL UNIQUE,
  'password' VARCHAR NOT NULL,
  'user_type' TEXT,
  'expiry_date' DATE
  'referred_by' 
);
")
# loading data from seller excel sheet to seller table
#dbWriteTable(connection, "seller", sellers_data, append = TRUE, row.names = FALSE)
#seller_table <- dbReadTable(connection, "seller")
#print(seller_table)
#dbExecute(connection, "DROP TABLE IF EXISTS seller")


## Create contact details Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'contact_details' (
  'address_id' VARCHAR PRIMARY KEY,
  'address' VARCHAR,
  'country' TEXT,
  'state' TEXT,
  'city' TEXT,
  'street' VARCHAR,
  'phone_number' VARCHAR(10),
  'buyer_id' VARCHAR,
  FOREIGN KEY ('buyer_id')
    REFERENCES buyer ('buyer_id')
);
")

# loading data from seller excel sheet to seller table
contact_data <- read_csv("data_upload/contact_df.csv")

# Use dbWriteTable to insert the data into the 'contact_details' table
dbWriteTable(connection, "contact_details", contact_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
contact_details_table <- dbReadTable(connection, "contact_details")
print(contact_details_table)
#dbExecute(connection, "DROP TABLE IF EXISTS contact_details")

## Create Review Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'review' (
  'review_id' VARCHAR PRIMARY KEY,
  'rating' INT CHECK (rating >= 1 AND rating <= 5),
  'review' TEXT,
  'review_date' DATE,
  'product_id' VARCHAR,
  'buyer_id' VARCHAR,
  FOREIGN KEY ('buyer_id')
    REFERENCES buyer ('buyer_id')
  FOREIGN KEY ('product_id')
    REFERENCES products ('product_id')
);
")
# loading data from seller excel sheet to seller table
review_data <- read_csv("data_upload/review_df.csv")

# Use dbWriteTable to insert the data into the 'review' table
dbWriteTable(connection, "review", review_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
review_table <- dbReadTable(connection, "review")
print(review_table)

#dbExecute(connection, "DROP TABLE IF EXISTS review")

## Create carrier Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'carrier' (
  'carrier_id' VARCHAR PRIMARY KEY,
  'carrier_name' TEXT NOT NULL,
  'carrier_phone' VARCHAR(10),
  'carrier_email' TEXT UNIQUE
);
")
# loading data from seller excel sheet to seller table
carrier_data <- read_csv("data_upload/carrier_df.csv")

# Use dbWriteTable to insert the data into the 'carrier' table
dbWriteTable(connection, "carrier", carrier_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
carrier_table <- dbReadTable(connection, "carrier")
print(carrier_table)
#dbExecute(connection, "DROP TABLE IF EXISTS seller")

## Create refund Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'refund' (
  'product_id' VARCHAR(255),
  'refund_ids' VARCHAR PRIMARY KEY,
  'refund_price' MONEY, 
  'refund_status' TEXT,
  'refund_reason' TEXT,
  FOREIGN KEY ('product_id')
    REFERENCES products ('product_id')
);
")
# loading data from seller excel sheet to seller table
refund_data <- read_csv("data_upload/refund_df.csv")

# Use dbWriteTable to insert the data into the 'refund' table
dbWriteTable(connection, "refund", refund_data, append = TRUE, row.names = FALSE)

# Optionally, verify the data was inserted
refund_table <- dbReadTable(connection, "refund")
print(refund_table)
#dbExecute(connection, "DROP TABLE IF EXISTS refund")

#Create relationship table buyer_orders_products
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'buyer_orders_products' (
  'product_id' VARCHAR(255),
  'product_name' TEXT NOT NULL,
  'price' MONEY NOT NULL,
  'quantity_ordered' INT NOT NULL,
  'discount' DECIMAL(3,2),
  'in_stock' BIT NOT NULL,
  'remaining_available_units' INT NOT NULL,
  'category_id' VARCHAR(10),
  'order_date' DATE,
  'delivery_date' DATE,
  'shipping' MONEY,
  'order_status' TEXT,
  'payment_type' TEXT,
  'buyer_id' VARCHAR(255),
  'first_name' TEXT NOT NULL,
  'last_name' TEXT NOT NULL,
  'email' VARCHAR(255),
  'password' VARCHAR(255),
  'user_type' TEXT,
  'expiry_date' DATE,
  'carrier_id' VARCHAR(10),
  'address_id' VARCHAR(255),
  'address' TEXT,
  'country' TEXT,
  'state' TEXT,
  'city' TEXT,
  'street' TEXT,
  'phone_number' VARCHAR(10),
  FOREIGN KEY ('product_id') REFERENCES products ('product_id'),
  FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id'),
  FOREIGN KEY ('category_id') REFERENCES categories ('category_id'),
  FOREIGN KEY ('carrier_id') REFERENCES carrier ('carrier_id'),
  FOREIGN KEY ('address_id') REFERENCES contact_details ('address_id')
);
")

# Load data from CSV file into R. The file should be named 'buyer_orders_products.csv'.
buyer_orders_products_data <- read_csv("data_upload/buyer_orders_products.csv")

# Use dbWriteTable to insert the data into the 'buyer_orders_products' table
dbWriteTable(connection, "buyer_orders_products", buyer_orders_products_data, append = TRUE, row.names = FALSE)

# Verify the data was inserted
buyer_orders_products_table <- dbReadTable(connection, "buyer_orders_products")
print(buyer_orders_products_table)
#dbExecute(connection, "DROP TABLE IF EXISTS buyer_orders_products")

# Verify the table was created by listing all tables in the database
RSQLite::dbListTables(connection)

