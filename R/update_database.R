# Load necessary libraries
library(readr)
library(RSQLite)

#shalvi


# Create a database connection
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/group32.db")

## Products Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_products' (
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


## Create Seller Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_seller' (
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


## Create category Table
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_categories' (
'category_id' VARCHAR(255) PRIMARY KEY,
'category_name' TEXT NOT NULL 
);
")


dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_buyer' (
  'buyer_id' VARCHAR PRIMARY KEY,
  'first_name' TEXT NOT NULL,
  'last_name' TEXT,
  'email' VARCHAR NOT NULL UNIQUE,
  'password' VARCHAR NOT NULL,
  'user_type' TEXT,
  'expiry_date' DATE,
  'referred_by' VARCHAR,
  FOREIGN KEY ('referred_by') REFERENCES buyer ('buyer_id')
);
")


## Create contact details Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_contact_details' (
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

## Create Review Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_review' (
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


## Create carrier Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_carrier' (
  'carrier_id' VARCHAR PRIMARY KEY,
  'carrier_name' TEXT NOT NULL,
  'carrier_phone' VARCHAR(10),
  'carrier_email' TEXT UNIQUE
);
")


## Create refund Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_refund' (
  'product_id' VARCHAR(255),
  'refund_ids' VARCHAR PRIMARY KEY,
  'refund_price' MONEY, 
  'refund_status' TEXT,
  'refund_reason' TEXT,
  FOREIGN KEY ('product_id')
    REFERENCES products ('product_id')
);
")


#Create relationship table buyer_orders_products
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_buyer_orders_products' (
  'product_id' VARCHAR(255),
  'category_id' VARCHAR(10),
  'order_date' DATE,
  'delivery_date' DATE,
  'shipping' MONEY,
  'order_status' TEXT,
  'payment_type' TEXT,
  'buyer_id' VARCHAR(255),
  'carrier_id' VARCHAR(10),
  FOREIGN KEY ('product_id') REFERENCES products ('product_id'),
  FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id'),
  FOREIGN KEY ('carrier_id') REFERENCES carrier ('carrier_id')
);
")


# Verify the table was created by listing all tables in the database
RSQLite::dbListTables(connection)


# loading data from seller excel sheet to seller table
products_data <- read_csv("data_upload/products_df.csv")

# loading data from seller excel sheet to seller table
seller_data <- read_csv("data_upload/sellers_df.csv")

# loading data from seller excel sheet to seller table
categories_data <- read_csv("data_upload/categories_df.csv")

# loading data from buyer excel sheet to buyer table
buyer_data <- read_csv("data_upload/buyer_df.csv")

contact_data <- read_csv("data_upload/contact_df.csv")

review_data <- read_csv("data_upload/review_df.csv")

carrier_data <- read_csv("data_upload/carrier_df.csv")

refund_data <- read_csv("data_upload/refund_df.csv")

buyer_orders_products_data <- read_csv("data_upload/buyer_orders_products.csv")


# Function to check if data exists in a table
data_exists <- function(table_name) {
  query <- paste0("SELECT COUNT(*) FROM ", table_name)
  result <- dbGetQuery(connection, query)
  return(result[[1]] > 0)
}

# Function to insert data into a table if it doesn't exist
insert_data <- function(table_name, data) {
  if (!data_exists(table_name)) {
    dbWriteTable(connection, table_name, data, append = TRUE, row.names = FALSE)
    cat("Data inserted into", table_name, "\n")
  } else {
    cat("Data already exists in", table_name, "\n")
  }
}

## Products Table
insert_data("products", read_csv("data_upload/products_df.csv"))

## Seller Table
insert_data("seller", read_csv("data_upload/sellers_df.csv"))

## Categories Table
insert_data("categories", read_csv("data_upload/categories_df.csv"))

## Buyer Table
insert_data("buyer", read_csv("data_upload/buyer_df.csv"))

## Contact Details Table
insert_data("contact_details", read_csv("data_upload/contact_df.csv"))

## Review Table
insert_data("review", read_csv("data_upload/review_df.csv"))

## Carrier Table
insert_data("carrier", read_csv("data_upload/carrier_df.csv"))

## Refund Table
insert_data("refund", read_csv("data_upload/refund_df.csv"))

## Buyer Orders Products Table
insert_data("buyer_orders_products", read_csv("data_upload/buyer_orders_products.csv"))

# Verify the table was created by listing all tables in the database
#RSQLite::dbListTables(connection)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "products")
print(products_table)

buyer_table <- dbReadTable(connection, "buyer")
print(buyer_table)


library(dplyr)

# Count the occurrences of each product_id
top_products <- buyer_orders_products_data %>%
  count(product_id, sort = TRUE) %>%
  top_n(10)

# Display the top 10 products sold
print(top_products)

library(ggplot2)

# Plotting the top 10 products sold
plot <- ggplot(top_products, aes(x = reorder(product_id, n), y = n)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Top 10 Products Sold",
       x = "Product ID",
       y = "Count") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Rotate x-axis labels for better readability

# Define the directory to save the figure
figure_directory <- "figures/"

# Create the directory if it doesn't exist
if (!dir.exists(figure_directory)) {
  dir.create(figure_directory)
}
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))
ggsave(paste0("figures/regression_plot_",
              this_filename_date,"_",
              this_filename_time,".png"))

#changes