# Load necessary libraries
library(readr)
library(RSQLite)


# Create a database connection
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/group32.db")

## Products Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_products' (
  'product_id' VARCHAR(255) PRIMARY KEY,
    'seller_id' VARCHAR(10) NOT NULL,
  'category_id' VARCHAR(10) NOT NULL,
  'product_name' TEXT NOT NULL,
  'in_stock' BIT NOT NULL DEFAULT 0,
  'available_units' INT NOT NULL DEFAULT 0,
  'price' MONEY NOT NULL CHECK (price > 0),
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

## Create buyer Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_buyer' (
  'buyer_id' VARCHAR PRIMARY KEY,
  'first_name' TEXT NOT NULL,
  'last_name' TEXT,
  'email' VARCHAR NOT NULL UNIQUE,
  'password' VARCHAR NOT NULL,
  'user_type' TEXT,
  'expiry_date' DATE
);
")


## Create contact details Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_contact_details' (
  'address_id' VARCHAR PRIMARY KEY,
   'buyer_id' VARCHAR,
  'address' VARCHAR,
  'country' TEXT,
  'state' TEXT,
  'city' TEXT,
  'street' VARCHAR,
  'phone_number' VARCHAR(10),
  'address_type' TEXT,
  FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id')
);
")

## Create Review Table
dbExecute(connection, "
  CREATE TABLE IF NOT EXISTS 'project_review' (
  'review_id' VARCHAR PRIMARY KEY,
    'product_id' VARCHAR,
  'buyer_id' VARCHAR,
  'rating' INT CHECK (rating >= 1 AND rating <= 5),
  'review' TEXT,
  'review_date' DATE,
   FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id'),
   FOREIGN KEY ('product_id') REFERENCES products ('product_id')
);
")



#Create relationship table buyer_orders_products
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_buyer_orders_products' (
  'buyer_id' VARCHAR(255),
  'product_id' VARCHAR(255),
  'quantity_ordered' INT NOT NULL DEFAULT 1,
  'order_date' DATE,
  'delivery_date' DATE,
  'order_status' TEXT,
  'payment_type' TEXT,
  'address_type' TEXT,
  FOREIGN KEY ('product_id') REFERENCES products ('product_id'),
  FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id')
);
")

#Create relationship table references
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_references' (
  'buyer_id' VARCHAR(255),
  'referred_by' VARCHAR(255),
   FOREIGN KEY ('buyer_id') REFERENCES buyer ('buyer_id')
);
")


# Verify the table was created by listing all tables in the database
RSQLite::dbListTables(connection)

#load the dataset into data frames
# List all CSV files in the data_upload directory
csv_files <- list.files(path = "data_upload", pattern = "\\.csv$", full.names = TRUE)

# Create a list to store all data frames
all_data_frames <- list()

# Loop through each CSV file
for (csv_file in csv_files) {
  # Extract filename without extension
  filename <- tools::file_path_sans_ext(basename(csv_file))
  
  # Read the CSV file into a data frame
  data_frame <- read.csv(csv_file)
  
  # Assign the data frame to a new variable with a name starting with "project_"
  assign(paste0("project_", filename), data_frame)
  
  # Add the data frame to the list
  all_data_frames[[paste0("project_", filename)]] <- data_frame
}

# Check the names of the created data frames
print(names(all_data_frames))

# Data Validation
# Function to validate phone numbers
validate_phone_number <- function(phone_number) {
  # Check if the length is 10 and starts with 7
  if (nchar(phone_number) != 10 || substr(phone_number, 1, 1) != "7") {
    return(FALSE)
  }
  # Check if all characters are numeric
  if (!grepl("^[0-9]+$", phone_number)) {
    return(FALSE)
  }
  return(TRUE)
}

# Function to validate first and last names
validate_name <- function(name) {
  # Check if the length is less than 25 characters
  if (nchar(name) > 25) {
    return(FALSE)
  }
  # Check if only alphabets are present
  if (!grepl("^[A-Za-z]+$", name)) {
    return(FALSE)
  }
  return(TRUE)
}

# Function to validate address
validate_address <- function(address) {
  # Check if the length is 6 characters
  if (nchar(address) != 6) {
    return(FALSE)
  }
  # Check if address consists of only alphanumeric characters
  if (!grepl("^[A-Za-z0-9]+$", address)) {
    return(FALSE)
  }
  return(TRUE)
}


# Load necessary libraries
library(readr)

# Read the contact dataframe
contact_df <- read_csv("data_upload/contact_df.csv")

# Validate phone numbers
valid_phone <- sapply(contact_df$phone_number, validate_phone_number)

# Validate addresses
valid_address <- sapply(contact_df$address, validate_address)

# Check for invalid phone numbers and addresses
invalid_phone_numbers <- contact_df$phone_number[!valid_phone]
invalid_addresses <- contact_df$address[!valid_address]

# If there are any invalid entries, remove them
if (length(invalid_phone_numbers) > 0) {
  cat("Invalid phone numbers:", invalid_phone_numbers, "\n")
  contact_df <- contact_df[valid_phone, ]
}

if (length(invalid_addresses) > 0) {
  cat("Invalid addresses:", invalid_addresses, "\n")
  contact_df <- contact_df[valid_address, ]
}

# Remove duplicate rows based on email IDs in buyer_df
buyer_df <- distinct(buyer_df, email, .keep_all = TRUE)

# Remove duplicate rows based on email IDs in sellers_df
sellers_df <- distinct(sellers_df, email, .keep_all = TRUE)

# Load necessary libraries
library(dplyr)

# Read CSV files into data frames
products_df <- read.csv("data_upload/products_df.csv")
sellers_df <- read.csv("data_upload/sellers_df.csv")
categories_df <- read.csv("data_upload/categories_df.csv")
buyer_df <- read.csv("data_upload/buyer_df.csv")
contact_df <- read.csv("data_upload/contact_df.csv")
review_df <- read.csv("data_upload/review_df.csv")
references_df <- read.csv("data_upload/references_df.csv")
buyer_orders_products <- read.csv("data_upload/buyer_orders_products.csv")

# Ensure referential integrity between tables

# Ensure category_id in products_df exists in categories_df
if (any(!products_df$category_id %in% categories_df$category_id)) {
  stop("Foreign key violation: category_id in products_df does not exist in categories_df")
}

# Ensure buyer_id in contact_df exists in buyer_df
if (any(!contact_df$buyer_id %in% buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id in contact_df does not exist in buyer_df")
}

# Ensure buyer_id in review_df exists in buyer_df
if (any(!review_df$buyer_id %in% buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id in review_df does not exist in buyer_df")
}

# Ensure product_id in review_df exists in products_df
if (any(!review_df$product_id %in% products_df$product_id)) {
  stop("Foreign key violation: product_id in review_df does not exist in products_df")
}

# Ensure buyer_id and product_id in buyer_orders_products exist in buyer_df and products_df respectively
if (any(!buyer_orders_products$buyer_id %in% buyer_df$buyer_id) ||
    any(!buyer_orders_products$product_id %in% products_df$product_id)) {
  stop("Foreign key violation: buyer_id or product_id in buyer_orders_products do not exist in buyer_df or products_df")
}

# Ensure buyer_id and referred_by in references_df exist in buyer_df
if (any(!references_df$buyer_id %in% buyer_df$buyer_id) ||
    any(!references_df$referred_by %in% buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id or referred_by in references_df do not exist in buyer_df")
}

# IF a new file is added to the data_upload folder it will create a table for it.
# Function to extract table name from file name
get_table_name <- function(file_name) {
  # Remove file extension
  table_name <- tools::file_path_sans_ext(file_name)
  # Prepend "project_" to table name
  table_name <- paste0("project_", table_name)
  return(table_name)
}

# Check for new CSV files in data_upload folder
csv_files <- list.files(path = "data_upload", pattern = "\\.csv$", full.names = TRUE)

# Create a list to store data frames
new_data_frames <- list()

# Loop through each CSV file
for (csv_file in csv_files) {
  # Extract file name
  file_name <- basename(csv_file)
  # Check if a table already exists for this CSV file
  table_name <- get_table_name(file_name)
  if (!(table_name %in% dbListTables(connection))) {
    # If table doesn't exist, create a new data frame
    new_data_frame <- read.csv(csv_file)
    # Add the data frame to the list with the file name as key
    new_data_frames[[file_name]] <- new_data_frame
    # Create a new table in the database
    dbWriteTable(connection, table_name, new_data_frame, row.names = FALSE)
    cat("Table", table_name, "created and data inserted.\n")
  } else {
    cat("Table", table_name, "already exists.\n")
  }
}

# Function to extract table name from file name
get_table_name <- function(file_name) {
  # Remove file extension
  table_name <- tools::file_path_sans_ext(file_name)
  # Prepend "project_" to table name
  table_name <- paste0("project_", table_name)
  return(table_name)
}

# Check for new CSV files in data_upload folder
csv_files <- list.files(path = "data_upload", pattern = "\\.csv$", full.names = TRUE)

# Create a list to store data frames
new_data_frames <- list()

# Loop through each CSV file
for (csv_file in csv_files) {
  # Extract file name
  file_name <- basename(csv_file)
  # Check if a table already exists for this CSV file
  table_name <- get_table_name(file_name)
  if (!(table_name %in% dbListTables(connection))) {
    # If table doesn't exist, create a new data frame
    new_data_frame <- read.csv(csv_file)
    # Add the data frame to the list with the file name as key
    new_data_frames[[file_name]] <- new_data_frame
    # Create a new table in the database
    dbWriteTable(connection, table_name, new_data_frame, row.names = FALSE)
    cat("Table", table_name, "created and data inserted.\n")
  } else {
    cat("Table", table_name, "already exists.\n")
  }
}

# Check for new data if available in the datasets.
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
insert_data("project_products", read_csv("data_upload/products_df.csv"))

## Seller Table
insert_data("project_seller", read_csv("data_upload/sellers_df.csv"))

## Categories Table
insert_data("project_categories", read_csv("data_upload/categories_df.csv"))

## Buyer Table
insert_data("project_buyer", read_csv("data_upload/buyer_df.csv"))

## Contact Details Table
insert_data("project_contact_details", read_csv("data_upload/contact_df.csv"))

## Review Table
insert_data("project_review", read_csv("data_upload/review_df.csv"))

## Buyer Orders Products Table
insert_data("project_buyer_orders_products", read_csv("data_upload/buyer_orders_products.csv"))


## Self referencing table for buyer
insert_data("project_references", read_csv("data_upload/references_df.csv"))


# Verify the table was created by listing all tables in the database
#RSQLite::dbListTables(connection)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "project_products")

buyer_table <- dbReadTable(connection, "project_buyer")

# Data Analysis

#Average daily sales
#calculating the average revenue we are earning every day


average_sales<- RSQLite::dbGetQuery(connection,"
          SELECT SUM(a.price) / COUNT(DISTINCT(b.order_date)) AS average_sales
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )
#Total sales 
#Calculating the revenue we have

total_sales<-RSQLite::dbGetQuery(connection, "
          SELECT SUM(a.price) AS total_sales
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )


# best performing products by revenue

top_products <- RSQLite::dbGetQuery(connection, 
                                    "SELECT SUM(a.price) AS revenue, b.product_id, a.product_name
                                           FROM project_buyer_orders_products b
                                           INNER JOIN project_products a ON b.product_id = a.product_id
                                           GROUP BY a.product_id
                                           ORDER BY revenue DESC
                                           " )


#revenue by categories
#We calculate the total revenue of each category

revenue_by_categories <- RSQLite::dbGetQuery(connection,
                                             "SELECT c.category_name, SUM(b.price) AS revenue
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_categories c ON b.category_id = c.category_id
                                      GROUP BY c.category_name
                                      ORDER BY revenue DESC
                                      " )

#Top sellers
#We check which sellers are making the most revenue

top_sellers <- RSQLite::dbGetQuery(connection, 
                                   "SELECT SUM(b.price) AS revenue, c.seller_name
                                           FROM project_buyer_orders_products a
                                           INNER JOIN project_products b ON a.product_id = b.product_id
                                           INNER JOIN project_seller c ON b.seller_id = c.seller_id
                                           GROUP BY c.seller_name
                                           ORDER BY revenue DESC
                                            " )

sales_by_state <- RSQLite::dbGetQuery(connection, 
                                      "SELECT SUM(b.price) AS revenue, c.state
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.state
                                      ORDER BY revenue DESC" )


sales_by_city <- RSQLite::dbGetQuery(connection, 
                                     "SELECT SUM(b.price) AS revenue, c.city
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.city
                                      ORDER BY revenue DESC" )

revenue_by_city <- ggplot(sales_by_city, aes(x = city, y = 1, size = revenue)) +
  geom_point(shape = 21, fill = "blue") +
  scale_size_continuous(range = c(3, 10)) +
  labs(title = "Revenue by City", x = "City", y = "") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

#adding it to "figures folder"
# Save the plot as an image to another directory
ggsave(filename = "../figures/your_plot_name.png", plot = revenue_by_city)

Sales_by_user_type <- RSQLite::dbGetQuery(connection, 
                                          "SELECT SUM(b.price) AS revenue, c.user_type
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_buyer c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.user_type
                                      ORDER BY revenue DESC" )


  
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
