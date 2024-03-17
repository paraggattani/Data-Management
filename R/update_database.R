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
insert_data("project_buyer_orders_products", read_csv("data_upload/buyer_orders_products.csv"))

# Verify the table was created by listing all tables in the database
#RSQLite::dbListTables(connection)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "products")
print(products_table)

buyer_table <- dbReadTable(connection, "buyer")
print(buyer_table)

<<<<<<< HEAD
#basic data analysis part

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

=======

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
>>>>>>> 44ac6648e4b2d2ef21c5afd03b058ecaf089d45c
