# Load necessary libraries
library(readr)
library(RSQLite)
library(dplyr)
library(tidyverse)
library(sf)
library(rnaturalearth)
library(ggplot2)
library(rnaturalearthdata)

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



#Create relationship table project_buyer_orders_products
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

# Validate phone numbers
valid_phone <- sapply(project_contact_df$phone_number, validate_phone_number)

# Validate addresses
valid_address <- sapply(project_contact_df$address, validate_address)

# Check for invalid phone numbers and addresses
invalid_phone_numbers <- project_contact_df$phone_number[!valid_phone]
invalid_addresses <- project_contact_df$address[!valid_address]

# If there are any invalid entries, remove them
if (length(invalid_phone_numbers) > 0) {
  cat("Invalid phone numbers:", invalid_phone_numbers, "\n")
  project_contact_df <- project_contact_df[valid_phone, ]
}

if (length(invalid_addresses) > 0) {
  cat("Invalid addresses:", invalid_addresses, "\n")
  project_contact_df <- project_contact_df[valid_address, ]
}

# Remove duplicate rows based on email IDs in project_buyer_df
project_buyer_df <- distinct(project_buyer_df, email, .keep_all = TRUE)

# Remove duplicate rows based on email IDs in project_sellers_df
project_sellers_df <- distinct(project_sellers_df, email, .keep_all = TRUE)



# Ensure referential integrity between tables

# Ensure category_id in project_products_df exists in project_categories_df
if (any(!project_products_df$category_id %in% project_categories_df$category_id)) {
  stop("Foreign key violation: category_id in project_products_df does not exist in project_categories_df")
}

# Ensure buyer_id in project_contact_df exists in project_buyer_df
if (any(!project_contact_df$buyer_id %in% project_buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id in project_contact_df does not exist in project_buyer_df")
}

# Ensure buyer_id in project_review_df exists in project_buyer_df
if (any(!project_review_df$buyer_id %in% project_buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id in project_review_df does not exist in project_buyer_df")
}

# Ensure product_id in project_review_df exists in project_products_df
if (any(!project_review_df$product_id %in% project_products_df$product_id)) {
  stop("Foreign key violation: product_id in project_review_df does not exist in project_products_df")
}

# Ensure buyer_id and product_id in project_buyer_orders_products exist in project_buyer_df and project_products_df respectively
if (any(!project_buyer_orders_products$buyer_id %in% project_buyer_df$buyer_id) ||
    any(!project_buyer_orders_products$product_id %in% project_products_df$product_id)) {
  stop("Foreign key violation: buyer_id or product_id in project_buyer_orders_products do not exist in project_buyer_df or project_products_df")
}

# Ensure buyer_id and referred_by in project_references_df exist in project_buyer_df
if (any(!project_references_df$buyer_id %in% project_buyer_df$buyer_id) ||
    any(!project_references_df$referred_by %in% project_buyer_df$buyer_id)) {
  stop("Foreign key violation: buyer_id or referred_by in project_references_df do not exist in project_buyer_df")
}

# Check for new data if available in the datasets.
# Function to check if data exists in a table
data_exists <- function(connection, table_name) {
  query <- paste0("SELECT COUNT(*) FROM ", table_name)
  result <- dbGetQuery(connection, query)
  return(result[[1]] > 0)
}

# Function to insert data into a table from a data frame
insert_data_from_df <- function(connection, table_name, data_frame) {
  # Check if data already exists in the table
  if (data_exists(connection, table_name)) {
    cat("Data already exists in", table_name, "\n")
    return()
  }
  
  # If data doesn't exist, insert it into the table
  dbWriteTable(connection, table_name, data_frame, row.names = FALSE, append = TRUE)
  cat("Data inserted into", table_name, "\n")
}

# Insert data from data frames into respective tables
insert_data_from_df(connection, "project_products", project_products_df)
insert_data_from_df(connection, "project_seller", project_sellers_df)
insert_data_from_df(connection, "project_categories", project_categories_df)
insert_data_from_df(connection, "project_buyer", project_buyer_df)
insert_data_from_df(connection, "project_contact_details", project_contact_df)
insert_data_from_df(connection, "project_review", project_review_df)
insert_data_from_df(connection, "project_buyer_orders_products", project_buyer_orders_products)
insert_data_from_df(connection, "project_references", project_references_df)


# Verify the table was created by listing all tables in the database
#RSQLite::dbListTables(connection)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "project_products")
buyer_table <- dbReadTable(connection, "project_buyer")

# Data Analysis

#1
#Average daily sales
#calculating the average revenue we are earning every day


sales<- RSQLite::dbGetQuery(connection,"
          SELECT a.price, datetime(b.order_date, 'unixepoch') AS date_time
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )


sales_by_date <- sales %>%
  separate(date_time, into = c("date", "time"), sep = " ")

average_revenue <-sales_by_date %>%
  group_by(date) %>%
  summarise(total_revenue = sum(price)) %>%
  summarise(average_revenue = sum(total_revenue) / n_distinct(date))

#2
#Total sales 
#Calculating the revenue we have

total_sales<-RSQLite::dbGetQuery(connection, "
          SELECT SUM(a.price) AS total_sales
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )

#3
# best performing products by revenue

products <- RSQLite::dbGetQuery(connection, 
                                "SELECT SUM(a.price) AS revenue, b.product_id, a.product_name
                                           FROM project_buyer_orders_products b
                                           INNER JOIN project_products a ON b.product_id = a.product_id
                                           GROUP BY a.product_id
                                           ORDER BY revenue DESC
                                           " )


top_10_products <- RSQLite::dbGetQuery(connection, 
                                       "SELECT SUM(a.price) AS revenue, b.product_id, a.product_name
                                           FROM project_buyer_orders_products b
                                           INNER JOIN project_products a ON b.product_id = a.product_id
                                           GROUP BY a.product_id
                                           ORDER BY revenue DESC
                                           LIMIT 10" )

top_10 <- ggplot(top_10_products, aes(x = reorder(product_name, -revenue), y = revenue, fill = product_name)) +
  geom_bar(stat = "identity",show.legend = FALSE) +
  labs(title = "Top 10 Products by Revenue", x = "Product Name", y = "Revenue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

top_10

#4
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

#Finding the revenue for each category over time 
revenue_by_categories_per_date <- RSQLite::dbGetQuery(connection,
                                                      "SELECT c.category_name, b.price, datetime(a.order_date, 'unixepoch') AS date_time
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_categories c ON b.category_id = c.category_id
                                      
                                      " )
revenue_by_categories_per_date<- revenue_by_categories_per_date %>%
  separate(date_time, into = c("date", "time"), sep = " ")


revenue_by_categories_per_date <- revenue_by_categories_per_date  %>%
  group_by(category_name, date) %>%
  summarise(sum_price = sum(price)) %>%
  ungroup()
#Revenue by category over time
revenue_by_categories_plot <- ggplot(revenue_by_categories_per_date, aes(x = date, y = sum_price, group = category_name, color = category_name)) +
  geom_line() +
  facet_wrap(~ category_name, scales = "free_y") +
  labs(title = "Revenue by Category Over Time", x = "Date", y = "Revenue") +
  theme_minimal() +
  theme(
    axis.text.x = element_blank(),
    legend.position = "none")

revenue_by_categories_plot

#ggplot for revenue by category bar_plot
revenue_by_categories <- revenue_by_categories[order(-revenue_by_categories$revenue),]

revenue_by_categories_bar <- ggplot(revenue_by_categories, aes(reorder(category_name, -revenue), y = revenue, fill = category_name)) +
  geom_bar(stat = "identity") +
  labs(title = "Revenue by Category", fill = "Category") +
  theme_minimal() +
  theme(legend.position = "none") 

revenue_by_categories_bar

#5
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

top_10_sellers <- RSQLite::dbGetQuery(connection, 
                                      "SELECT SUM(b.price) AS revenue, c.seller_name
                                           FROM project_buyer_orders_products a
                                           INNER JOIN project_products b ON a.product_id = b.product_id
                                           INNER JOIN project_seller c ON b.seller_id = c.seller_id
                                           GROUP BY c.seller_name
                                           ORDER BY revenue DESC
                                           LIMIT 10" )
top_10_sellers_plot <- ggplot(top_10_sellers, aes(x = reorder(seller_name, -revenue), y = revenue, fill = seller_name)) +
  geom_bar(stat = "identity",show.legend = FALSE) +
  labs(title = "Top 10 Sellers by Revenue", x = "Seller Name", y = "Revenue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

top_10_sellers_plot

#6
#calculating sales by state
sales_by_state <- RSQLite::dbGetQuery(connection, 
                                      "SELECT SUM(b.price) AS revenue, c.state
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.state
                                      ORDER BY revenue DESC" )
#7
#sales_by_city
sales_by_city <- RSQLite::dbGetQuery(connection, 
                                     "SELECT SUM(b.price) AS revenue, c.city
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.city
                                      ORDER BY revenue DESC" )


sales_by_top10_city <- RSQLite::dbGetQuery(connection, 
                                           "SELECT SUM(b.price) AS revenue, c.city, c.state
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.city
                                      ORDER BY revenue DESC
                                      LIMIT 10" )


cities_uk <- data.frame(
  city = c("Edinburgh", "Bangor", "Derby", "St Davids", "Ripon", "Doncaster", "Canterbury", "Coventry", "Lisburn", "St Asaph"),
  lon = c(-3.1883, -4.1267, -1.4721, -5.2717, -1.5211, -1.1307, 1.0756, -1.5118, -6.0332, -3.4436),
  lat = c(55.9533, 53.2268, 52.9228, 51.8815, 54.1361, 53.5228, 51.2808, 52.4068, 54.5097, 53.2577)
)

merged_data <- merge(cities_uk, sales_by_top10_city, by = "city", all.x = TRUE)

world <- ne_countries(scale = "medium", returnclass = "sf")
uk <- world[world$admin == 'United Kingdom', ]
df_sf <- st_as_sf(merged_data, coords = c("lon", "lat"), crs = 4326)

#city_colors <- c("London" = "red", "Manchester" = "blue", "Birmingham" = "green", "Leeds" = "yellow", "Glasgow" = "purple", "Liverpool" = "orange", "Newcastle" = "pink", "Sheffield" = "brown", "Bristol" = "cyan", "Edinburgh" = "magenta")

highest_rev_plot <- ggplot() +
  geom_sf(data = uk, fill = "white", color = "black") +
  geom_sf(data = df_sf, aes(color = city)) +
  geom_text(data = merged_data , aes(x = lon, y = lat, label = revenue), size = 3, hjust = 0, nudge_x = 0.05) +
  scale_color_discrete(name = "City") +
  theme_void() +
  theme(legend.position = "bottom")+
  labs(title = "top 10 cities with highest revenue")

highest_rev_plot



#8
#using sql to create a dataframe to find the sale per product
Sales_user_type <- RSQLite::dbGetQuery(connection,
                                       "SELECT a.price, b.buyer_id, c.user_type,datetime(b.order_date, 'unixepoch') AS date_time
                                      FROM project_products a
                                      INNER JOIN project_buyer_orders_products b ON a.product_id = b.product_id
 
                                                                           INNER JOIN project_buyer c ON b.buyer_id = c.buyer_id ")
#8
#using R to sum sales per order
revenue_per_order <- Sales_user_type  %>%
  group_by(date_time, buyer_id, user_type) %>%
  summarise(sum_price = sum(price)) %>%
  ungroup()

#Counting orders per buyer
number_of_orders_per_buyer <- revenue_per_order %>%
  count(buyer_id)

Count_of_buyers <- RSQLite::dbGetQuery(connection,"
                                       SELECT DISTINCT(buyer_id)
                                       FROM project_buyer ")

total_orders_per_buyer <- left_join(Count_of_buyers ,number_of_orders_per_buyer, by = "buyer_id") %>%
  mutate(order_quantity = ifelse(is.na(n), 0, n))

total_orders_per_buyer<- total_orders_per_buyer %>%
  select(-n)

orders_plot_data <-total_orders_per_buyer %>%
  group_by(order_quantity) %>%
  summarise(num_buyers = n_distinct(buyer_id))

# Plot bar graph
buyers_by_orders_plot <- ggplot(orders_plot_data, aes(x = order_quantity, y = num_buyers)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Number of Buyers by Number of Orders", x = "Number of Orders", y = "Number of Buyers") +
  theme_minimal()

buyers_by_orders_plot

#9
#dividing the date and time column to just get date

revenue_per_order <- revenue_per_order %>%
  separate(date_time, into = c("date", "time"), sep = " ")

#Adding discounts and shipping
final_balance<- revenue_per_order %>%
  mutate(
    discount = ifelse(user_type == "VIP", 0.1, 0),
    price_after_discount = sum_price * (1 - discount),
    shipping = case_when(
      sum_price < 100 & user_type != "VIP" & user_type != "premium" ~ 10,
      user_type %in% c("premium", "VIP") ~ 0,
      TRUE ~ NA_real_
    ),
    total = price_after_discount + shipping
  )

average_revenue_after_shipping_discounts <-final_balance %>%
  summarise(total_revenue = sum(total))

#Finding total revenue after adding the discount and shipping cost
total_revenue <- sum(final_balance$total, na.rm = TRUE)


#10
#Finding total sales by the user type

Sales_by_user_type <- RSQLite::dbGetQuery(connection, 
                                          "SELECT SUM(b.price) AS revenue, c.user_type
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_buyer c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.user_type
                                      ORDER BY revenue DESC" )






# Count the occurrences of each product_id
top_products <- project_buyer_orders_products %>%
  count(product_id, sort = TRUE) %>%
  top_n(10)

# Display the top 10 products sold
print(top_products)

# Plotting the top 10 products sold
plot <- ggplot(top_products, aes(x = reorder(product_id, n), y = n)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Top 10 Products Sold",
       x = "Product ID",
       y = "Count") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Rotate x-axis labels for better readability

# Define the directory to save the figures
figure_directory <- "figures/"

# Create the directory if it doesn't exist
if (!dir.exists(figure_directory)) {
  dir.create(figure_directory)
}

# Save each plot as an image
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))

# Save top_10 plot
ggsave(filename = paste0(figure_directory, "top_10_", this_filename_date, "_", this_filename_time, ".png"), plot = top_10)

# Save revenue_by_categories_plot
ggsave(filename = paste0(figure_directory, "revenue_by_categories_plot_", this_filename_date, "_", this_filename_time, ".png"), plot = revenue_by_categories_plot)

# Save revenue_by_categories_bar plot
ggsave(filename = paste0(figure_directory, "revenue_by_categories_bar_", this_filename_date, "_", this_filename_time, ".png"), plot = revenue_by_categories_bar)

# Save top_10_sellers_plot
ggsave(filename = paste0(figure_directory, "top_10_sellers_plot_", this_filename_date, "_", this_filename_time, ".png"), plot = top_10_sellers_plot)

# Save highest_rev_plot
ggsave(filename = paste0(figure_directory, "highest_rev_plot_", this_filename_date, "_", this_filename_time, ".png"), plot = highest_rev_plot)

# Save buyers_by_orders_plot
ggsave(filename = paste0(figure_directory, "buyers_by_orders_plot_", this_filename_date, "_", this_filename_time, ".png"), plot = buyers_by_orders_plot)
