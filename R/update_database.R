# Load necessary libraries
library(readr)
library(RSQLite)
library(dplyr)
library(sf)
library(rnaturalearth)
library(tidyr)
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
  FOREIGN KEY ('seller_id') REFERENCES project_seller ('seller_id'),
  FOREIGN KEY ('category_id') REFERENCES project_category('category_id')
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
  FOREIGN KEY ('buyer_id') REFERENCES project_buyer ('buyer_id')
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
   FOREIGN KEY ('buyer_id') REFERENCES project_buyer ('buyer_id'),
   FOREIGN KEY ('product_id') REFERENCES project_products ('product_id')
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
  FOREIGN KEY ('product_id') REFERENCES project_products ('product_id'),
  FOREIGN KEY ('buyer_id') REFERENCES project_buyer ('buyer_id')
);
")

#Create relationship table references
dbExecute(connection, "
CREATE TABLE IF NOT EXISTS 'project_references' (
  'buyer_id' VARCHAR(255),
  'referred_by' VARCHAR(255),
   FOREIGN KEY ('buyer_id') REFERENCES project_buyer ('buyer_id')
);
")


# Verify the table was created by listing all tables in the database
RSQLite::dbListTables(connection)

# Create a list to store all entity dataframes
all_entity_data <- list()

# List all entity folders within the "data_upload" directory
entity_folders <- list.dirs(path = "data_upload", full.names = TRUE, recursive = FALSE)

# Loop through each entity folder
for (entity_folder in entity_folders) {
  # Extract the folder name
  entity_name <- basename(entity_folder)
  
  # List all CSV files in the entity folder
  csv_files <- list.files(path = entity_folder, pattern = "\\.csv$", full.names = TRUE)
  
  # Initialize an empty dataframe to store data for the entity
  entity_data <- data.frame()
  
  # Loop through each CSV file in the entity folder
  for (csv_file in csv_files) {
    # Read the CSV file into a dataframe
    csv_data <- read.csv(csv_file)
    
    # Merge the data from the CSV file into the entity dataframe
    entity_data <- rbind(entity_data, csv_data)
  }
  
  # Assign the entity dataframe to a variable with a name starting with "project_"
  assign(paste0("project_", entity_name), entity_data)
  
  # Add the entity dataframe to the list
  all_entity_data[[paste0("project_", entity_name)]] <- entity_data
}

# Check the names of the created entity dataframes
print(names(all_entity_data))



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

# Function to check if data exists in a table
data_exists <- function(connection, table_name, data_frame) {
  # Construct the query to check for existence of data
  query <- paste0("SELECT COUNT(*) FROM ", table_name)
  result <- dbGetQuery(connection, query)
  return(result[[1]] > 0)
}

# Function to insert data into a table if it doesn't exist
insert_data_if_not_exists <- function(connection, table_name, data_frame) {
  # Check if data already exists in the table
  if (data_exists(connection, table_name)) {
    cat("Data already exists in", table_name, "\n")
    return()
  }
  
  # Extract column names
  columns <- names(data_frame)
  
  # Construct the INSERT INTO SQL query
  insert_query <- paste0("INSERT INTO '", table_name, "' (", paste0("'", columns, "'", collapse = ", "), ") VALUES ")
  
  # Loop through each row of the data frame and insert values
  for (i in 1:nrow(data_frame)) {
    values <- paste0("(", paste0("'", gsub("'", "''", unlist(data_frame[i,])), "'", collapse = ","), ")")
    dbExecute(connection, paste0(insert_query, values))
  }
  
  cat("Data inserted into", table_name, "\n")
}






# Insert data from data frames into respective tables
insert_data_if_not_exists(connection, "project_products", project_products_df)
insert_data_if_not_exists(connection, "project_seller", project_sellers_df)
insert_data_if_not_exists(connection, "project_categories", project_categories_df)
insert_data_if_not_exists(connection, "project_buyer", project_buyer_df)
insert_data_if_not_exists(connection, "project_contact_details", project_contact_df)
insert_data_if_not_exists(connection, "project_review", project_review_df)
insert_data_if_not_exists(connection, "project_buyer_orders_products", project_buyer_orders_products)
insert_data_if_not_exists(connection, "project_references", project_references_df)


# Verify the table was created by listing all tables in the database
#RSQLite::dbListTables(connection)

# Optionally, verify the data was inserted
products_table <- dbReadTable(connection, "project_products")
buyer_table <- dbReadTable(connection, "project_buyer")

# Data Analysis

#1
#Average daily sales
#calculating the average revenue we are earning every day

#Writing SQL query to retrieve the money generated from sales and the date from product and orders table

sales<- RSQLite::dbGetQuery(connection,"
          SELECT a.price, datetime(b.order_date, 'unixepoch') AS date_time
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )

#seperating the date_time column to get date separately
sales_by_date <- sales %>%
  separate(date_time, into = c("date", "time"), sep = " ")

#dividing the total sales by the distinct date time to calculate the average sales

average_revenue <-sales_by_date %>%
  group_by(date) %>%
  summarise(total_revenue = sum(price)) %>%
  summarise(average_revenue = sum(total_revenue) / n_distinct(date))

#2
#Total sales 
#Calculating the total revenue we have

#writing the SQL query to retrieve the sum of revenue gathered through all sales

total_sales<-RSQLite::dbGetQuery(connection, "
          SELECT SUM(a.price) AS total_sales
          FROM project_buyer_orders_products b
          INNER JOIN project_products a ON b.product_id = a.product_id " )

#3
# best performing products by revenue

#writing the SQL query to retrieve best selling products by revenue gathered

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

#plotting the best selling products by revenue using GGplot
top_10 <- ggplot(top_10_products, aes(x = reorder(product_name, -revenue), y = revenue, fill = product_name)) +
  geom_bar(stat = "identity",show.legend = FALSE) +
  labs(title = "Top 10 Products by Revenue", x = "Product Name", y = "Revenue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

top_10

#4
#revenue by categories
#We calculate the total revenue of each category

#Writing the SQL query to calculate the revenue for each category

revenue_by_categories <- RSQLite::dbGetQuery(connection,
                                             "SELECT c.category_name, SUM(b.price) AS revenue
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_categories c ON b.category_id = c.category_id
                                      GROUP BY c.category_name
                                      ORDER BY revenue DESC
                                      " )

#Finding the revenue for each category over the time period
revenue_by_categories_per_date <- RSQLite::dbGetQuery(connection,
                                                      "SELECT c.category_name, b.price, datetime(a.order_date, 'unixepoch') AS date_time
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_categories c ON b.category_id = c.category_id
                                      
                                      " )

#seperating the date_time format into date and time
revenue_by_categories_per_date<- revenue_by_categories_per_date %>%
  separate(date_time, into = c("date", "time"), sep = " ")

#calculating the revene each category gathered each day 
revenue_by_categories_per_date <- revenue_by_categories_per_date  %>%
  group_by(category_name, date) %>%
  summarise(sum_price = sum(price)) %>%
  ungroup()

#plotting Revenue by category over time for each category using ggplot and facet wrap

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

#first we write an sql query to get sellers and their revenue
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

#plotting top 10 sellers by revenue
top_10_sellers_plot <- ggplot(top_10_sellers, aes(x = reorder(seller_name, -revenue), y = revenue, fill = seller_name)) +
  geom_bar(stat = "identity",show.legend = FALSE) +
  labs(title = "Top 10 Sellers by Revenue", x = "Seller Name", y = "Revenue") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

top_10_sellers_plot

#6
#calculating sales by state

# we write the SQL query to get the dataframe for the revenue grouped by state

sales_by_state <- RSQLite::dbGetQuery(connection, 
                                      "SELECT SUM(b.price) AS revenue, c.state
                                      FROM project_buyer_orders_products a
                                      INNER JOIN project_products b ON a.product_id = b.product_id
                                      INNER JOIN project_contact_details c ON a.buyer_id = c.buyer_id
                                      GROUP BY c.state
                                      ORDER BY revenue DESC" )

sales_by_state


#7
#sales_by_city

#we write the SQL query to get the dataframe for the revenue grouped by city

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

#to plot the cities on the map we create a dataframe with the names of our top 10 cities and their longitude and latitude

cities_uk <- data.frame(
  city = c("Edinburgh", "Bangor", "Derby", "St Davids", "Ripon", "Doncaster", "Canterbury", "Coventry", "Lisburn", "St Asaph"),
  lon = c(-3.1883, -4.1267, -1.4721, -5.2717, -1.5211, -1.1307, 1.0756, -1.5118, -6.0332, -3.4436),
  lat = c(55.9533, 53.2268, 52.9228, 51.8815, 54.1361, 53.5228, 51.2808, 52.4068, 54.5097, 53.2577)
)

#we merge our dataframe that we got through SQL with the one we created on the names of the cities to get revenue and longitude and latitude in the same dataframe

merged_data <- merge(cities_uk, sales_by_top10_city, by = "city", all.x = TRUE)

#plotting the top 10 cities on the map using the sf library and ggplot

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

#we use the unique combinations of buyer_id and order_date as one order_id 

revenue_per_order <- Sales_user_type  %>%
  group_by(date_time, buyer_id, user_type) %>%
  summarise(sum_price = sum(price)) %>%
  ungroup()

#Counting orders per buyer
number_of_orders_per_buyer <- revenue_per_order %>%
  count(buyer_id)

#counting total number of buyers on the system
Count_of_buyers <- RSQLite::dbGetQuery(connection,"
                                       SELECT DISTINCT(buyer_id)
                                       FROM project_buyer ")

#joining the buyers who have made an order with those who havent made any order

total_orders_per_buyer <- left_join(Count_of_buyers ,number_of_orders_per_buyer, by = "buyer_id") %>%
  mutate(order_quantity = ifelse(is.na(n), 0, n))

total_orders_per_buyer<- total_orders_per_buyer %>%
  select(-n)

#plotting how many buyers we have per order number
#we are trying to see how many buyers have never ordered, how many have made 1 and so on.

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
#finding the total revenue after shipping and discounts

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


#11
#Finding average order revenue 

average_revenue_by_order <- mean(revenue_per_order$sum_price)

#The average revenue we make from an order is 54.26 pounds

#12
#Finding the average number of products in an order
#Writing an SQL query to get the product_id , buyer_id, order_date, we use the unique combinations of buyer_id and order_date as one order_id
Orders <- RSQLite::dbGetQuery(connection,
                              "SELECT a.product_id, b.buyer_id, b.order_date
                              FROM project_buyer_orders_products b
                              INNER JOIN project_products a ON b.product_id = a.product_id" )


#using dplyr to count how many products have the same buyer_id and date_time to see the average quantity of products in an order
order_quantity <- Orders %>%
  group_by( buyer_id, order_date) %>%
  summarise(count = n_distinct(product_id)) 

average_order_quantity <- round(mean(order_quantity$count))

#From this we can see that on average we have 3 products in an order





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
ggsave(filename = paste0(figure_directory, "top_10_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 6, plot = top_10)

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

