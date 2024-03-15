library(RSQLite)

# Establish a connection to your SQLite database
connection <- dbConnect(RSQLite::SQLite(), "database/group32.db")
# Define a function to perform data validation checks
perform_data_validation <- function(table_name, column_name) {
  
  # Check for missing values
  missing_values <- dbGetQuery(connection, paste("SELECT COUNT(*) AS num_missing FROM ", table_name, " WHERE ", column_name, " IS NULL"))
  cat("Missing values in", table_name, ":", missing_values$num_missing, "\n")
  
  # Check data types
  column_data_types <- get_column_data_types(table_name)
  print(column_data_types)
  
  # Check for duplicates
  duplicate_rows <- dbGetQuery(connection, paste("SELECT COUNT(*) AS num_duplicates FROM (SELECT *, COUNT(*) AS num_rows FROM ", table_name, " GROUP BY ", column_name, " HAVING num_rows > 1)"))
  cat("Duplicate rows in", table_name, ":", duplicate_rows$num_duplicates, "\n")
  
  # Check referential integrity (foreign key constraints)
  # You can join tables to check referential integrity based on foreign key relationships
  
}

# Perform data validation checks for each table
perform_data_validation("buyer", "email")
perform_data_validation("seller", "seller_id")
perform_data_validation("products", "product_id")
perform_data_validation("categories", "category_id")
perform_data_validation("contact_details", "address_id")
perform_data_validation("review", "review_id")
perform_data_validation("carrier", "carrier_id")
perform_data_validation("refund", "refund_ids")
perform_data_validation("buyer_orders_products", "product_id")


# Close the database connection
dbDisconnect(connection)
