library(RSQLite)

# Establish a connection to your SQLite database
connection <- dbConnect(RSQLite::SQLite(), "database/group32.db")

# Define a function to perform data validation checks
perform_data_validation <- function(table_name) {
  
  # Check for missing values
  missing_values_query <- paste0("SELECT COUNT(*) AS num_missing FROM ", table_name)
  missing_values <- dbGetQuery(connection, missing_values_query)
  cat("Missing values in", table_name, ":", missing_values$num_missing, "\n")
  
  # Check data types
  column_data_types <- get_column_data_types(table_name)
  print(column_data_types)
  
  # Check for duplicates
  duplicate_rows_query <- paste0("SELECT COUNT(*) AS num_duplicates FROM (SELECT *, COUNT(*) AS num_rows FROM ", table_name, " GROUP BY ", paste(column_data_types$Column, collapse = ", "), " HAVING num_rows > 1)")
  duplicate_rows <- dbGetQuery(connection, duplicate_rows_query)
  cat("Duplicate rows in", table_name, ":", duplicate_rows$num_duplicates, "\n")
  
  # Check referential integrity (foreign key constraints)
  # You can join tables to check referential integrity based on foreign key relationships
  
}


# Perform data validation checks for each table
perform_data_validation("buyer")
perform_data_validation("seller")
perform_data_validation("products")
perform_data_validation("categories")
perform_data_validation("contact_details")
perform_data_validation("review")
perform_data_validation("carrier")
perform_data_validation("refund")
perform_data_validation("buyer_orders_products")

# Close the database connection
dbDisconnect(connection)
