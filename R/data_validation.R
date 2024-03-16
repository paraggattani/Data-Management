# Load necessary libraries
library(readr)


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

# Function to perform data validation
perform_data_validation <- function(connection) {
  # List to store validation results
  validation_results <- list()
  
  # List of SQL table names
  table_names <- c("project_products", "project_seller", "project_categories", 
                   "project_buyer", "project_contact_details", "project_review",
                   "project_carrier", "project_refund", "project_buyer_orders_products")
  
  # Loop through each SQL table
  for (table_name in table_names) {
    # Initialize list to store validation results for the current table
    table_validation_results <- list()
    
    # Primary Key Validation
    primary_key_query <- paste0("SELECT COUNT(*) FROM (SELECT DISTINCT '",
                                table_name, "'.'product_id' FROM '", table_name, "')")
    primary_key_result <- dbGetQuery(connection, primary_key_query)
    if (primary_key_result[[1]] == 0) {
      table_validation_results[["Primary Key"]] <- "No unique primary keys"
    }
    
    # Unique Columns Validation
    unique_columns <- c("seller_id", "category_id", "email")  # Add more unique columns here
    for (column_name in unique_columns) {
      unique_query <- paste0("SELECT COUNT(*) FROM (SELECT DISTINCT '", column_name,
                             "' FROM '", table_name, "')")
      unique_result <- dbGetQuery(connection, unique_query)
      if (unique_result[[1]] != 0) {
        table_validation_results[[paste0("Unique ", column_name)]] <- "Duplicate values found"
      }
    }
    
    # Referential Integrity Validation (Foreign Key Constraints)
    foreign_key_constraints <- list(
      "seller_id" = "project_seller",
      "category_id" = "project_categories"
      # Add more foreign key constraints here
    )
    for (column_name in names(foreign_key_constraints)) {
      reference_table <- foreign_key_constraints[[column_name]]
      foreign_key_query <- paste0("SELECT COUNT(*) FROM (SELECT DISTINCT '",
                                  table_name, "'.'", column_name, "' FROM '",
                                  table_name, "' LEFT JOIN '", reference_table, "' ON '",
                                  table_name, "'.'", column_name, "' = '",
                                  reference_table, "'.'", column_name, "')")
      foreign_key_result <- dbGetQuery(connection, foreign_key_query)
      if (foreign_key_result[[1]] != 0) {
        table_validation_results[[paste0("Referential Integrity for ", column_name)]] <- "Invalid references found"
      }
    }
    
    # Check if any errors occurred during validation
    if (length(table_validation_results) > 0) {
      # If errors occurred, print validation results for the current table
      print(paste("Invalid data in table:", table_name))
      print(table_validation_results)
    } else {
      # If validation passed, print confirmation for the current table
      print(paste("Valid data in table:", table_name))
    }
  }
  
  # Return the overall validation results
  return(validation_results)
}

# Perform data validation for all SQL tables
perform_data_validation(connection)
