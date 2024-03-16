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

# Function to perform data validation on all data frames
perform_data_validation <- function(data_frames) {
  # List to store validation results
  validation_results <- list()
  
  # Loop through each data frame
  for (df_name in names(data_frames)) {
    df <- data_frames[[df_name]]
    
    # Initialize list to store validation results for the current data frame
    df_validation_results <- list()
    
    # Initialize invalid indices vector
    invalid_indices <- integer(0)
    
    # Loop through each column in the data frame
    for (col_name in names(df)) {
      column <- df[[col_name]]
      
      # Check column data type and perform specific validation
      if (col_name == "name") {
        # Validate if column contains only text
        invalid_indices <- c(invalid_indices, which(!grepl("^[A-Za-z ]+$", column)))
      } else if (grepl("phone_number", col_name, ignore.case = TRUE)) {
        # Validate phone number columns
        invalid_indices <- c(invalid_indices, which(!(nchar(column) == 10 & grepl("^\\d+$", column))))
      } else if (grepl("email", col_name, ignore.case = TRUE)) {
        # Validate email address columns
        invalid_indices <- c(invalid_indices, which(!grepl("^.+@.+\\..+$", column)))
      }
      
      # Remove rows with invalid values
      if (length(invalid_indices) > 0) {
        df <- df[-invalid_indices, ]
        df_validation_results[[paste0("Invalid values in column:", col_name)]] <- "Removed invalid rows"
      }
    }
    
    # Check if any errors occurred during validation
    if (length(df_validation_results) > 0) {
      # If errors occurred, print validation results
      print(paste("Invalid data in", df_name))
      print(df_validation_results)
    } else {
      # If validation passed, print confirmation
      print(paste("Valid data in", df_name))
    }
    
    # Update the data frame in the list
    data_frames[[df_name]] <- df
  }
  
  # Return validated data frames
  return(data_frames)
}

# Perform data validation for all data frames and update them if validation passes
all_data_frames <- perform_data_validation(all_data_frames)
