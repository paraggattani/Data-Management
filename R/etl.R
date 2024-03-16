# Load necessary libraries
library(readr)
library(RSQLite)

# List all CSV files in the data_upload directory
csv_files <- list.files(path = "data_upload", pattern = "\\.csv$", full.names = TRUE)

# Loop through each CSV file
for (csv_file in csv_files) {
  # Extract filename without extension
  filename <- tools::file_path_sans_ext(basename(csv_file))
  
  # Assign each CSV file to a new data frame with a name starting with "project_"
  assign(paste0("project_", filename), read.csv(csv_file))
}

# Check the names of the created data frames
print(ls(pattern = "^project_"))

