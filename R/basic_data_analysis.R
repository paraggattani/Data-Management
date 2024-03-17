# Define the referential integrity constraints (foreign key relationships)
foreign_key_constraints <- list(
  "project_products" = list("seller_id" = sellers_df, "category_id" = categories_df),
  # Add more foreign key constraints here if needed
)



# Perform referential integrity check
for (table_name in names(foreign_key_constraints)) {
  for (column_name in names(foreign_key_constraints[[table_name]])) {
    invalid_indices <- referential_integrity_check(get(table_name), column_name, foreign_key_constraints[[table_name]][[column_name]])
    if (length(invalid_indices) > 0) {
      cat("Referential integrity violation in table:", table_name, ", column:", column_name, "\n")
      # Remove rows with invalid foreign key values
      assign(table_name, get(table_name)[-invalid_indices, ])
    }
  }
}

