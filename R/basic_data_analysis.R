# Load necessary libraries
library(readr)
library(dplyr)
library(ggplot2)

# Assuming you have loaded your data frames into appropriate variables,
# Extract the relevant columns for category sales
category_sales <- buyer_orders_products_data %>%
  group_by(category_id) %>%
  summarise(total_sales = sum(price))

# Plotting category sales
ggplot(category_sales, aes(x = category_id, y = total_sales)) +
  geom_bar(stat = "identity") +
  labs(title = "Category Sales", x = "Category ID", y = "Total Sales")

