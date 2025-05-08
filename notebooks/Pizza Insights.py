# Databricks notebook source
# MAGIC %md
# MAGIC # Pizza Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS PizzaDB;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG PizzaDB;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS PizzaStore;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CURRENT SCHEMA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA PizzaStore;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CURRENT SCHEMA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS PizzaUploads;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load `Pizza_Sales.csv` into `PizzaMenu` Table within `PizzaStore` Schema

# COMMAND ----------

# Define path without using dbfs
file_path = "/Volumes/pizzadb/pizzastore/pizzauploads/Pizza_Sales.csv"

# Read the CSV file
pizza_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Display Data
display(pizza_df)

# Define catalog, schema, and table names
catalog_name = "PizzaDB"
schema_name = "PizzaStore"
table_name = "PizzaMenu"

# Write data to a table
try:
    pizza_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
except Exception as e:
    print(f"Error creating table: {e}")
else:
    print(f"Table {catalog_name}.{schema_name}.{table_name} created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quantity, total_price, unit_price, pizza_category
# MAGIC FROM PizzaDB.PizzaStore.PizzaMenu;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC <br/>
# MAGIC
# MAGIC ## Sales Performace Analysis - Bubble Chart

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type 1: DBSQL Code 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     SUM(quantity) AS total_no_of_pizzas_sold,
# MAGIC     SUM(total_price) AS total_revenue_per_pizza,
# MAGIC     SUM(unit_price) AS price_per_pizza,
# MAGIC     pizza_category
# MAGIC FROM PizzaDB.PizzaStore.PizzaMenu
# MAGIC GROUP BY pizza_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type 2: PySpark with DBSQL Code 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Perform Aggregation Using PySpark

# COMMAND ----------

# Perform aggregation using PySpark
agg_df_1 = spark.sql("""
    SELECT 
        SUM(quantity) AS total_no_of_pizzas_sold,
        SUM(total_price) AS total_revenue_per_pizza,
        SUM(unit_price) AS price_per_pizza,
        pizza_category
    FROM PizzaDB.PizzaStore.PizzaMenu
    GROUP BY pizza_category
""")

# Display Aggregation
display(agg_df_1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Convert to Pandas for Visualization

# COMMAND ----------

# Convert to Pandas for visualization
p_agg_df_1 = agg_df_1.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create a Bubble Chart Using Plotly

# COMMAND ----------

import plotly.express as px

# Plot Bubble Chart using Plotly
fig_1 = px.scatter(
    p_agg_df_1, 
    x='total_revenue_per_pizza', 
    y='total_no_of_pizzas_sold', 
    size='price_per_pizza', 
    color='pizza_category',
    hover_name='pizza_category',
    title='Pizza Sales Analysis'
)

fig_1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type 3: Pure PySpark Code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Perform Aggregation Using PySpark

# COMMAND ----------

from pyspark.sql import functions as F

# Perform the aggregation using PySpark
agg_df_2 = pizza_df.groupBy("pizza_category").agg(
    F.sum("quantity").alias("total_no_of_pizzas_sold"),
    F.sum("total_price").alias("total_revenue_per_pizza"),
    F.sum("unit_price").alias("price_per_pizza")
)

# Display Aggregation
display(agg_df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Convert to Pandas for Visualization

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame for Plotly
p_agg_df_2 = agg_df_2.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create a Bubble Chart Using Plotly

# COMMAND ----------

import plotly.express as px

# Plot Bubble Chart using Plotly
fig_2 = px.scatter(
    p_agg_df_2,
    x="total_revenue_per_pizza",
    y="total_no_of_pizzas_sold",
    size="price_per_pizza",
    color="pizza_category",
    hover_name="pizza_category",
    title="Pizza Sales Analysis"
)

# Show the plot
fig_2.show()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Normalize bubble size for better visualization
size_factor = 50  # Adjust for better scaling
p_agg_df_2["bubble_size"] = p_agg_df_2["price_per_pizza"] * size_factor

# Set figure size
plt.figure(figsize=(12, 7))

# Create Bubble Chart using Seaborn
scatter = sns.scatterplot(
    data=p_agg_df_2,
    x="total_revenue_per_pizza",
    y="total_no_of_pizzas_sold",
    hue="pizza_category",  # Distinct color for each category
    size="bubble_size",  # Bubble size (not shown in legend)
    sizes=(50, 1000),  # Set min and max bubble sizes
    palette="tab10",  # Use a distinct color palette
    alpha=0.7,  # Transparency for better readability
    legend=True  # Disable the default legend (removes size from legend)
)

# Add a separate legend only for Pizza Category
handles, labels = scatter.get_legend_handles_labels()
plt.legend(handles[:len(p_agg_df_2["pizza_category"].unique())],  # Get only category handles
           labels[:len(p_agg_df_2["pizza_category"].unique())], 
           title="Pizza Category", 
           bbox_to_anchor=(1.05, 1), loc='upper left')

# Customize the plot
plt.title("Pizza Sales Analysis", fontsize=14, fontweight="bold")
plt.xlabel("Total Revenue Per Pizza")
plt.ylabel("Total Number of Pizzas Sold")

# Show the plot
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('QUARTER', order_date) AS quarter,
# MAGIC   pizza_category,
# MAGIC   SUM(total_price) AS total_price
# MAGIC FROM 
# MAGIC   PizzaDB.PizzaStore.PizzaMenu
# MAGIC GROUP BY 
# MAGIC   DATE_TRUNC('QUARTER', order_date),
# MAGIC   pizza_category
# MAGIC ORDER BY 
# MAGIC   quarter;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   pizza_category,
# MAGIC   SUM(total_price) AS total_price
# MAGIC FROM 
# MAGIC   PizzaDB.PizzaStore.PizzaMenu
# MAGIC GROUP BY 
# MAGIC   pizza_category
# MAGIC ORDER BY 
# MAGIC   total_price DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   pizza_size,
# MAGIC   pizza_category,
# MAGIC   SUM(quantity) AS total_pizzas_sold
# MAGIC FROM 
# MAGIC   PizzaDB.PizzaStore.PizzaMenu
# MAGIC GROUP BY 
# MAGIC   pizza_size,
# MAGIC   pizza_category
# MAGIC ORDER BY 
# MAGIC   pizza_size, 
# MAGIC   pizza_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   pizza_size,
# MAGIC   pizza_category,
# MAGIC   SUM(unit_price) AS total_unit_price
# MAGIC FROM 
# MAGIC   PizzaDB.PizzaStore.PizzaMenu
# MAGIC GROUP BY 
# MAGIC   pizza_size,
# MAGIC   pizza_category
# MAGIC ORDER BY 
# MAGIC   pizza_size, 
# MAGIC   pizza_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(total_price) / 1000 AS total_revenue
# MAGIC FROM PizzaDB.PizzaStore.PizzaMenu;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(quantity) / 1000, 0) AS total_quantity_sold
# MAGIC FROM PizzaDB.PizzaStore.PizzaMenu;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(unit_price) AS avg_price_per_pizza
# MAGIC FROM PizzaDB.PizzaStore.PizzaMenu;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE IF NOT EXISTS PizzaMenu;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS PizzaMenu;
# MAGIC
# MAGIC -- CREATE TABLE IF NOT EXISTS PizzaDB.PizzaStore.PizzaMenu (
# MAGIC --     id INT, 
# MAGIC --     name STRING, 
# MAGIC --     age INT, 
# MAGIC --     department STRING,
# MAGIC --     salary DOUBLE,
# MAGIC --     join_date DATE
# MAGIC -- );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESC TABLE PizzaMenu;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO PizzaMenu (id, name, age, department, salary, join_date) 
# MAGIC -- VALUES
# MAGIC --     (1, 'Alice', 30, 'HR', 55000.00, '2020-01-15'),
# MAGIC --     (2, 'Bob', 40, 'IT', 80000.00, '2018-03-22'),
# MAGIC --     (3, 'Charlie', 35, 'Finance', 65000.00, '2019-06-30'),
# MAGIC --     (4, 'David', 28, 'IT', 75000.00, '2021-02-10'),
# MAGIC --     (5, 'Eva', 50, 'HR', 85000.00, '2015-09-08'),
# MAGIC --     (6, 'Frank', 45, 'Finance', 90000.00, '2017-11-20'),
# MAGIC --     (7, 'Grace', 32, 'IT', 95000.00, '2022-08-15'),
# MAGIC --     (8, 'Hannah', 38, 'Finance', 67000.00, '2016-07-12');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM PizzaDB.PizzaStore.PizzaMenu;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average Salary by Department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Average Salary by Department
# MAGIC -- SELECT department, AVG(salary) AS avg_salary 
# MAGIC -- FROM PizzaDB.PizzaStore.PizzaMenu
# MAGIC -- GROUP BY department;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count of Employees by Age Group and Department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Count of Employees by Age Group and Department
# MAGIC -- SELECT 
# MAGIC --     CASE 
# MAGIC --         WHEN age BETWEEN 20 AND 30 THEN '20-30'
# MAGIC --         WHEN age BETWEEN 31 AND 40 THEN '31-40'
# MAGIC --         WHEN age BETWEEN 41 AND 50 THEN '41-50'
# MAGIC --         ELSE '50+'
# MAGIC --     END AS age_group, 
# MAGIC --     department,
# MAGIC --     COUNT(*) AS count_employees
# MAGIC -- FROM PizzaDB.PizzaStore.PizzaMenu
# MAGIC -- GROUP BY age_group, department
# MAGIC -- ORDER BY age_group, department;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salary Distribution by Join Date \(Yearly\)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Salary Distribution by Join Date (Yearly)
# MAGIC -- SELECT YEAR(join_date) AS join_year, AVG(salary) AS avg_salary 
# MAGIC -- FROM PizzaDB.PizzaStore.PizzaMenu
# MAGIC -- GROUP BY join_year
# MAGIC -- ORDER BY join_year;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Table
# MAGIC -- DROP TABLE IF EXISTS PizzaMenu;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Volume
# MAGIC -- DROP VOLUME IF EXISTS PizzaUploads;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Schema
# MAGIC -- DROP SCHEMA IF EXISTS PizzaStore;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Catalog
# MAGIC -- DROP CATALOG IF EXISTS PizzaDB CASCADE;
