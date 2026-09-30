# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Joining
#Loading my tables for analysis
import pandas as pd
spark.sql("USE CATALOG shopperfomance")
spark.sql("USE SCHEMA shop_perfomances")

customers_df = spark.table("customers").toPandas()
orders_df = spark.table("orders").toPandas()
products_df = spark.table("products").toPandas()
payments_df = spark.table("payments").toPandas()

#Applying my cleaning code before joining

#Customers
customers_df["City"] = customers_df["City"].replace({
    "tehran": "Tehran",
    "Mashad": "Mashhad"
})

customers_df["Age"] = customers_df["Age"].fillna(
    customers_df["Age"].median()
)

#Orders
orders_df = orders_df.drop_duplicates()

orders_df["Discount"] = orders_df["Discount"].fillna(0)

#Payments
#Missing PaymentDate values are retained as missing

#Joining Orders with Products

orders_products_df = orders_df.merge(
    products_df,
    on="ProductID",
    how="left"
)

print("Orders rows:", len(orders_df))
print("After Products join:", len(orders_products_df))

#Joining Orders + Products with Customers

orders_products_customers_df = orders_products_df.merge(
    customers_df,
    on="CustomerID",
    how="left"
)

print("Before Customers join:", len(orders_products_df))
print("After Customers join:", len(orders_products_customers_df))

#Joining Orders + Products + Customers with Payments

final_df = orders_products_customers_df.merge(
    payments_df,
    on="OrderID",
    how="left"
)

print("Before Payments join:", len(orders_products_customers_df))
print("After Payments join:", len(final_df))

# COMMAND ----------

# DBTITLE 1,CASE STUDY ANALYSIS
#Creating my Revenue colum

final_df["Revenue"] = (
    final_df["Quantity"]
    * final_df["UnitPrice"]
    * (1 - final_df["Discount"])
)

#Rounding my Revenue to 2 decimal places
final_df["Revenue"] = final_df["Revenue"].round(2)

#Converting my OrderDate to datetime
final_df["OrderDate"] = pd.to_datetime(final_df["OrderDate"])

#Creating my Year and Month columns
final_df["Year"] = final_df["OrderDate"].dt.year
final_df["Month"] = final_df["OrderDate"].dt.month

display(final_df.head(5))

revenue_df = final_df[
    (final_df["Status"] == "Completed") &
    (final_df["PaymentStatus"] == "Paid")
].copy() #copy means make this a seperate copy of dataframe - to run the conditions, taking the work done above, which meet my condition

#variable calculation answer question 1
# Total Revenue
total_revenue = revenue_df["Revenue"].sum()
# Number of Orders
total_orders = revenue_df["OrderID"].nunique()
# Average Order Value
average_order_value = total_revenue / total_orders
#showing results of codes applied to variables
print("Total Revenue:", round(total_revenue, 2))
print("Total Orders:", total_orders)
print("Average Order Value:", round(average_order_value, 2))

#Checking my monthly revenue
monthly_revenue = (
    revenue_df
    .groupby(["Year", "Month"])["Revenue"]
    .sum()
    .reset_index() #reset and show as dataframe
    )
display(monthly_revenue)

#Checking my highest and lowest revenue months
highest_month = monthly_revenue.loc[
    monthly_revenue["Revenue"].idxmax()]

lowest_month = monthly_revenue.loc[
    monthly_revenue["Revenue"].idxmin()]  #---- Idmax/min locates the value in the table & row

print("Highest Revenue Month:")
print(highest_month)
print("\nLowest Revenue Month:")
print(lowest_month)

#Checking average monthly revenue by calendar month
seasonal_revenue = (
    monthly_revenue
    .groupby(["Year","Month"])["Revenue"]
    .mean()
    .round(2)
    .reset_index()
    .sort_values("Revenue", ascending=False))

display(seasonal_revenue)

#Top 3 seasonal revenue months

display(
    seasonal_revenue.head(3)
)

#Checking revenue by product
product_revenue = (
    revenue_df
    .groupby(["ProductID", "ProductName"])["Revenue"]
    .sum()
    .reset_index()
    .sort_values("Revenue", ascending=False)
)
display(product_revenue.head(10))

#Checking units sold by product
product_units = (
    revenue_df
    .groupby(["ProductID", "ProductName"])["Quantity"]
    .sum()
    .reset_index()
    .sort_values("Quantity", ascending=False)
)

display(product_units.head(10))

#Comparing top products by Revenue and Units Sold
product_comparison = product_revenue.merge(
    product_units,
    on=["ProductID", "ProductName"],
    how="left"
)

display(product_comparison.head(10))

#Checking revenue and units sold by category
category_performance = (
    revenue_df
    .groupby("Category")
    .agg(
        Revenue=("Revenue", "sum"),
        UnitsSold=("Quantity", "sum")
    )
    .reset_index()
    .sort_values("Revenue", ascending=False)
)

display(category_performance)

#Checking revenue by city
city_revenue = (
    revenue_df
    .groupby("City")["Revenue"]
    .sum()
    .round(2)
    .reset_index()
    .sort_values("Revenue", ascending=False)
)
display(city_revenue)

#Checking revenue by customer segment
segment_revenue = (
    revenue_df
    .groupby("CustomerSegment")["Revenue"]
    .sum()
    .round(2)
    .reset_index()
    .sort_values("Revenue", ascending=False)
)
display(segment_revenue)

#Checking order status
order_status = (
    revenue_df["Status"]
    .value_counts(normalize=True)
    .mul(100)
    .round(2)
    .reset_index()
)
order_status.columns = ["Status", "Percentage"]
display(order_status)

#Checking payment status
payment_status = (
    revenue_df["PaymentStatus"]
    .value_counts(normalize=True)
    .mul(100)
    .round(2)
    .reset_index()
)
payment_status.columns = ["PaymentStatus", "Percentage"]
display(payment_status)

#Checking payment status values
display(revenue_df["PaymentStatus"].value_counts())

#Checking discount impact on orders and revenue
discount_analysis = (
    revenue_df
    .groupby("Discount")
    .agg(
        AverageQuantity=("Quantity", "mean"),
        TotalRevenue=("Revenue", "sum")
    )
    .round(2)
    .reset_index()
    .sort_values("Discount")
)
display(discount_analysis)
