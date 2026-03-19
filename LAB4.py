from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import date_format, sum as _sum
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize SparkSession
spark = SparkSession.builder.appName("LAB3").getOrCreate()
# A. Load sample data into a DataFrame
df = spark.read.csv("amazon.csv", header=True, inferSchema=True)

# =============
# ANALYSIS 1
# =============
# Cleaning:
cleaned_df = df.dropna().dropDuplicates() 

# Selecting and Filtering:
filtered_df = cleaned_df.select("order_id", "product_category", "price", "quantity_sold", "customer_region") \
                        .filter((col("product_category") == "Books") & (col("quantity_sold") > 2))
# Visualization #1 (Matplotlib)
# Convert to Pandas
pandas_filtered_df = filtered_df.toPandas()
plt.figure(figsize=(8, 5))
plt.hist(pandas_filtered_df['price'], color='skyblue', edgecolor='gray')
plt.title("Price Distribution of  Book Orders > 2", fontdict={'family': 'serif', 'size': 15})
plt.xlabel("Price")
plt.ylabel("Frequency")
plt.show()

# Visualization #1 (Seaborn)
plt.figure(figsize=(8, 5))
sns.countplot(data=pandas_filtered_df, x='customer_region', palette='viridis')
plt.title("Count of Bulk Book Orders by Customer Region")
plt.xlabel("Customer Region")
plt.ylabel("Number of Orders")
plt.show()

# =============
# ANALYSIS 2
# =============
# Write complex SQL queries to extract insights
cleaned_df.createOrReplaceTempView("ecommerce_sales")
# Query: Extract the top 5 most frequent product categories
top_categories_sql = spark.sql("""
    SELECT product_category, COUNT(*) AS frequency, CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS revenue
    FROM ecommerce_sales
    GROUP BY product_category
    ORDER BY frequency DESC
    LIMIT 5
""")

# Visualization #2 (Matplotlib)
# Convert to pandas
top_categories_pd = top_categories_sql.toPandas()
# Convert revenue to float for accuracy of the plots
top_categories_pd['revenue'] = top_categories_pd['revenue'].astype(float)
plt.figure(figsize=(10, 6))
plt.barh(top_categories_pd['product_category'], top_categories_pd['frequency'], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'])
plt.title("Top 5 Most Frequent Product Categories", fontdict={'family': 'serif', 'size': 15})
plt.xlabel("Frequency (Number of Orders)")
plt.ylabel("Product Category")
plt.gca().invert_yaxis()
plt.show()

# Visualization #2 (Seaborn)
plt.figure(figsize=(10, 6))
sns.barplot(x='product_category', y='revenue', data=top_categories_pd, palette='viridis')
plt.title("Total Revenue of Top 5 Product Categories", fontsize=15)
plt.xlabel("Total Revenue ($)")
plt.ylabel("Product Category")
plt.show()


# =============
# ANALYSIS 3
# =============
# Format the date and aggregate the sum of revenue and profit natively
trend_df = cleaned_df.withColumn("YearMonth", date_format("order_date", "yyyy-MM"))
monthly_trend_spark = trend_df.groupBy("YearMonth").agg(
    _sum("total_revenue").alias("total_revenue"),
    _sum("profit").alias("profit")
).orderBy("YearMonth")

# Visualization #3 (Matplotlib)
# convert to pandas
monthly_trend_pd = monthly_trend_spark.toPandas()

plt.figure(figsize=(12, 6))
plt.plot(monthly_trend_pd['YearMonth'], monthly_trend_pd['total_revenue'], marker='o', color='b', linestyle='-')
plt.title("Total Revenue Over Time")
plt.xlabel("Date (Year-Month)")
plt.ylabel("Total Revenue ($)")
plt.xticks(rotation=45)
plt.show()

# Visualization #3 (Seaborn)
plt.figure(figsize=(12, 6))
sns.lineplot(x='YearMonth', y='profit', data=monthly_trend_pd, marker='s', color='green')
plt.title("Total Profit Over Time")
plt.xlabel("Date (Year-Month)")
plt.ylabel("Total Profit ($)")
plt.xticks(rotation=45)
plt.show()

# =============
# ANALYSIS 4
# =============
# Group by product category and calculate total profit using PySpark
category_profit_spark = cleaned_df.groupBy("product_category").agg(
    _sum("profit").alias("total_profit")
)
# Convert to Pandas 
category_profit_pd = category_profit_spark.toPandas()

# Visualization #4 (Matplotlib)
plt.figure(figsize=(10, 6))
plt.bar(category_profit_pd['product_category'], category_profit_pd['total_profit'], color='teal', edgecolor='black')
plt.title("Total Profit by Product Category")
plt.xlabel("Product Category")
plt.ylabel("Total Profit ($)")
plt.xticks(rotation=45)
plt.show()

# Visualization #4 (Seaborn)
profit_distribution_pd = cleaned_df.select("product_category", "profit").toPandas()
plt.figure(figsize=(10, 6))
sns.boxplot(x='product_category', y='profit', data=profit_distribution_pd, palette='Set2')
plt.title("Distribution of Profit per Product Category")
plt.xlabel("Product Category")
plt.ylabel("Profit ($)")
plt.xticks(rotation=45)
plt.show()


# =============
# ANALYSIS 5
# =============
# Groupby and Convert to pandas
payment_counts_pd = cleaned_df.groupBy("payment_method").count().toPandas()

# Visualization #5 (Matplotlib)
plt.figure(figsize=(8, 8))
plt.pie(payment_counts_pd['count'], labels=payment_counts_pd['payment_method'], autopct='%1.1f%%', 
        startangle=140, colors=['#ff9999','#66b3ff','#99ff99','#ffcc99', "#ff3df2"])
plt.title("Market Share by Payment Method")
plt.show()

# Visualization #5 (Seaborn)
# Convert to Pandas 
category_counts_pd = cleaned_df.groupBy("product_category").count().toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x='product_category', y='count', data=category_counts_pd, palette='Set2')
plt.title("Number of Orders per Product Category", fontsize=15)
plt.xlabel("Product Category")
plt.ylabel("Order Count")
plt.show()