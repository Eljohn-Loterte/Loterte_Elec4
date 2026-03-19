# **Amazon Sales Data Analysis System**
Course: BS - Information Technology 3A
Institution: Bicol University

# Introduction
This project focuses on performing large-scale data analysis on Amazon e-commerce datasets using Apache Spark (PySpark). As an aspiring Data Analyst, I developed this system to demonstrate the ability to process raw datasets, perform cleaning operations, and extract meaningful business insights through both programmatic transformations and complex SQL queries.

# About the Project
The core objective of this project is to analyze sales trends, profitability, and customer behavior. By leveraging the distributed computing power of Spark, the system handles data cleaning (removing nulls and duplicates) and processes multiple analytical dimensions, including:

    Segmented Pricing: Analyzing specific categories like "Books" with volume-based filters.

    Operational Metrics: Evaluating the top-performing categories by both order frequency and total revenue.

    Temporal Trends: Mapping financial growth (Revenue and Profit) over a monthly timeline.

    Financial Distribution: Understanding profit margins and outlier detection using advanced statistical plots.

    Market Share: Visualizing payment preferences and category demand.

# Data Visualizations & Insights
Analysis 1: Price Distribution & Regional Demand (Books > 2 Units)

Chart 1: Price Distribution

<img width="933" height="603" alt="image" src="https://github.com/user-attachments/assets/cc8acf4d-c455-4abd-b145-0c77766ae510" />
        Description: This histogram displays the price frequency of book orders more than two units. It identifies the most common price points for bulk book purchases, helping determine if bulk buyers favor economy or premium titles.<br>

Chart 2: Bulk Orders by Region

<img width="929" height="597" alt="image" src="https://github.com/user-attachments/assets/329ca3b7-79c4-4662-9128-4acd05f755f1" />
        Description: A regional breakdown showing where the highest volume of book orders originates, useful for targeting logistics and regional marketing efforts.

Analysis 2: Top 5 Product Categories Performance

Chart 3: Frequency of Orders

<img width="1160" height="717" alt="image" src="https://github.com/user-attachments/assets/b965ed5c-0776-4699-b466-2687de19d1e5" />
        Description: Shows which product categories are the most popular based on the total number of orders placed.<br>

Chart 4: Revenue Contribution

<img width="1114" height="710" alt="image" src="https://github.com/user-attachments/assets/8473498d-1a7e-475d-8399-2b9e02de410f" />
        Description: Compares the frequency against the actual dollar amount generated. This helps identify "High Volume, Low Value" vs. "Low Volume, High Value" categories.

Analysis 3: Financial Trends Over Time

Chart 5: Total Revenue Over Time

<img width="1360" height="725" alt="image" src="https://github.com/user-attachments/assets/84de8632-e1df-47e2-a94a-abd2b14d1b33" />
        Description: A monthly trend line that tracks the "Top Line" growth of the store.<br>

Chart 6: Total Profit Over Time

<img width="1339" height="729" alt="image" src="https://github.com/user-attachments/assets/a39f944f-4868-4d66-a62b-65b994906ef0" />
        Description: A trend line tracking net profit. Comparing this to the revenue chart reveals periods of high operational costs or improved efficiency.

Analysis 4: Category Profitability & Distribution

Chart 7: Total Profit by Category

<img width="1134" height="729" alt="image" src="https://github.com/user-attachments/assets/a36fa475-a5e3-490e-b732-59e6602c3d5b" />
        Description: A high-level view of which category puts the most money in the bank after all expenses are accounted for.<br>

Chart 8: Profit Distribution

<img width="1119" height="727" alt="image" src="https://github.com/user-attachments/assets/5d35ac82-55c5-43df-9290-0f91779095cc" />
        Description: This statistical plot shows the median profit per order, the interquartile range (IQR), and outliers. It reveals how consistent the profit is for every single sale within a specific category.

Analysis 5: Market Share & Order Volume

Chart 9: Payment Method Market Share

<img width="912" height="829" alt="image" src="https://github.com/user-attachments/assets/7d080202-9b39-4f50-b136-334a78966574" />
        Description: Visualizes the percentage of customers using different payment methods (Credit Card, E-wallet, etc.), indicating customer trust and payment convenience.<br>

Chart 10: Total Order Count per Category

<img width="1089" height="708" alt="image" src="https://github.com/user-attachments/assets/13276e98-cf6a-4881-a1cf-7f7d160c8ac7" />
        Description: A summary of overall demand across all product types in the dataset.

Technologies Used

    Language: Python
    Engine: Apache Spark (PySpark)
    Database: Spark SQL
    Visualization: Matplotlib & Seaborn
    Data Handling: Pandas (for final plotting)
