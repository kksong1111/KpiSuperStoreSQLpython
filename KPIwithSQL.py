# pip install --upgrade google-cloud-bigquery

import os
from google.cloud import bigquery

# This script uses Google Bigquery
# The results will be both printed on terminal and written to query_results.txt

# Setting cloud key to system environment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './cloud_credentials.json'

client = bigquery.Client()

output_file = "query_results.txt"

# Open file in write mode
with open(output_file, "w") as f:

    # Query total sales
    query_statement = """
    SELECT 
    SUM(Sales) AS Total_Sales
    FROM 
    `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`;
    """

    query_total_sales = client.query(query_statement)

    for row in query_total_sales.result():
        total_sales = round(row.Total_Sales, 2)

    print(f"Total sales is ${total_sales}\n")
    f.write(f"Total sales is ${total_sales} \n\n")


    # Query total profit
    query_statement = """
    SELECT 
    SUM(Profit) AS Total_Profit
    FROM 
    `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`;
    """

    query_total_profit = client.query(query_statement)

    for row in query_total_profit.result():
        total_profit = round(row.Total_Profit, 2)


    print(f"Total profit is ${total_profit}\n")
    f.write(f"Total profit is ${total_profit} \n\n")

    
    # Query profit margin
    query_statement = """
    SELECT 
    (SUM(Profit) / SUM(Sales)) * 100 AS Profit_Margin_Percentage
    FROM 
    `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`;
    """

    query_profit_margin = client.query(query_statement)

    for row in query_profit_margin.result():
        profit_margin = round(row.Profit_Margin_Percentage, 2)

    print(f"Profit margin is {profit_margin}%\n")
    f.write(f"Profit margin is {profit_margin}% \n\n")


    # Query total sales in each product category
    query_statement = """
    SELECT 
    Category,
    SUM(Sales) AS Total_Sales
    FROM 
    `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`
    GROUP BY 
    Category
    ORDER BY 
    Total_Sales DESC;
    """

    query_categories_sales = client.query(query_statement)

    categories_sales = []
    for row in query_categories_sales.result():
        categories_sales.append((row.Category, f"${round(row.Total_Sales, 2)}"))

    print(f"Sales for each category are \n {categories_sales}\n")
    f.write(f"Sales for each category are \n {categories_sales} \n\n")


    # Query the top 5 customers
    query_statement = """
    SELECT 
    `Customer Name` as Customer_name,
    round(SUM(Sales),2) AS Total_Sales
    FROM 
    `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`
    GROUP BY 
    Customer_name
    ORDER BY 
    Total_Sales DESC
    LIMIT 5;
    """

    query_top_customers = client.query(query_statement)

    top_customers = []
    print("The top 5 customers are ")
    f.write("The top 5 customers are \n")
    for row in query_top_customers.result():
        top_customers.append((row.Customer_name, f"${row.Total_Sales}"))
        print((row.Customer_name, f"${row.Total_Sales}"))
        f.write(str((row.Customer_name, f"${row.Total_Sales}")) + "\n")
    print("")
    f.write("\n")


    # Query the total sales in each region
    query_statement = """
    SELECT 
        Region, 
        ROUND(SUM(Profit),2 ) AS total_profit
    FROM `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`
    GROUP BY Region
    ORDER BY total_profit DESC;
    """

    query_region_profit = client.query(query_statement)

    region_profit = []
    print(f"Profit for each region is")
    f.write(f"Profit for each region is \n")
    for row in query_region_profit.result():
        region_profit.append((row.Region, f"${row.total_profit}"))
        print((row.Region, f"${row.total_profit}"))
        f.write(str((row.Region, f"${row.total_profit}")))
    f.write("\n\n")


    # Query monthly sales
    query_statement = """
    SELECT 
        EXTRACT(YEAR FROM `Order Date`) AS year,
        EXTRACT(MONTH FROM `Order Date`) AS month,
        ROUND(SUM(Sales), 2) AS total_sales
    FROM `modern-tangent-442709-a7.TableauSuperStoreKPI.SuperStore`
    GROUP BY year, month
    ORDER BY year, month;
    """

    query_monthly_sales = client.query(query_statement)

    monthly_sales = []
    print("\nSales for each month is ")
    f.write("Sales for each month is \n")
    for row in query_monthly_sales.result():
        monthly_sales.append((row.year, row.month, f"${row.total_sales}"))
        print((row.year, row.month, f"${row.total_sales}"))
        f.write(str((row.year, row.month, f"${row.total_sales}")) + "\n")