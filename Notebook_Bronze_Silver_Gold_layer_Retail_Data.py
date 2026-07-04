#!/usr/bin/env python
# coding: utf-8

# ## Notebook_Bronze_Silver_Gold_layer_Retail_Data
# 
# null

# #Read bronze layer data and create data frame

# In[2]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
df_orders_raw = spark.read.parquet ("abfss://963c34b2-fed7-4297-a704-b35b65dea6ce@onelake.dfs.fabric.microsoft.com/0303a2ca-3928-4c82-8723-fc85690f240d/Files/Bronze_layer/Orders_data.parquet")
df_returns_raw = spark.read.parquet("abfss://963c34b2-fed7-4297-a704-b35b65dea6ce@onelake.dfs.fabric.microsoft.com/0303a2ca-3928-4c82-8723-fc85690f240d/Files/Bronze_layer/Return_data.parquet")
df_inventory_raw = spark.read.parquet("abfss://963c34b2-fed7-4297-a704-b35b65dea6ce@onelake.dfs.fabric.microsoft.com/0303a2ca-3928-4c82-8723-fc85690f240d/Files/Bronze_layer/Inventory_data.parquet")


# In[3]:


display(df_orders_raw)


# hanlde first raw as header

# In[4]:


# Extract first row as header
first_row = df_returns_raw.first()
columns = [str(item).strip() for item in first_row]
# Remove the first row (header row now part of data)
df_returns_raw = df_returns_raw.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]).toDF (columns)
# Show cleaned data
display(df_returns_raw)


# Creating a bronze delta table

# In[5]:


df_orders_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_orders")
df_returns_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_returns")
df_inventory_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_inventory")


# Clean data silver layer preparation

# In[6]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

df_orders = (
    df_orders_raw
    # 2. Clean column names (Fixed to match exact raw schema names with underscores)
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("cust_id", "CustomerID")
    .withColumnRenamed("Product_Name", "ProductName")
    .withColumnRenamed("Qty", "Quantity")
    .withColumnRenamed("Order_Date", "OrderDate")
    .withColumnRenamed("Order_Amount$", "OrderAmount")  # Fixed space to underscore
    .withColumnRenamed("Delivery_Status", "DeliveryStatus")  # Fixed space to underscore
    .withColumnRenamed("Payment_Mode", "PaymentMode")  # Fixed space to underscore
    .withColumnRenamed("Ship_Address", "ShipAddress")
    .withColumnRenamed("Promo_Code", "PromoCode")  # Fixed space to underscore
    .withColumnRenamed("Feedback_Score", "FeedbackScore")
    
    # 3. Normalize Quantity: convert words like 'one', 'Two' to integer
    .withColumn("Quantity",
        when(lower(col("Quantity")) == "one", 1)
        .when(lower(col("Quantity")) == "two", 2)
        .when(lower(col("Quantity")) == "three", 3)
        .otherwise(col("Quantity").cast(IntegerType()))
    )
    
    # 4. Standardize date format using multiple patterns
    .withColumn("OrderDate", to_date(
        coalesce(
            to_date(col("OrderDate"), "yyyy/MM/dd"),
            to_date(col("OrderDate"), "dd-MM-yyyy"),
            to_date(col("OrderDate"), "MM-dd-yyyy"),
            to_date(col("OrderDate"), "yyyy.MM.dd"),
            to_date(col("OrderDate"), "dd/MM/yyyy"),
            to_date(col("OrderDate"), "dd.MM.yyyy"),
            to_date(col("OrderDate"), "MMMM dd yyyy")
        )
    ))
    
    # 5. Clean and convert OrderAmount
    .withColumn("OrderAmount", regexp_replace(col("OrderAmount"), "[$*RS. USD, INR]", ""))
    .withColumn("OrderAmount", col("OrderAmount").cast(DoubleType()))
    
    # 6. Standardize PaymentMode
    .withColumn("PaymentMode", lower(regexp_replace(col("PaymentMode"), "[^a-zA-Z]", "")))
    
    # 7. Standardize DeliveryStatus
    .withColumn("DeliveryStatus", lower(regexp_replace(col("DeliveryStatus"), "[^a-zA-Z]", "")))
    
    # 8. Validate email using simple regex pattern
    .withColumn("Email", when(col("Email").rlike("^[A-Za-z0-9. %+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), col("Email")).otherwise(None))
    
    # 9. Clean address: remove special characters like #, !, $, @ etc.
    .withColumn("ShipAddress", regexp_replace(col("ShipAddress"), r" [%@!$]", ""))
    
    # 10. FeedbackScore: convert to float, handle NaN/bad values
    .withColumn("FeedbackScore", col("FeedbackScore").cast(DoubleType()))
    
    # 11. Fill nulls where possible
    .fillna({"Quantity": 0, "OrderAmount": 0.0, "DeliveryStatus": "unknown", "PaymentMode": "unknown"})
    
    # 12. Drop rows with no CustomerID or ProductName
    .na.drop(subset=["CustomerID", "ProductName"])
    
    # 13. Remove duplicates by OrderID
    .dropDuplicates(["OrderID"])
)

# Display to verify
display(df_orders)

#save cleaned orders table in silver table
df_orders.write.mode("overwrite").format("delta").saveAsTable("silver_orders")


# creating silver inventory table

# In[18]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

df_inventory = (
    df_inventory_raw
    # 1. Clean column names (Fixed "LastStocked" to have no space)
    .withColumnRenamed("productName", "ProductName")
    .withColumnRenamed("cost_price", "CostPrice")
    .withColumnRenamed("last_stocked", "LastStocked") 
    
    # 2. Clean stock column: convert to integer
    .withColumn("Stock",
        when(col("stock").rlike("^[0-9]+$"), col("stock").cast(IntegerType()))
        .when(col("stock").isNull() | (col("stock") == ""), lit(None))
        .otherwise(
            when(col("stock").rlike(".*twenty five.*"), lit(25))
            .when(col("stock").rlike(".*twenty.*"), lit(20))
            .when(col("stock").rlike(".*eighteen.*"), lit(18))
            .when(col("stock").rlike(".*fifteen.*"), lit(15))
            .when(col("stock").rlike(".*twelve.*"), lit(12))
            .otherwise(lit(None))
        ).cast(IntegerType())
    )
    
    # 3. Clean LastStocked: normalize multiple date formats to yyyy-MM-dd (Fixed the missing closing brackets here)
    .withColumn("LastStocked", to_date(
        coalesce(
            to_date(col("LastStocked"), "yyyy-MM-dd"),
            to_date(col("LastStocked"), "yyyy/MM/dd")
        )
    ))
    
    # 4. Clean Cost Price: extract numeric value and convert to float
    .withColumn("CostPrice", regexp_extract(col("CostPrice"), r"(\d+\.?\d*)", 1).cast(DoubleType()))
    
    # 5. Clean Warehouse: remove special characters, trim, capitalize
    .withColumn("Warehouse", initcap(trim(regexp_replace(col("warehouse"), r"[^a-zA-Z0-9\s]", ""))))
    
    # 6. Standardize Available: convert to boolean
    .withColumn("Available",
        when(lower(col("available")).isin("yes", "y", "true"), lit(True))
        .when(lower(col("available")).isin("no", "n", "false"), lit(False))
        .otherwise(lit(None))
    )
    
    # 7. Drop raw messy lower-case columns that were replaced
    #.drop("stock", "warehouse", "available")
)

# Display the cleaned data
display(df_inventory)

# Save to Silver Layer with overwriteSchema to handle any older table structure
df_inventory.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_inventory")


# Creating silver return table

# In[17]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

df_returns = (
    df_returns_raw
    # 2.1 Standardize column names
    .withColumnRenamed("Return_ID", "ReturnID")
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("Customer_ID", "CustomerID")
    .withColumnRenamed("Return_Reason", "ReturnReason")
    .withColumnRenamed("Return_Date", "ReturnDate")
    .withColumnRenamed("Refund_Status", "RefundStatus")
    .withColumnRenamed("Pickup_Address", "PickupAddress")
    .withColumnRenamed("Return_Amount", "ReturnAmount")
    
    # 2.2 Clean ReturnDate standardize date formats
    .withColumn("ReturnDate", to_date(
        coalesce(
            to_date(col("ReturnDate"), "dd-MM-yyyy"),
            to_date(col("ReturnDate"), "yyyy-MM-dd"),
            to_date(col("ReturnDate"), "dd/MM/yyyy"),
            to_date(col("ReturnDate"), "MM/dd/yyyy"),
            to_date(regexp_replace(col("ReturnDate"), r"[\./]", "-"), "dd-MM-yyyy")
        )
    ))
    
    # 2.3 Clean RefundStatus: lower case, remove everything EXCEPT letters
    .withColumn("RefundStatus", lower(trim(regexp_replace(col("RefundStatus"), "[^a-zA-Z\s]", ""))))
    
    # 2.4 Clean ReturnAmount: extract numeric part (Fixed missing closing parenthesis)
    .withColumn("ReturnAmount", regexp_extract(col("ReturnAmount"), r"(\d+\.?\d*)", 1).cast(DoubleType()))
    
    # 2.5 Clean PickupAddress: remove special characters (Fixed regex pattern [^...])
    .withColumn("PickupAddress", initcap(trim(regexp_replace(col("PickupAddress"), r"[^a-zA-Z0-9\s,.-]", ""))))
    
    # 2.6 Clean Product: remove extra symbols and spaces (Fixed regex pattern [^...])
    .withColumn("Product", initcap(trim(regexp_replace(col("Product"), r"[^a-zA-Z0-9\s]", ""))))
    
    # 2.7 Clean CustomerID trim, fix wrong prefixes
    .withColumn("CustomerID", trim(upper(col("CustomerID"))))
    
    # 2.8 Drop rows with null ReturnID
    .filter(col("ReturnID").isNotNull())
)

# Step 3: Show cleaned Silver data (Fixed variable name)
display(df_returns)

# Optional: Save to Silver Layer using overwriteSchema to prevent collisions
df_returns.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_returns")


# creating gold aggregate table

# In[20]:


from pyspark.sql import functions as F

# 1. Load your clean Silver layer tables
df_orders = spark.table("silver_orders")
df_inventory = spark.table("silver_inventory")
df_returns = spark.table("silver_returns")

# 2. Left join starting from orders to capture sales profiles
df_gold_joined = (
    df_orders
    .join(df_inventory, on="ProductName", how="left")
    .join(df_returns, on=["OrderID", "CustomerID"], how="left")
)

# 3. Aggregate KPIs grouped by ProductName to match image_dc9e0f.png
df_product_kpis = (
    df_gold_joined
    .groupBy("ProductName")
    .agg(
        # 1. Total Orders
        F.countDistinct("OrderID").alias("Total_Orders"),
        
        # 2. Unique Number of Customers
        F.countDistinct("CustomerID").alias("Unique_Customers"),
        
        # 3. Total Returns
        F.count("ReturnID").alias("Total_Returns"),
        
        # 4. Return Rate % (Matches 'Return_Rate_%' in image)
        F.round((F.count("ReturnID") / F.countDistinct("OrderID")) * 100, 2).alias("Return_Rate_%"),
        
        # 5. Total Revenue
        F.round(F.sum(
            F.when(F.col("ReturnID").isNull(), F.col("OrderAmount")).otherwise(0)
        ), 2).alias("Total_Revenue"),
        
        # 6. Average Order Value (Matches 'Avg_Order_Value' in image)
        F.round(F.avg("OrderAmount"), 2).alias("Avg_Order_Value"),
        
        # 7. Total Stock (Matches 'Total_Stock' in image)
        F.sum("Stock").alias("Total_Stock"),
        
        # 8. Average Cost (Matches 'Avg_Cost' in image)
        F.round(F.avg("CostPrice"), 2).alias("Avg_Cost"),
        
        # 9. Net Profit
        F.round(F.sum(
            F.when(F.col("ReturnID").isNull(), F.col("OrderAmount") - (F.col("Quantity") * F.col("CostPrice"))).otherwise(0)
        ), 2).alias("Net_Profit")
    )
)

# Display the aggregated product-level metrics
display(df_product_kpis)

# Save to your Gold layer
#df_product_kpis.write \
    #.mode("overwrite") \
    #.option("overwriteSchema", "true") \
    #.format("delta") \
   #.saveAsTable("gold_product_kpis")


# In[1]:


from pyspark.sql import functions as F

# Load the clean Silver tables (or use your existing DataFrames)
df_orders = spark.table("silver_orders")
df_inventory = spark.table("silver_inventory")
df_returns = spark.table("silver_returns")

# ==========================================
# STEP 1: JOIN THE THREE TABLES
# ==========================================
# We use a Left Join starting from Orders to keep all transactions, 
# and bring in Product specs and Return details if they exist.
df_gold_joined = (
    df_orders
    .join(df_inventory, on="ProductName", how="left")
    .join(df_returns, on=["OrderID", "CustomerID"], how="left")
)

# Cache it or display it to verify the master view
# display(df_gold_joined)


# ==========================================
# STEP 2: CREATE AGGREGATED BUSINESS KPIs
# ==========================================
df_business_kpis = df_gold_joined.select(
    # 1. Total Orders (Counting unique orders)
    F.countDistinct("OrderID").alias("Total_Orders"),
    
    # 2. Unique Number of Customers
    F.countDistinct("CustomerID").alias("Unique_Customers"),
    
    # 3. Total Returns (Counting valid return IDs)
    F.count("ReturnID").alias("Total_Returns"),
    
    # 4. Return Rate % (Total Returns divided by Total Orders * 100)
    F.round((F.count("ReturnID") / F.countDistinct("OrderID")) * 100, 2).alias("Return_Rate_Percent"),
    
    # 5. Total Revenue (Sum of OrderAmount for orders that weren't returned/refunded)
    F.round(F.sum(
        F.when(F.col("ReturnID").isNull(), F.col("OrderAmount")).otherwise(0)
    ), 2).alias("Total_Revenue"),
    
    # 6. Average Order Value (AOV)
    F.round(F.avg("OrderAmount"), 2).alias("Average_Order_Value"),
    
    # 7. Total Stock (Sum of current inventory)
    F.lit(df_inventory.agg(F.sum("Stock")).first()[0]).alias("Total_Stock"),
    
    # 8. Average Cost (Average cost price of items in inventory)
    F.lit(df_inventory.agg(F.round(F.avg("CostPrice"), 2)).first()[0]).alias("Average_Cost"),
    
    # 9. Net Profit (Total Revenue minus Total Cost of Goods Sold for non-returned items)
    F.round(F.sum(
        F.when(F.col("ReturnID").isNull(), F.col("OrderAmount") - (F.col("Quantity") * F.col("CostPrice"))).otherwise(0)
    ), 2).alias("Net_Profit")
)

# Display the aggregated business dashboard results
display(df_business_kpis)

# ==========================================
# STEP 3: SAVE TO GOLD LAYER
# ==========================================
df_business_kpis.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("gold_business_kpis")

