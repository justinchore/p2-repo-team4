from datetime import datetime
# from itertools import count
# from sqlite3 import Timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, TimestampType
from pyspark.sql.functions import col, abs, expr, desc, max, lit, date_format, split, to_timestamp, to_date, hour, count, unix_timestamp, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import split, date_format
import pyspark.sql.functions as F
spark = SparkSession.builder.master("local")\
        .appName('p2_analysis')\
        .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

#Read file with inferred Schema:
#Comment out others and run your csv file initialization here:

#JUSTINS SPARK.READ:
orders_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("file:/home/jcho/project_2/p2_Team2_Data.csv")

#JORDANS SPARK.READ:

#ADETUNJIS SPARK.READ:

#ANDREWS SPARK.READ:

#NILESHS SPARK.READ:
    
'''
What is the top selling category of items? Per Country? JORDAN

How does the popularity of products change throughout 
the year? Per country? ANDREW

Which locations see the highest traffic of sales? Nilesh

What times have the highest traffic of sales? Per country? Justin./Adetunji

'''
###FILTERS AND GLOBAL VARIABLES#####
txn_success_filter = "payment_txn_success in ('Y', 'N')"
txn_id_regex = "(?i)[a-z]{2}\-[0-9]{6}"
valid_states = ['South Carolina','Mississippi','Virginia','West Virginia','kentucky','Alabama','North Carolina','Arkansas','Louisiana','Tennessee','Florida','Georgia','Hawaii']
valid_categories = ['Nissan', 'Toyota', 'Honda', 'Ford', 'Chevrolet', 'Jeep', 'Tesla', 'GMC', 'Hyundai', 'Ram', 'Mazda', 'Subaru', 'Pontiac', 'Wrangler']

#MAP FUNCTION FOR RDD:
def rdd_date_format(r): 
    if r[9] != None:
        #Date string validator
        
        checker = date_checker(r[9])
        if checker == 0:
            return r
        split_date = r[9].split(" ")
        
        date_string = split_date[0]
        time_string = split_date[1]
        if ("/" in date_string and int(date_string.split('/')[0]) < 10):
            date_string = "0"+ date_string
        if ("-" in date_string):
            date_string = date_string.replace("-", "/")
            
        date_year = date_string.split('/')[2]
        
        if (date_year != "2021"):
            date_string.replace(date_year, "2021")
            
        final_datetime = date_string + ' ' + time_string
        return (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], final_datetime, r[10], r[11], r[12], r[13], r[14], r[15])
    
    return (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], None, r[10], r[11], r[12], r[13], r[14], r[15])


#DATE CHECKER FUNCTION TO VALIDATE (WE CAN ADD MORE FORMATS HERE)
def date_checker(s):
    formats = ["%m/%d/%Y %H:%M", "%m-%d-%Y %H:%M"]
    result = 0
    for n in range(0, len(formats)):
        try:
            date_conv = datetime.strptime(s, formats[n])
            if date_conv != None:
                result = 1
        except ValueError:
            continue
        
    
    return result


#Returns rows with formatted date    
date_formatted_orders_rdd = orders_df.rdd.map(lambda x : rdd_date_format(x))
#Save RDD into memory
date_formatted_orders_rdd.cache()
#Create DataFrame
orders_df_w_date = spark.createDataFrame(date_formatted_orders_rdd, orders_df.schema)
#Apply filters (feel free to add more here)
clean_DF = orders_df_w_date.filter(txn_success_filter)\
    .filter(col("payment_txn_id")\
    .rlike(txn_id_regex))\
    .filter(col("country(state)").isin(valid_states))\
    .filter(col("product_category").isin(valid_categories))\
    .cache()       
clean_DF.show()

#############START QUERIES HERE################## cd into Analysis_PT2 to run!

####JORDAN######
def q1(df):
    df.createOrReplaceTempView("data")
    spark.sql("SELECT SUM(Quantity) as numSold, Country, ProductCategory FROM data GROUP BY Country, ProductCategory").toDF("numSold", "Country", "ProductCategory").createOrReplaceTempView("temp")
    spark.sql("SELECT MAX(numSold), Country FROM temp GROUP BY Country").toDF("numSold", "Country").createOrReplaceTempView("temp2")
    print(spark.sql("SELECT temp2.Country, temp.ProductCategory, temp2.numSold FROM temp2 LEFT JOIN temp ON temp2.numSold=temp.numSold ORDER BY numSold").show(500))
    
    
#ALL QUERIES FOR ANDREW MVP QUESTIONS + EXTRA###
clean_DF.createOrReplaceTempView("Data")
clean_DF2 = spark.sql("SELECT datetime,product_category,product_name,quantity,state FROM Data")
#df2 is our split between date and time, we select date at index [0]
clean_DF3 = clean_DF2.withColumn('date', split("datetime", " ")[0])
clean_DF3.createOrReplaceTempView("date_category")
clean_DF4= spark.sql("SELECT date,product_category,quantity,state,product_name FROM date_category")
clean_DF4_transformed = clean_DF4.withColumn('new_date',F.to_date(F.unix_timestamp('date', 'MM/dd/yyyy').cast('timestamp')))
clean_DF4_transformed.createOrReplaceTempView("date_category_count")
#This data lists the top sales by year by category
q1DF=spark.sql("SELECT state,product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-01-01' AND '2021-03-31' AND state=='West Virginia' GROUP BY state,product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()

#q1DF=spark.sql("SELECT product_category AS Brand,COUNT(product_category) AS BrandOrders,SUM(quantity) AS TotalProducstSold FROM date_category_count WHERE new_date BETWEEN '2021-01-01' AND '2021-12-31' GROUP BY product_category ORDER BY SUM(quantity) DESC")
#This data lists the most popular product of the fiscal year 2021
#q1DF=spark.sql("SELECT product_category AS Brand,product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-01-01' AND '2021-12-31' GROUP BY product_category,product_name ORDER BY SUM(quantity) DESC")
#Overall
q1DF=spark.sql("SELECT product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-01-01' AND '2021-03-31' GROUP BY product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q2DF=spark.sql("SELECT product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-04-01' AND '2021-06-30' GROUP BY product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q3DF=spark.sql("SELECT product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-07-01' AND '2021-09-30' GROUP BY product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q4DF=spark.sql("SELECT product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-10-01' AND '2021-12-31' GROUP BY product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
#By State
q1DFS=spark.sql("SELECT state,product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-01-01' AND '2021-03-31' GROUP BY state,product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q2DFS=spark.sql("SELECT state,product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-04-01' AND '2021-06-30' GROUP BY state,product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q3DFS=spark.sql("SELECT state,product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-07-01' AND '2021-09-30' GROUP BY state,product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()
q4DFS=spark.sql("SELECT state,product_category AS Brand, product_name AS Model,COUNT(product_name) AS Orders,SUM(quantity) AS TotalProductsSold FROM date_category_count WHERE new_date BETWEEN '2021-10-01' AND '2021-12-31' GROUP BY state,product_category,product_name ORDER BY SUM(quantity) DESC LIMIT 5").show()

####JUSTINS#######

#What times have the highest traffic of sales

#split string step
id_time_df = clean_DF.withColumn('time', split("datetime", " ")[1])\
    .select('order_id', "time")

#make into timestamp, add dummy "date" 
formatted_time_df = id_time_df.withColumn("timestamp",  date_format(col("time"), 'HH:mm')\
    .cast(TimestampType()))\
    .select("order_id", "timestamp")

# formatted_time_df.printSchema()
# formatted_time_df.show()

formatted_time_df.groupBy(hour("timestamp").alias("hour"))\
    .agg(count("order_id").alias('order_traffic'))\
    .sort('hour')\
    .show()    