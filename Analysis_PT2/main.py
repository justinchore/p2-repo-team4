from datetime import datetime
# from itertools import count
# from sqlite3 import Timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, TimestampType
from pyspark.sql.functions import col, abs, expr, desc, max, lit, date_format, split, to_timestamp, to_date, hour, count, unix_timestamp, coalesce
from pyspark.sql.window import Window

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

