#!/usr/bin/env python3

# Importing libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
import psycopg2 as psy
import os 
from dotenv import load_dotenv
from pyspark.sql import functions as f

# Path to the PostgreSQL JDBC driver
jdbc_driver_path= r'C:\Users\HP SPECTRE\Documents\Data Journey\10Alytics\ETL_Orchestration\class_case_study\New_York_Taxi_Trip_Record\postgresql-42.7.3.jar'

#Setup Spark Session
spark= SparkSession.builder.appName('NY Trip Data') \
                   .config("spark.jars" , jdbc_driver_path) \
                   .config ("spark.executor.memory", "4g") \
                   .getOrCreate()


file_path= r"C:\Users\HP SPECTRE\Documents\Data Journey\10Alytics\ETL_Orchestration\class_case_study\yellow_tripdata_2009-01DATASET.parquet"
ny_df=spark.read.option('mode','DROPMALFORMED' ).parquet(file_path)


#taking a subset of our dataset

ny_df_subset=ny_df.limit(20000)

# Checking for duplicate rows
ny_df_subset.groupBy('vendor_name',
 'Trip_Pickup_DateTime',
 'Trip_Dropoff_DateTime',
 'Passenger_Count',
 'Trip_Distance',
 'Start_Lon',
 'Start_Lat',
 'Rate_Code',
 'store_and_forward',
 'End_Lon',
 'End_Lat',
 'Payment_Type',
 'Fare_Amt',
 'surcharge',
 'mta_tax',
 'Tip_Amt',
 'Tolls_Amt',
 'Total_Amt').count().filter('count > 1').show()




#FInding minssing values using list comprehension
null_counts_2= [(column, ny_df_subset.where(f.col(column).isNull()).count()) for column in ny_df_subset.columns]

#Getting columns to drop
columns_to_drop = [column for column, count in null_counts_2 if count > 0.1 * ny_df_subset.count()]

#Droping columns with more than 10% nulls
ny_df_subset= ny_df_subset.drop(*columns_to_drop)

#Data transformation - filtering invalid datasets
ny_df_subset=ny_df_subset.filter(
    (f.col('Passenger_Count') > 0.0) &
    (f.col('Trip_Distance') > 0.0) & 
    (f.col("Fare_Amt") > 0.0) &
    (f.col("Total_Amt") > 0.0) & 
    (f.col("Tip_Amt")>= 0.0) &
    (f.col("Tolls_Amt")>= 0.0) &
    (f.col("surcharge") >= 0.0)
    )

# Define a dictionary for the columns and their desired data types
columns_to_cast = {
    "Trip_Pickup_DateTime": "timestamp",
    "Trip_Dropoff_DateTime": "timestamp",
    "Passenger_Count": "integer",
}


# In[22]:


#Iterate over the disctionary and cast the columns
for col_name, col_type in columns_to_cast.items():
    ny_df_subset= ny_df_subset.withColumn(col_name, f.col(col_name).cast(col_type))


# Fixing payment_type column
ny_df_subset= ny_df_subset.withColumn("Payment_Type", 
                                      when(f.col('Payment_Type')== 'CASH', 'Cash')
                                      .when(f.col('Payment_Type') == 'CREDIT', 'Credit')
                                      .otherwise(f.col('Payment_Type'))
                                    )

# Creating Different Tables


vendors= ny_df_subset.select('vendor_name') \
                    .withColumn('vendor_id', monotonically_increasing_id()+1) \
                    .select('vendor_id', 'vendor_name')

payments= ny_df_subset.select('Payment_Type', 'Fare_Amt', 'surcharge', 'Tip_Amt', 'Tolls_Amt', 'Total_Amt') \
                      .withColumn('payment_id', monotonically_increasing_id()+1) \
                      .select('payment_id','Payment_Type', 'Fare_Amt', 'surcharge', 'Tip_Amt', 'Tolls_Amt', 'Total_Amt' )

#Locations table
locations= ny_df_subset.select('Start_Lon', 'Start_Lat', 'End_Lon', 'End_Lat') \
                       .withColumn('location_id', monotonically_increasing_id()+1) \
                       .select('location_id','Start_Lon', 'Start_Lat', 'End_Lon', 'End_Lat')

#Trip Table
trips= ny_df_subset.withColumn('trip_id',monotonically_increasing_id()+1 )

trips= trips.join(vendors, trips.trip_id==vendors.vendor_id, 'left')\
            .join(payments,trips.trip_id==payments.payment_id, 'left' )\
            .join (locations,trips.trip_id==locations.location_id, 'left' ) \
            .select ('trip_id','Trip_Pickup_DateTime','Trip_Dropoff_DateTime','Passenger_Count',
                     'Trip_Distance','vendor_id','payment_id', 'location_id', )

print('Dataframes created succesfully')

#Creating an environment for password
load_dotenv()

password= os.getenv('PASSWORD')


# CREATING CONNECTION TO DATABASE
def get_connection():
    connection= psy.connect(dbname='ny_taxi_trips', user='postgres', password=password, host='localhost', port=5432)
    return connection

conn=get_connection()
cur=conn.cursor()

#Create tables in database
conn= get_connection()
cur=conn.cursor()

create_table_query= '''
                    DROP TABLE IF EXISTS vendors CASCADE;
                    DROP TABLE IF EXISTS payments CASCADE;
                    DROP TABLE IF EXISTS locations CASCADE;
                    DROP TABLE IF EXISTS trips CASCADE;

                    CREATE TABLE IF NOT EXISTS vendors (
                        vendor_id INT PRIMARY KEY,
                        vendor_name VARCHAR(50)
                    );

                    CREATE TABLE locations (
                        location_id INT PRIMARY KEY,
                        start_lon FLOAT,
                        start_lat FLOAT,
                        end_lon FLOAT,
                        end_lat FLOAT
                    );

                    CREATE TABLE payments (
                        payment_id INT PRIMARY KEY,
                        payment_type VARCHAR(50),
                        fare_amt FLOAT,
                        surcharge FLOAT,
                        tip_amt FLOAT,
                        tolls_amt FLOAT,
                        total_amt FLOAT
                    );

                    CREATE TABLE trips (
                        trip_id INT PRIMARY KEY,
                        trip_pickup_datetime TIMESTAMP,
                        trip_dropoff_datetime TIMESTAMP,
                        passenger_count INT,
                        trip_distance FLOAT,
                        vendor_id INT,
                        payment_id INT,
                        location_id INT,
                        FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
                        FOREIGN KEY (payment_id) REFERENCES payments(payment_id),
                        FOREIGN KEY (location_id) REFERENCES locations(location_id)
                       );

    '''

cur.execute(create_table_query)

conn.commit()

# Loading Data Into Postgresql Database Using JDBC Driver

# PostgreSQL JDBC URL and properties
jdbc_url = "jdbc:postgresql://localhost:5432/ny_taxi_trips"
jdbc_properties = {"user": "postgres", "password": password, "driver":"org.postgresql.Driver"}

dataframes_to_save = [
    (vendors, "vendors"),
    (locations, "locations"),
    (payments, "payments"),
    (trips, "trips")
]

for dataframe, table_name in dataframes_to_save:
    writer=dataframe.write \
    .format('jdbc') \
    .option ('url', jdbc_url) \
    .option ('dbtable', table_name) \
    
    # Set each property individually
    for key, value in jdbc_properties.items():
        writer_incl_properties = writer.option(key, value)

    writer_incl_properties.mode('append').save()

conn.close()
print("Data saved successfully!")
spark.stop()






