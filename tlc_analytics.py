import getpass
import os
from pandas import DataFrame
from datetime import datetime, time

import sys
sys.path.append("/home/ra-terminal/spark-3.5.0-bin-hadoop3/python")
sys.path.append("/home/ra-terminal/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, round as spark_round, stddev, avg, col, count, mode, to_timestamp, date_format, to_date
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, StructField, DecimalType

username = getpass.getuser()

os.environ["SPARK_HOME"] = "/home/ra-terminal/spark-3.5.0-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.9"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.9"

yellow_taxi_trip_recs = f'/home/{username}/datasets/nyc_tlc/2024/yellow_trip_taxi_records/yellow_tripdata_2024-01.parquet'
green_taxi_trip_recs = f'/home/{username}/datasets/nyc_tlc/2024/green_trip_taxi_records/green_tripdata_2024-01.parquet'
#for_hire_vehicle_trip_recs = f'/home/{username}/datasets/nyc_tlc/2024/for_hire_vehicle_trip_records/fhv_tripdata_2024-01.parquet'
#high_volume_for_hire_vehicle_trip_recs = f'/home/{username}/datasets/nyc_tlc/2024/high_volume_for_hire_vehicle_trip_records/fhvhv_tripdata_2024-01.parquet'

spark = SparkSession.builder \
            .appName("nyc_tlc_project") \
            .master("local") \
            .config('spark.ui.port', '4040') \
            .getOrCreate()

# yellow_trip_recs = pd.read_parquet(yellow_taxi_trip_recs)
# green_trip_recs = pd.read_parquet(green_taxi_trip_recs)
# fhv_trip_recs = pd.read_parquet(for_hire_vehicle_trip_recs)
# hvfhv_trip_recs = pd.read_parquet(high_volume_for_hire_vehicle_trip_recs)

# print(yellow_trip_recs.head())

yellow_trip_recs = spark.read.parquet(yellow_taxi_trip_recs)
green_trip_recs = spark.read.parquet(green_taxi_trip_recs)
#fhv_trip_recs = spark.read.parquet(for_hire_vehicle_trip_recs)
#hvfhv_trip_recs = spark.read.parquet(high_volume_for_hire_vehicle_trip_recs)

yellow_trip_recs_df = yellow_trip_recs.limit(1000000).toPandas()
# yellow_trip_recs_df = yellow_trip_recs.toPandas()
df: DataFrame = yellow_trip_recs_df

# green_trip_recs_df = green_trip_recs.limit(100000).toPandas()
# df: DataFrame = green_trip_recs_df

# fhv_trip_recs_df = fhv_trip_recs.limit(100000).toPandas()
# df: DataFrame = fhv_trip_recs_df

# hvfhv_trip_recs_df = hvfhv_trip_recs.limit(100000).toPandas()
# df: DataFrame = hvfhv_trip_recs_df

#light preprocessing

def cols_to_lower(df):
    for col in df.columns:
        df=df.withColumnRenamed(col, col.lower())
    return df

def remove_pass_count_0(df):
    return df.filter((col("passenger_count").isNotNull()) & (col("passenger_count") != 0))

def remove_neg_tips(df):
    return df.filter((col('tip_amount') < 0))

def rename_payment_type_col(val):
    payment_type_dict = {
        0: 'Flex Fare trip',
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    return payment_type_dict.get(val, 'Other')

def rename_ratecodeid_col(val):
    ratecodeid_dict = {
        1: 'stardard_rate',
        2: 'jfk',
        3: 'newark',
        4: 'nassau or westchester',
        5: 'negotiated fare',
        6: 'group ride',
        99: 'null/unknown'
    }
    return ratecodeid_dict.get(val, 'Other')

def add_category_ride_time(day_val, time_val):
    '''
    Weekdays:
    - 7am - 10am = weekday morning rush hour
    - 4pm - 7pm = weekday afternoon rush hour
    - 10pm - 2am = weekday evening rush hour
    Weekends(Friday - Saturday):
    - 6pm - 2am = weekend evening rush hour
    - other: all times not around given rush hour times
    '''
    
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekend = ['Friday', 'Saturday', 'Sunday']

    #convert time_val to 24hr format
    try:
        time_obj24 = datetime.strptime(time_val, "%I:%M %p").time()
    except Exception:
        return 'other'

    if day_val in weekdays:
        if time(7,0) <= time_obj24 <= time(10, 0):
            return 'weekday morning rush hour'
        elif time(16,0) <= time_obj24 <= time(19, 0):
            return 'weekday afternoon rush hour'
        elif time_obj24 >= time(22, 0) or time_obj24 <= time(2, 0):
            return 'weekday evening rush hour'
        else:
            return 'other'
    elif day_val in weekend:
        if time_obj24 >= time(18, 0) or time_obj24 <= time(2, 0):
            return 'weekend evening rush hour'
        else:
            return 'other'


# Calculations

def tip_on_fare_pct(df):
    df = df.withColumn('tip_on_fare_pct',(df['tip_amount'] / df['fare_amount']) * 100)
    df = df.withColumn("tip_on_fare_pct", spark_round(df["tip_on_fare_pct"], 2))
    # df = df.withColumn("tip_on_fare_pct", df["tip_on_fare_pct"].cast(DecimalType(10, 2)))
    return df

def pass_num_on_tip_on_fare_summary(df):
    '''
    Checking average tip and trip count by passenger count
    '''
    groupby_pass_num_df = df.groupBy('passenger_count').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)')
    )
    return groupby_pass_num_df

def pay_type_on_tip_fare_avg_summary(df):
    '''
    checking average tip and fare by device type
    '''
    groupby_payment_type_df = df.groupby('payment_type').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)')
        )
    return groupby_payment_type_df

def groupby_ride_time_and_pay_type_summary(df):
    groupby_ridetime_paytype_df = df.groupby('category_ride_time', 'payment_type').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)')
    )
    return groupby_ridetime_paytype_df

def groupby_ride_time_and_passenger_number_summary(df):
    groupby_ridetime_passenger_number_df = df.groupby('category_ride_time','passenger_count').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)')
    )
    return groupby_ridetime_passenger_number_df

#udf 
#great for column based operations only, not row
#udf assignment =         custom function name,        casting post variable type 
rename_payment_type_udf = udf(rename_payment_type_col, StringType())
rename_ratecodeid_udf = udf (rename_ratecodeid_col, StringType())
ride_time_udf = udf(add_category_ride_time, StringType())

#Preprocessing Steps
yellow_trip_recs = cols_to_lower(yellow_trip_recs)
yellow_trip_recs = yellow_trip_recs.withColumn(
    "payment_type", rename_payment_type_udf("payment_type")
)
yellow_trip_recs = yellow_trip_recs.withColumn(
    'ratecodeid', rename_ratecodeid_udf('ratecodeid')
)
yellow_trip_recs = remove_pass_count_0(yellow_trip_recs)

yellow_trip_recs = yellow_trip_recs.withColumn('trip_date', to_date('tpep_pickup_datetime'))
yellow_trip_recs = yellow_trip_recs.withColumn('start_time', date_format("tpep_pickup_datetime", "hh:mm a"))
yellow_trip_recs = yellow_trip_recs.withColumn('end_time', date_format("tpep_dropoff_datetime", "hh:mm a"))
yellow_trip_recs = yellow_trip_recs.withColumn('day', date_format('trip_date', 'EEEE'))

yellow_trip_recs = yellow_trip_recs.withColumn(
    'category_ride_time',
    ride_time_udf('day', 'start_time')
)

#Transformation Steps
yellow_trip_recs = tip_on_fare_pct(yellow_trip_recs)
payment_type_summary = pay_type_on_tip_fare_avg_summary(yellow_trip_recs)
passenger_num_summary = pass_num_on_tip_on_fare_summary(yellow_trip_recs)
category_ride_time_payment_type_summary = groupby_ride_time_and_pay_type_summary(yellow_trip_recs)
category_ride_time_passenger_num_summary = groupby_ride_time_and_passenger_number_summary(yellow_trip_recs)
#display single groupby
# yellow_trip_recs.show(5)
# payment_type_summary.show()
# passenger_num_summary.show()

# display dual groupby category_ride_time
# category_ride_time_payment_type_summary.toPandas()
category_ride_time_by_payment_type_df = category_ride_time_payment_type_summary.toPandas().head()
category_ride_time_by_passenger_num_df = category_ride_time_passenger_num_summary.toPandas().head()
