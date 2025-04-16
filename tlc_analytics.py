import getpass
import os
from pandas import DataFrame
from datetime import datetime, time
import matplotlib.pyplot as plt
import seaborn as sns

import sys
sys.path.append("/home/ra-terminal/spark-3.5.0-bin-hadoop3/python")
sys.path.append("/home/ra-terminal/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, round as spark_round, stddev, avg, col, count, date_format, to_date
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, StructField, DecimalType

username = getpass.getuser()

os.environ["SPARK_HOME"] = "/home/ra-terminal/spark-3.5.0-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.9"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.9"


yellow_taxi_trip_recs_path = f'/home/{username}/datasets/nyc_tlc/2024/yellow_trip_taxi_records/yellow_tripdata_2024-01.parquet'
# green_taxi_trip_recs_path = f'/home/{username}/datasets/nyc_tlc/2024/green_trip_taxi_records/green_tripdata_2024-01.parquet'
#for_hire_vehicle_trip_recs_path = f'/home/{username}/datasets/nyc_tlc/2024/for_hire_vehicle_trip_records/fhv_tripdata_2024-01.parquet'
#high_volume_for_hire_vehicle_trip_recs_path = f'/home/{username}/datasets/nyc_tlc/2024/high_volume_for_hire_vehicle_trip_records/fhvhv_tripdata_2024-01.parquet'


spark = SparkSession.builder \
            .appName("nyc_tlc_project") \
            .master("local") \
            .config('spark.ui.port', '4040') \
            .config('spark.driver.memory', '2g') \
            .getOrCreate()
# .config('spark.driver.memory', '4g') \

yellow_trip_recs = spark.read.parquet(yellow_taxi_trip_recs_path)
# green_trip_recs = spark.read.parquet(green_taxi_trip_recs_path)
#fhv_trip_recs = spark.read.parquet(for_hire_vehicle_trip_recs_path)
#hvfhv_trip_recs = spark.read.parquet(high_volume_for_hire_vehicle_trip_recs_path)

yellow_trip_recs = yellow_trip_recs.limit(1000000)
# green_trip_recs_df = green_trip_recs.limit(1000000)
# fhv_trip_recs_df = fhv_trip_recs.limit(1000000)
# hvfhv_trip_recs_df = hvfhv_trip_recs.limit(1000000)

#light preprocessing

def cols_to_lower(df):
    for col in df.columns:
        df=df.withColumnRenamed(col, col.lower())
    return df

def remove_pass_count_0(df):
    return df.filter((col("passenger_count").isNotNull()) & (col("passenger_count") != 0))

def remove_neg_numbers(df):
    return df.filter(
        (col('tip_amount') >= 0) &
        (col('fare_amount') >= 0) &
        (col('trip_distance') >= 0) &
        (col('total_amount') >= 0)
    )
#was thinking about removing outliers but after taking a deeper look,
#the trip cost to number of miles seem realistic.
def remove_outliers(df):
    return df.filter(
        (col('tip_amount') <= 100) &
        (col('fare_amount') <= 250) &
        (col('trip_distance') <= 100) &
        (col('total_amount') <= 300)  
    )

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
        avg('trip_distance').alias('avg_trip_dist(mi)'),
        # avg('fare_amount').alias('avg_fare_price'),
        # spark_round((avg('tip_amount') / avg('fare_amount') * 100), 2).alias('avg_tip_pct')
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
        avg('trip_distance').alias('avg_trip_dist(mi)'),
        # avg('fare_amount').alias('avg_fare_price'),
        # spark_round((avg('tip_amount') / avg('fare_amount') * 100), 2).alias('avg_tip_pct')
        )
    return groupby_payment_type_df

def groupby_ride_time_and_pay_type_summary(df):
    groupby_ridetime_paytype_df = df.groupby('category_ride_time', 'payment_type').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)'),
        # avg('fare_amount').alias('avg_fare_price'),
        # spark_round((avg('tip_amount') / avg('fare_amount') * 100), 2).alias('avg_tip_pct')
    )
    return groupby_ridetime_paytype_df

def groupby_ride_time_and_passenger_number_summary(df):
    groupby_ridetime_passenger_number_df = df.groupby('category_ride_time','passenger_count').agg(
        avg('tip_amount').alias('avg_tip($)'),
        count('*').alias('trip_count'),
        stddev('tip_amount').alias('std_dev_tip_amt($)'),
        avg('trip_distance').alias('avg_trip_dist(mi)'),
        # avg('fare_amount').alias('avg_fare_price'),
        # spark_round((avg('tip_amount') / avg('fare_amount') * 100), 2).alias('avg_tip_pct')
    )
    return groupby_ridetime_passenger_number_df

def plt_distance_on_tip_amount_and_fare(df):
    sample_fraction = 0.1
    sample_df = df.select('trip_distance', 'fare_amount', 'tip_amount').sample(False, sample_fraction, seed=42)
    sample_pd = sample_df.toPandas()

    # Need to clip the tip_amount to reduce impact of outliers
    sample_pd['tip_amount'] = sample_pd['tip_amount'].clip(upper=40)
    # clip_value = sample_pd['tip_amount'].quantile(0.99)
    # sample_pd['tip_amount'] = sample_pd['tip_amount'].clip(upper=clip_value)  

    plt.figure(figsize=(10, 6))
    sc = plt.scatter(
        sample_pd['trip_distance'],
        sample_pd['fare_amount'],
        c=sample_pd['tip_amount'],
        cmap='viridis',
        alpha=0.6
    )

    plt.colorbar(sc, label='Tip Amount ($)')
    plt.xlabel('Trip Distance (miles)')
    plt.ylabel('Fare Amount ($)')
    plt.title('Trip Distance vs. Fare Amount (Colored by Tip Amount)')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig("trip_distance_vs_fare_vs_tip.png")
    plt.show()

def bx_plt_distance_on_card_or_cash(df):
    df_pd = df.select('payment_type', 'fare_amount').toPandas()
    df_pd['fare_amount'] = df_pd['fare_amount'].clip(upper=100)
    sns.boxplot(data=df_pd, x='payment_type', y='fare_amount')
    plt.title('Fare Amount by Payment Type')
    plt.tight_layout()
    plt.savefig("Payment Type by Fare Amount.png")
    plt.show()

#udf 
#great for column based operations only, not row
#udf assignment =         custom function name,        casting post variable type 
rename_payment_type_udf = udf(rename_payment_type_col, StringType())
rename_ratecodeid_udf = udf (rename_ratecodeid_col, StringType())
ride_time_udf = udf(add_category_ride_time, StringType())


#Preprocessing Steps
yellow_trip_recs = cols_to_lower(yellow_trip_recs)
yellow_trip_recs = remove_pass_count_0(yellow_trip_recs)
yellow_trip_recs = remove_neg_numbers(yellow_trip_recs)
yellow_trip_recs = remove_outliers(yellow_trip_recs)

yellow_trip_recs = yellow_trip_recs.withColumn(
    "payment_type", rename_payment_type_udf("payment_type")
)
yellow_trip_recs = yellow_trip_recs.withColumn(
    'ratecodeid', rename_ratecodeid_udf('ratecodeid')
)

yellow_trip_recs = yellow_trip_recs.withColumn('trip_date', to_date('tpep_pickup_datetime'))
yellow_trip_recs = yellow_trip_recs.withColumn('start_time', date_format("tpep_pickup_datetime", "hh:mm a"))
yellow_trip_recs = yellow_trip_recs.withColumn('end_time', date_format("tpep_dropoff_datetime", "hh:mm a"))
yellow_trip_recs = yellow_trip_recs.withColumn('day', date_format('trip_date', 'EEEE'))

yellow_trip_recs = yellow_trip_recs.withColumn(
    'category_ride_time',
    ride_time_udf('day', 'start_time')
)

#----------------------------------------------------------------------------

#Transformation Steps
yellow_trip_recs = tip_on_fare_pct(yellow_trip_recs)
# yellow_trip_recs_df=yellow_trip_recs.toPandas()

#Summary Transformations
# payment_type_summary = pay_type_on_tip_fare_avg_summary(yellow_trip_recs)
# passenger_num_summary = pass_num_on_tip_on_fare_summary(yellow_trip_recs)

# payment_type_summary.show()
# passenger_num_summary.show()
# payment_type_summary_df = payment_type_summary.toPandas()
# passenger_num_summary_df = passenger_num_summary.toPandas()

#category rush hour times on payment type and passenger number
category_ride_time_payment_type_summary = groupby_ride_time_and_pay_type_summary(yellow_trip_recs)
# category_ride_time_passenger_num_summary = groupby_ride_time_and_passenger_number_summary(yellow_trip_recs)

#Behavior Insight Card vs. Cash Insights Summary Stats
cash_payment_count = yellow_trip_recs.filter(col("payment_type") == "Cash").count()
card_samplecashsize_summary = yellow_trip_recs.filter(col("payment_type") == "Credit card").limit(cash_payment_count)
card_samplecashsize_summary = pay_type_on_tip_fare_avg_summary(card_samplecashsize_summary)
card_samplecashsize_summary_df = card_samplecashsize_summary.toPandas()

# display dual groupby category_ride_time
category_ride_time_by_payment_type_df = category_ride_time_payment_type_summary.toPandas()
# category_ride_time_by_passenger_num_df = category_ride_time_passenger_num_summary.toPandas()

#average distance on tip amount
# plt_distance_on_tip_amount_and_fare(yellow_trip_recs)

#Can Distance Impact Tip
bx_plt_distance_on_card_or_cash(yellow_trip_recs)

