import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, udf, desc, dayofmonth, to_timestamp, month, lit, to_date, count, col, unix_timestamp, when, mean
from datetime import datetime

# import matplotlib.pyplot as plt
from graphframes import *



def fieldcleansing(dataframe):

  if (dataframe.first()['taxi_type']=='yellow_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    
    
  elif (dataframe.first()['taxi_type']=='green_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
  
  
  dataframe = dataframe.filter((dataframe["trip_distance"] >= 0) & (dataframe["fare_amount"] >= 0))
  return dataframe 
  
    
    

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TaxiData")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H-%M-%S")

    # Load Data
    #Test directory references
    yellow_test_dir = "mini_green_tripdata_dataset.csv"
    green_test_dir = "mini_yellow_tripdata_dataset.csv"
    lookup_test_dir = "taxi_zone_lookup.csv"

    yellow_bucket_dir = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/"
    green_bucket_dir = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/"
    lookup_bucket_dir = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    
    
    yellow_tripdata_df = spark.read.option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(yellow_bucket_dir)
    green_tripdata_df =  spark.read.option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(green_bucket_dir)
    taxi_zone_lookup =  spark.read.option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(lookup_bucket_dir)


    # checking and removing any null values or wrong format in the dataset and cleaning them for further processing
    yellow_tripdata_df = fieldcleansing(yellow_tripdata_df)
    green_tripdata_df  =  fieldcleansing(green_tripdata_df)

    
    ##### TASK 1 10%
    #joining to taxizone lookup twice for each df
    #taxizone lookup cols are renamed and locationID is dropped after each join
    yellow_tripdata_df = yellow_tripdata_df.join(taxi_zone_lookup, 
                                                 yellow_tripdata_df.PULocationID ==  taxi_zone_lookup.LocationID, 'left') \
                                            .withColumnRenamed("Borough","Pickup_Borough") \
                                            .withColumnRenamed("Zone","Pickup_Zone") \
                                            .withColumnRenamed("service_zone","Pickup_service_zone") \
                                            .drop('LocationID') \
                                            .join(taxi_zone_lookup, 
                                                 yellow_tripdata_df.DOLocationID ==  taxi_zone_lookup.LocationID, 'left') \
                                            .drop('LocationID') \
                                            .withColumnRenamed("Borough","Dropoff_Borough") \
                                            .withColumnRenamed("Zone","Dropoff_Zone") \
                                            .withColumnRenamed("service_zone","Dropoff_service_zone") \

    green_tripdata_df = green_tripdata_df.join(taxi_zone_lookup, 
                                                 green_tripdata_df.PULocationID ==  taxi_zone_lookup.LocationID, 'left') \
                                            .withColumnRenamed("Borough","Pickup_Borough") \
                                            .withColumnRenamed("Zone","Pickup_Zone") \
                                            .withColumnRenamed("service_zone","Pickup_service_zone") \
                                            .drop('LocationID') \
                                            .join(taxi_zone_lookup, 
                                                 green_tripdata_df.DOLocationID ==  taxi_zone_lookup.LocationID, 'left') \
                                            .drop('LocationID') \
                                            .withColumnRenamed("Borough","Dropoff_Borough") \
                                            .withColumnRenamed("Zone","Dropoff_Zone") \
                                            .withColumnRenamed("service_zone","Dropoff_service_zone") \


                                            
    # #Total Records and cols after filtering
    total_records_green = green_tripdata_df.count()
    total_records_yellow = yellow_tripdata_df.count()
    green_cols = len(green_tripdata_df.columns)
    yellow_cols = len(yellow_tripdata_df.columns)

    #print schema occurs at end of script to preserve output in logs
    #References to original post-join dfs are stored as some columns are added/converted
    green_ref_df = green_tripdata_df
    yellow_ref_df = yellow_tripdata_df


    #####Task 2 10%
    #Count the number of pickups (rows) by borough for the yellow taxi data set and Green Taxi Data Set
    #Sorting in prep for task 4

    pu_plot_green = green_tripdata_df.groupBy('Pickup_Borough').count().sort(desc('count'))
    pu_plot_yellow = yellow_tripdata_df.groupBy('Pickup_Borough').count().sort(desc('count'))

    
    # #Count the number of dropoffs (rows) by borough for the yellow taxi data set and Green Taxi Data Set
    # #Plot barchart in matplotlib with data ouput
    do_plot_green = green_tripdata_df.groupBy('Dropoff_Borough').count().sort(desc('count'))
    do_plot_yellow = yellow_tripdata_df.groupBy('Dropoff_Borough').count().sort(desc('count'))

    #saving data
    
    #####Task 3 10%
    #for YT data filter out trips that have a fare greater than $50 but a distance of less than 1 mile 
    #for the first week of the year 2023

    #Define UDF
    def extract_date(ts_string):
        timestamp = datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
        date = timestamp.strftime('%Y-%m-%d')
        return date
    dateUDF = udf(lambda x: extract_date(x)) 

    
    #converting to necessary fields to floats date type
    yellow_tripdata_df = yellow_tripdata_df.withColumn('fare_amount',col('fare_amount').cast('float')) \
                                            .withColumn('trip_distance',col('trip_distance').cast('float')) \
                                            .withColumn('pickup_date',dateUDF(col('tpep_pickup_datetime')).cast('date'))

    #Applying col conversions to green trip data for consistency
    green_tripdata_df = green_tripdata_df.withColumn('fare_amount',col('fare_amount').cast('float')) \
                                            .withColumn('trip_distance',col('trip_distance').cast('float')) \
                                            .withColumn('pickup_date',dateUDF(col('lpep_pickup_datetime')).cast('date'))


    #applying filters
    start_date = '2023-01-01'
    end_date = '2023-01-07'
    
    filtered_yellow_t3 = yellow_tripdata_df.filter(yellow_tripdata_df.fare_amount > 50) \
                                            .filter(yellow_tripdata_df.trip_distance < 1) \
                                            .filter(yellow_tripdata_df.pickup_date.between(start_date,end_date ))

    filtered_yellow_t3 = filtered_yellow_t3.withColumn('pickup_day', dayofmonth('pickup_date'))

    #grouping by and count for plot
    filtered_yellow_t3 = filtered_yellow_t3.groupby('pickup_day').count()
    filtered_yellow_t3 = filtered_yellow_t3.sort('pickup_day')

    
    ###Task 4 10% (40% total)
    #Visuals Most popular Pickup and Dropoff Boroughs for both yellow and green taxi datasets

    # #reusing dfs from task 2. Already sorted in desc so just selecting top 5
    pu_plot_green_top_5 = pu_plot_green.limit(5)
    pu_plot_yellow_top_5 =  pu_plot_yellow.limit(5)

    do_plot_green_top_5 = do_plot_green.limit(5)
    do_plot_yellow_top_5 = do_plot_yellow.limit(5)


    ###Task 5 (55% total)
    #Analyzing anolomies for yellow cab service in june 2023
    #using same method of filtering as in T3

    start_date = '2023-06-01'
    end_date = '2023-06-30'
    
    filtered_yellow_t5 = yellow_tripdata_df.filter(yellow_tripdata_df.pickup_date.between(start_date,end_date))

    total_trips_t5 = filtered_yellow_t5.count()
    #total trips over number of days in the month
    mean_trips = total_trips_t5/30

    #Counting trips by day and creating columns for integer day of month
    #adding aggregate mean number of daily trips in June for ease of plotting
    plot_yellow_t5 = filtered_yellow_t5.groupby('pickup_date').count()
    plot_yellow_t5 = plot_yellow_t5.withColumn('pickup_day', dayofmonth('pickup_date'))
    plot_yellow_t5 = plot_yellow_t5.withColumn('mean_trips', lit(mean_trips))

    #creating column for upper and lower bound of anomalies
    plot_yellow_t5 = plot_yellow_t5.withColumn('anom_upper', col('mean_trips')*1.8)
    plot_yellow_t5 = plot_yellow_t5.withColumn('anom_lower', col('mean_trips')*.2)

    #flagging anomalies
    plot_yellow_t5 = plot_yellow_t5.withColumn('anomaly', when(col('count') < col('anom_lower'), True )
                                                         .when(col('count') > col('anom_upper'), True)
                                                        .otherwise(False))
    
    plot_yellow_t5 = plot_yellow_t5.sort('pickup_day')

    
    ###Task 6 (70% total)
    ##Statistical anomolies: scatter plot measuring trips with high fare per mile.
    start_date = '2023-03-01'
    end_date = '2023-03-31'

    #filtering based on date range
    filtered_yellow_t6 = yellow_tripdata_df.filter(yellow_tripdata_df.pickup_date.between(start_date,end_date))

    #fpm calculated and numeric day column added for ease of plotting
    filtered_yellow_t6 =filtered_yellow_t6 \
                            .withColumn('fare_per_mile',filtered_yellow_t6.fare_amount/filtered_yellow_t6.trip_distance) \
                                            .withColumn('pickup_day', dayofmonth('pickup_date'))

    #sum fare_per_mile column and divide by number of rows
    #mean fare per mile column added for ease of plotting
    mean_fpm = filtered_yellow_t6.select(mean('fare_per_mile')).first()[0]
    filtered_yellow_t6 = filtered_yellow_t6.withColumn('mean_fpm', lit(mean_fpm))
    
    filtered_yellow_t6 = filtered_yellow_t6.select('fare_per_mile', 'mean_fpm', 'fare_amount', 'trip_distance')
    

    ###Task 7  (85% total)
    #plot percentage of trips by passenger count for yellow and green trip data
    #Count records by passenger count column. divide counts by total records in yellow and green and mult by 100 for %
    
    yellow_t7 = yellow_tripdata_df.groupBy('passenger_count').count().sort(desc('count'))
    yellow_t7 = yellow_t7.withColumn('percent_pass_count', col('count')/total_records_yellow * 100 )

    green_t7 = green_tripdata_df.groupBy('passenger_count').count().sort(desc('count'))
    green_t7 = green_t7.withColumn('percent_pass_count', col('count')/total_records_green * 100)
    
    
    ###Task 8 (100% total)
    #correlation between the trip duration and distance traveled of the trips in the yellow taxi dataset
    start_date = '2023-01-01'
    end_date = '2023-01-31'
    
    #goal is to plot trip duration in relation to distance 
    #Only trips in the specified month (jan)
    filtered_yellow_t8 = yellow_tripdata_df.filter(yellow_tripdata_df.pickup_date.between(start_date,end_date))

    #create time columns
    #duration in seconds col
    filtered_yellow_t8 = filtered_yellow_t8 \
    .withColumn('trip_duration_secs', 
                unix_timestamp(to_timestamp('tpep_dropoff_datetime')) - unix_timestamp(to_timestamp('tpep_pickup_datetime')))
    
    #duration in hours col
    filtered_yellow_t8 = filtered_yellow_t8.withColumn('trip_duration_hours',filtered_yellow_t8.trip_duration_secs/3600)
    
    filtered_yellow_t8 = filtered_yellow_t8.select('trip_distance', 'trip_duration_hours')
    
    ###Task 9 (115% total)
    #reusing data from T2
    t9_plot_yellow = pu_plot_yellow
    t9_plot_green = pu_plot_green

    #adding cols for service id in plot
    t9_plot_yellow = t9_plot_yellow.withColumn('service_color',lit('yellow'))
    t9_plot_green = t9_plot_green.withColumn('service_color',lit('green'))

    #Goal is to produce a barchart that takes the top 10 of the number of trips by pickup borough across taxi services
    #can do a union between the two dataframes with counts from by PU borough from T2, sort and select top 10

    t9_plot_un = t9_plot_yellow.union(t9_plot_green)

    t9_plot_un = t9_plot_un.sort(desc('count'))

    t9_plot_un_top10 = t9_plot_un.limit(10)

    
    ###Task 10
    #Identify the month where the most trips were recorded for both the yellow and the green taxi datasets
    #join on month and borough, then sum yellow and green taxi counts
    #select max

    t10_yellow_group = yellow_tripdata_df
    t10_green_group = green_tripdata_df

    #add pickup month column
    t10_green_group = t10_green_group.withColumn('pickup_month',month('pickup_date'))
    t10_yellow_group = t10_yellow_group.withColumn('pickup_month',month('pickup_date'))
    
    t10_green_group = t10_green_group.groupBy('pickup_month') \
                                    .count().withColumnRenamed('count','count_green') \
                                    .withColumnRenamed('pickup_month','pickup_month_green')
    
    t10_yellow_group = t10_yellow_group.groupBy('pickup_month') \
                                        .count().withColumnRenamed('count','count_yellow') \
                                        .withColumnRenamed('pickup_month','pickup_month_yellow')

    
    t10_join = t10_yellow_group.join(t10_green_group, 
                                                 t10_yellow_group.pickup_month_yellow ==  t10_green_group.pickup_month_green , 'inner')


    t10_join = t10_join.withColumn('total_trips',col('count_green') + col('count_yellow'))

    t10_join = t10_join.sort(desc('total_trips'))
    
    #Top month for combined trips
    t10_top = t10_join.limit(1)

    ###Saving Data
    #Task 2
    pu_plot_green.coalesce(1).write.option("header", "true").format('csv') \
                            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task2/pu_green')
    pu_plot_yellow.coalesce(1).write.option("header", "true").format('csv') \
                            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task2/pu_yellow')
    do_plot_green.coalesce(1).write.option("header", "true").format('csv') \
                            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task2/do_green')
    do_plot_yellow.coalesce(1).write.option("header", "true").format('csv') \
                            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task2/do_yellow')

    #Task 3
    filtered_yellow_t3.coalesce(1).write.option("header", "true").format('csv') \
                        .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task3/yellow')

    #Task 4
    pu_plot_green_top_5.coalesce(1).write.option("header", "true").format('csv') \
                        .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task4/pu_green')
    pu_plot_yellow_top_5.coalesce(1).write.option("header", "true").format('csv') \
                        .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task4/pu_yellow')
    do_plot_green_top_5.coalesce(1).write.option("header", "true").format('csv') \
                        .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task4/do_green')
    do_plot_yellow_top_5.coalesce(1).write.option("header", "true").format('csv') \
                        .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task4/do_yellow')

    #Task 5
    plot_yellow_t5.coalesce(1).write.option("header", "true").format('csv') \
                    .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task5/yellow')

    #Task 6
    filtered_yellow_t6.coalesce(1).write.option("header", "true").format('csv') \
                       .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task6/yellow')

    #Task 7
    yellow_t7.coalesce(1).write.option("header", "true").format('csv') \
            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task7/yellow')
    green_t7.coalesce(1).write.option("header", "true").format('csv') \
            .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task7/green')

    #Task 8
    filtered_yellow_t8.coalesce(1).write.option("header", "true").format('csv') \
                       .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task8/yellow')

    #Task 9
    t9_plot_un_top10.coalesce(1).write.option("header", "true").format('csv') \
                    .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task9/union')

    #Task 10
    t10_top.coalesce(1).write.option("header", "true").format('csv') \
                    .save('s3a://' + s3_bucket + '/cw_out_' + date_time + '/task10/join')
    
    
    #Printing Schema and Row counts from
    #Need to confirm if I'm printing the edited schema
    print('Green Trip Schema')
    print(green_ref_df.printSchema())
    print('There are {} records in the green trip dataframe'.format(total_records_green))
    print('There are {} columns in the green trip dataframe'.format(green_cols))

    print('Yellow Trip Schema')
    print(yellow_ref_df.printSchema())
    print('There are {} records in the yellow trip dataframe'.format(total_records_yellow))
    print('There are {} columns in the yellow trip dataframe'.format(yellow_cols))
    
    spark.stop()