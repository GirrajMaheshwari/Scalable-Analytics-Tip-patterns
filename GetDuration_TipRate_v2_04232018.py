# DOCUMENTATION:
'''
Purpose:
- The purpose of this script is to create three new objects from the underlying dataset. 

Object 1: Trip Duration in minutes
- A new column has been created to capture the trip duration in minutes. 

Object 2: Trip Duration Quartile:
- A new column has been created to group the trip durations into 4 quartiles 1Q-4Q. 

Object 3: Tip Rate
- A new colum has been created to generate a value called 'Tip Rate', which is the tip amount / total amount.  The purpose of this value is to provide a relative value for the tip amount that can then be used to understand how and or why passengers are tipping (ex: looking at tip rate by duration, pick-up / drop-off location, time of day, etc. 

Dataset:
- Yellow cab trip data, single month, January 2017. 

'''

# CREATE SPARK CONTEXT & SET FEATURES
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster('local[*]').setAppName('CcirelliCode')
sc = SparkContext(conf = conf)
sc.setLogLevel('ERROR')

# Import basic python packages. 
from dateutil import parser
import pandas as pd
import os

# Import CSV File
Data = sc.textFile(r'/home/ccirelli2/Desktop/GSU/Scalable_Analytics/Final_Project/DataFiles/yellow_tripdata_2017-02.csv')

# CREATE RDD 
'''Column Names
[u'VendorID', u'tpep_pickup_datetime', u'tpep_dropoff_datetime', u'passenger_count', u'trip_distance', u'RatecodeID', u'store_and_fwd_flag', u'PULocationID', u'DOLocationID', u'payment_type', u'fare_amount', u'extra', u'mta_tax', u'tip_amount', u'tolls_amount', u'improvement_surcharge', u'total_amount']

Note: the filter function removes the first row that contains the column names. 
'''

Data_splitNewLine = Data.map(lambda x: x.split(','))\
                        .filter(lambda x: len(x) > 1)\
                        .filter(lambda x: 'VendorID' not in x)


# Reference: Index values for the time (hour:minute) for Pick-up & Drop-off times 
'''Documentation
- Pick-up time:         x[1][10:16]
- Drop-off time:        x[2][10:16]  
'''


# STEPS TO CREATE DURATION QUARTILES

#1. Trip Duration:  Get Median, Min, and Max for grouping:     
'''Documentation:
- A sample set of 10,000 records was used in order to calculate the Max, Median and Min values for the trip duration. 

- SampeSize     = 10,000
- Max           = 1,429
- Median        = 9
- Min           = 1

# Code
- Sample_RDD = Trip_Duration_minutes_RDD.take(10000)
- df = pd.DataFrame(Sample_RDD)
- print('@@@@@@@@@', '\n', 'Medain =>',df.median(), '\n', 'Max =>', df.max(), '\n', 
        'Min =>', df.min())
'''

#2. Get Upper half of dataset (above median)
'''
- Filter:               Limit RDD to upper bound
- Median_upper:         16 was the median for the upper bound. 

# Code
- UpperHalf_RDD = Trip_Duration_minutes_RDD.filter(lambda x: x > 9).take(10000)
- df_median = pd.DataFrame(UpperHalf_RDD).median()
- print('@@@@@@@', 'Median =>', df_median)
'''

#3. Get Median Lower Bound
'''
- Filter:               Limit RDD to lower bound (== or less than 9)
- Median_lower:         5 

LowerHalf_RDD = Trip_Duration_minutes_RDD.filter(lambda x: x < 9 or x == 9).take(10000)
df_median = pd.DataFrame(LowerHalf_RDD).median()
print('@@@@@@', 'Median =>', df_median)
'''

#4. Define Quartiles:
'''Below are the quartiles that are the result from the above analysis
- Q1 =  0  to 5
- Q2 =  6  to 9
- Q3 =  10 to 16
- Q4 =     >  16
'''


# FINAL FUNCTION - RDD TRANSFORMATION 
'''New Objects:
- Trip Duration_minutes
- Tip Rate
- Duration Quartile. 
''' 

# Top Level Function to Create Trip Duration & TripDuration Quartiles
'''Transformations:
- Calculate Trip Duration in minutes
- Group Trip duration into quartiles using conditional statements. 
- Calculate Tip-rate = Tip / Total Amount.  
'''
def tranform_RDD(RDD):
        VendorID =              RDD[0]
        PU_datetime =           RDD[1]
        DO_datetime=            RDD[2]
        passenger_count =       RDD[3]
        trip_distance =         RDD[4]
        RatecodeID =            RDD[5]
        store_and_fwd_flag=     RDD[6]
        PULocationID=           RDD[7]
        DOLocationID=           RDD[8]
        payment_type=           RDD[9]
        fare_amount=            RDD[10]
        extra=                  RDD[11]
        mta_tax=                RDD[12]
        tip_amount=             float(RDD[13])
        tolls_amount=           RDD[14]
        improvement_surcharge=  RDD[15]
        total_amount=           float(RDD[16])

        # Calculate Trip Duration
        Trip_duration_minutes = (((parser.parse(DO_datetime[10:16])\
                                 - parser.parse(PU_datetime[10:16]))\
                                .total_seconds())/ 60)
        # Calculate Tip Rate
        Tip_rate =              round((tip_amount / total_amount),2)

        # Categorize Trip Duration into Quartiles i-iv
        Duration_quartile = ''
        if Trip_duration_minutes < 5 or Trip_duration_minutes == 5:
                Duration_quartile = '1Q'
        elif Trip_duration_minutes > 5 and Trip_duration_minutes < 9\
                                        or Trip_duration_minutes == 9:
                Duration_quartile = '2Q'
        elif Trip_duration_minutes > 9 and Trip_duration_minutes < 16\
                                        or Trip_duration_minutes == 16:
                Duration_quartile = '3Q'
        elif Trip_duration_minutes > 16:
                Duration_quartile = '4Q'

        # Return Original Data Set with the Addition of TripDuration, TipRate, DurationQ
        return  VendorID, PU_datetime, DO_datetime, passenger_count, trip_distance,\
                RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type,\
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,\
                tip_amount, Trip_duration_minutes, Duration_quartile, Tip_rate

# Map Transformation Function to RDD
'''Pass transformer function to RDD'''
RDD_add_Duration_TipRate = Data_splitNewLine.map(tranform_RDD)

# Print Results
print('#######', RDD_add_Duration_TipRate.take(10))

# Write Results to Text File
RDD_add_Duration_TipRate.saveAsTextFile('/home/ccirelli1/Scripts/RDD_Transformed.txt')























