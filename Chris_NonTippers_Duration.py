# CREATE CONTEXT
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
conf = SparkConf().setMaster('local[*]')\
		  .setAppName('The_Cheapest_Tipper')\
		  .set('spark.ui.port', '5400')
		  

sc = SparkContext(conf = conf)
#sc.setLogLevel('ERROR')

# Import basic python packages. 
from dateutil import parser
import pandas as pd
import os


# TAXI DATA - CREATE RDD 
File = r'yellow_tripdata_2016-01.csv'

Yellow_Cab_RDD = sc.textFile(File)
Yellow_Cab_data = Yellow_Cab_RDD.map(lambda x: x.split(','))\
                                .filter(lambda x: len(x) > 5)\
                                .filter(lambda x: 'VendorID' not in x)
# RDD TRANSFORMATIONS
'''
1.) Trip Duration in Minutes
2.) Tip rate
3.) PU / DO grid location
'''

# TIP RELATIONSHIPS
'''
1.) Set Tip rate == 0
2.) Tip rate by duration
3.) Tip rate by location
4.) Tip rate by number of passengers
5.) Tip rate by time of day
6.) Tip rate by taxi cab company
'''

# Original Data File - 
'''
0  VendorID 
1  tpep_pickup_datetime
2  tpep_dropoff_datetime
3  passenger_count
4  trip_distance
5  pickup_longitude 
6  pickup_latitude 
7  RatecodeID
8  store_and_fwd_flag
9  dropoff_longitude 
10 dropoff_latitude 
11 payment_type
12 fare_amount 
13 extra
14 mta_tax 
15 tip_amount 
16 tolls_amount 
17 improvement_surcharge 
18 total_amount
'''


# TRANSFORMATION 1-2: DURATION & TIP RATE

# Reference: Index values for the time (hour:minute) for Pick-up & Drop-off times 
'''Documentation
- Pick-up time:         x[1][10:16]
- Drop-off time:        x[2][10:16]  
'''
#4. Define Quartiles:
'''Below are the quartiles that are the result from the above analysis
- Q1 =  0  to 5
- Q2 =  6  to 9
- Q3 =  10 to 16
- Q4 =     >  16
'''

def tip_duration_transformation(RDD):
        # Create Objects
	DO_datetime  = RDD[1]
	PU_datetime  = RDD[2]
	tip_amount   = round(float(RDD[15]),2)
	total_amount = round(float(RDD[18]),2)

	# Calculate Trip Duration
        Trip_duration_minutes = (((parser.parse(DO_datetime[10:16])\
                                 - parser.parse(PU_datetime[10:16]))\
                                .total_seconds())/ 60)
        # Calculate Tip Rate
        Tip_rate =              tip_amount
	'''since we know the tip amount == 0, we dont need this calculation
	round((tip_amount / total_amount),2)'''

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
	return (
		RDD[0],             	#VendorID 
        	RDD[1],             	#tpep_pickup_datetime
        	RDD[2],             	#tpep_dropoff_datetime
        	RDD[3],             	#passenger_count
        	round(float(RDD[4]), 4),#trip_distance
        	round(float(RDD[5]), 4),#pickup_longitude 
        	round(float(RDD[6]), 4),#pickup_latitude 
        	RDD[7],             	#RatecodeID
        	RDD[8],             	#store_and_fwd_flag
        	round(float(RDD[9]),4), #dropoff_longitude 
        	round(float(RDD[10]),4),#dropoff_latitude 
        	RDD[11],            	#payment_type
        	RDD[12],            	#fare_amount 
        	RDD[13],            	#extra
        	RDD[14],            	#mta_tax 
        	float(RDD[15]),        	#tip_amount 
        	RDD[16],            	#tolls_amount 
        	RDD[17],            	#improvement_surcharge 
        	float(RDD[18]),        	#total_amount
		Trip_duration_minutes,  #trip_duration_minutes (new)
		Duration_quartile, 	#duration_quartile (new)
		Tip_rate		#tip_rate (new)
		)

# Transform RDD
Yellow_cab_data_tipRate_duration = Yellow_Cab_data.map(tip_duration_transformation)
'''print('@@@@@@@',Yellow_cab_data_tipRate_duration.take(1))'''



# RELATIONSHIP #1 - TIP AMOUNT == 0, DURATION
'''
Tip_set_to_zero = Yellow_cab_data_tipRate_duration.filter(lambda x: x[15] == 0)\
						  .map(lambda x: (x[-2], 1))
Tip_zero_reduceByKey = Tip_set_to_zero.reduceByKey(lambda x, y: x + y)

print('@@@@@@@@@@', Tip_zero_reduceByKey.collect())
'''

NonTipper_Duration_Distance = Yellow_cab_data_tipRate_duration.filter(lambda x: x[15] ==0)\
					.map(lambda x: (x[-2],(x[4],1)))

Sum = NonTipper_Duration_Distance.reduceByKey(lambda x,y: (x[0] + y[0], 
							   x[1] + y[1]))

print('@@@@@@@@@', Sum.collect())







