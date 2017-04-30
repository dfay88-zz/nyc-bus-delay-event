
# coding: utf-8

# In[1]:

sc


# In[2]:

import csv
import pandas as pd
from scipy import stats
from datetime import datetime
from pyspark.sql import functions
from pyspark.sql.types import TimestampType


# In[3]:

def parseCSV(idx, records):
    for row in csv.reader(records):
        direction = 0
        bus = row[7].split('_')[2]
        tripid = row[7].split('_')[1].split('-')[2]
        start = int(row[0].split('T')[1].split(':')[0])
        minute = int(row[0].split('T')[1].split(':')[1])
        t = datetime.strptime(row[0], '%Y-%m-%dT%H:%M:%SZ')  
        date = t.strftime('%Y-%m-%d')
        tm = t.strftime('%Y-%m-%d %H:%M:%S')
        unique_key = str(date) + str(bus) + str(tripid)
        
        # Create bus direction
        # Achilles changes - changed the values for the bearings and switched directions for Q48
        
        if bus == 'BX1':
            if float(row[4]) < 200:
                direction = 1
            else:
                direction = 2
        elif bus == 'BX6':
            if float(row[4]) < 110:
                direction = 1
            else:
                direction = 2
        elif bus == 'BX13':
            if 70 <= float(row[4]) < 150:
                direction = 1
            else:
                direction = 2
        elif bus == 'Q48':
            if 100 <= float(row[4]) < 150:
                direction = 2
            else:
                direction = 1 
                
        if minute < 15:
            interval = str(start) + str(':00-') + str(start) + str(':15')
        elif 15 <= minute < 30:
            interval = str(start) + str(':15-') + str(start) + str(':30')
        elif 30 <= minute < 45:
            interval = str(start) + str(':30-') + str(start) + str(':45')
        elif 45 <= minute < 60:
            interval = str(start) + str(':45-') + str(start+1) + str(':00')

        yield unique_key, tm, bus, tripid, direction, interval

## Source data file. 
# path = '/Users/JordanVani/Documents/NYU/BDM/nyc-bus-delay-event/Data/BDM_BusData.csv'
path = 'data/BDM_BusData_subset.csv'

## Parse datafile to RDD.
data = sc.textFile(path).mapPartitionsWithIndex(parseCSV)

## For each unique bus line, calculate route start time.
min_by_group = (data
                .map(lambda x: (x[0], x[0:6]))
                .reduceByKey(lambda x1, x2: min(x1, x2, key=lambda x: x[1]))
                .values()
                .map(lambda x: (x[0], (x[1:6]))))

## For each unique bus line, calculate route end time.
max_by_group = (data
                .map(lambda x: (x[0], x[0:2]))
                .reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[1]))
                .values())

## Join start and stop times.
rdd = min_by_group.join(max_by_group)
rdd = rdd.flatMap(lambda x: [[x[0], x[1][0][0], x[1][1], x[1][0][1],
                              x[1][0][2], x[1][0][4]]])

# Calculate duration of bus.
time_diff = rdd.toDF(['id', 'start', 'stop', 'bus', 'tripid', 'interval'])
time_diff = time_diff.select('id', time_diff['start'].cast('timestamp'),
                             time_diff['stop'].cast('timestamp'), 'bus', 'tripid', 'interval')
timeDiff = (functions.unix_timestamp('stop', format="yyyy-MM-dd HH:mm:ss")
            - functions.unix_timestamp('start', format="yyyy-MM-dd HH:mm:ss"))
time_diff = time_diff.withColumn('duration', timeDiff)

# Calculate mean direction
trip_dir = data.toDF(['id_', 'time', 'bus', 'tripid', 'direction', 'interval'])
trip_dir = trip_dir.groupby("id_").agg({'direction': 'avg'})

# Join direction back to data.
master = time_diff.join(trip_dir, time_diff.id == trip_dir.id_, how='left_outer')
master = master.select('id', 'start', 'bus', 'tripid', 'interval', 'duration', 
                       functions.col('avg(direction)').cast('int').alias('direction'))


# In[4]:

rdd_times = (master
             .groupby("bus", functions.date_format('start', 'yyyy-MM-dd').alias('date'), 
                      "direction", "interval")
             .agg({"duration": "avg", "id": "count"}))


# In[5]:

(rdd_times
 .sort(functions.col('bus'), functions.col('date'), functions.col('interval')))


# In[6]:

# read in schedule data
schedules = sc.textFile('game_schedules/combined_schedules.csv').map(lambda line: line.split(","))


# In[7]:

# convert schedule data to dataframe
sched_df = schedules.toDF(['index','Home team','starttime','endtime','startwindow_start',                                'startwindow_end','endwindow_start','endwindow_end'])
#sched_df.head(3)


# In[8]:

#sched_df.printSchema()


# In[9]:

#master.printSchema()


# In[10]:

# convert all time columns to type 'timestamp'
for col in sched_df.columns[2:]:
    sched_df = sched_df.withColumn(col, sched_df[col].cast('timestamp'))
#sched_df.printSchema()


# In[11]:

# add a 'date' column to use for join with rdd_times
sched_df = sched_df.withColumn('date', functions.date_format('starttime', 'yyyy-MM-dd'))


# In[12]:

# join schedule data to bus interval data
joined_df = rdd_times.join(sched_df, 'date', 'left')
#joined_df.take(3)


# In[13]:

joined_df = joined_df.drop('index', 'Home team', 'starttime', 'endtime')
#joined_df.printSchema()


# In[14]:

#from pyspark.sql.functions import lit, concat, col

joined_df = joined_df.withColumn('time', functions.split(joined_df.interval, '-')[0])
joined_df = joined_df.withColumn('start', functions.concat(functions.col('date'), functions.lit(' '),                                                            functions.col('time')).cast('timestamp'))
#joined_df.take(3)


# In[15]:

joined_df = joined_df.drop('time')
#joined_df.printSchema()


# In[16]:

def checkWindows(start, start_start, start_end, end_start, end_end):
    try:
        if (start >= start_start) & (start <= start_end) |             (start >= end_start) & (start <= end_end):
            return 1
        else:
            return 0
    except:
        pass

labelFunc = functions.udf(checkWindows)


# In[17]:

labeled_df = joined_df.withColumn('is_game_bus', labelFunc(joined_df.start,                                                     joined_df.startwindow_start,                                                     joined_df.startwindow_end,                                                     joined_df.endwindow_start,                                                     joined_df.endwindow_end))


# In[18]:

labeled_df = labeled_df.drop('startwindow_start', 'startwindow_end', 'endwindow_start', 'endwindow_end', 'start')
labeled_df = labeled_df.orderBy('date', 'bus', 'interval')
#labeled_df.show(5)


# In[21]:

dates = ("2014-03-31",  "2014-11-04")
date_from, date_to = [functions.to_date(functions.lit(s)).cast(TimestampType()) for s in dates]

df_2014 = labeled_df.where((labeled_df.date > date_from) & (labeled_df.date < date_to))


# In[22]:

dates = ("2015-03-31",  "2015-11-04")
date_from, date_to = [functions.to_date(functions.lit(s)).cast(TimestampType()) for s in dates]

df_2015 = labeled_df.where((labeled_df.date > date_from) & (labeled_df.date < date_to))


# In[23]:

dates = ("2016-03-31",  "2016-11-04")
date_from, date_to = [functions.to_date(functions.lit(s)).cast(TimestampType()) for s in dates]

df_2016 = labeled_df.where((labeled_df.date > date_from) & (labeled_df.date < date_to))


# In[24]:

dates = ("2017-03-31",  "2017-11-04")
date_from, date_to = [functions.to_date(functions.lit(s)).cast(TimestampType()) for s in dates]

df_2017 = labeled_df.where((labeled_df.date > date_from) & (labeled_df.date < date_to))


# In[25]:

df_season = df_2014.unionAll(df_2015)
df_season = df_season.unionAll(df_2016)
df_season = df_season.unionAll(df_2017)


# In[26]:

df_baseball = df_season.where(df_season.is_game_bus == 1)
df_no_baseball = df_season.where(df_season.is_game_bus == 0)


# In[27]:

df_no_baseball = df_no_baseball.where(functions.col("interval").isin({'11:30-11:45', '11:45-12:00', '12:00-12:15',                                                                    '12:15-12:30', '12:30-12:45', '12:45-13:00',                                                                    '13:00-13:15', '13:15-13:30', '14:30-14:45',                                                                    '14:45-15:00', '15:00-15:15', '15:15-15:30',                                                                    '15:30-15:45', '15:45-16:00', '16:00-16:15',                                                                    '16:15-16:30', '16:30-16:45', '16:45-17:00',                                                                    '17:00-17:15', '17:15-17:30', '17:30-17:45',                                                                    '17:45-18:00', '18:00-18:15', '18:15-18:30',                                                                    '18:30-18:45', '18:45-19:00', '19:00-19:15',                                                                    '19:15-19:30', '19:30-19:45', '20:00-20:15',                                                                    '20:15-20:30', '21:30-21:45', '21:45-22:00',                                                                    '22:00-22:15', '22:15-22:30', '22:30-22:45',                                                                    '22:45-23:00', '23:00-23:15', '23:15-23:30'}))


# In[28]:

df_no_baseball_pd = df_no_baseball.toPandas()
df_baseball_pd = df_baseball.toPandas()


# In[ ]:

df_no_baseball_pd.to_csv('/user/is1480/project/mta_no_baseball.csv')
df_baseball_pd.to_csv('/user/is1480/project/mta_baseball.csv')


# In[29]:

stats.ks_2samp(df_no_baseball_pd['avg(duration)'], df_baseball_pd['avg(duration)'])


# In[ ]:



