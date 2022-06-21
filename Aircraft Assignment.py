#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Importing Libraries

import os 
import pandas as pd
import numpy as np
import sqlite3
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from calendar import weekday


# In[3]:


# Set Working Directory
os.chdir('/Users/hongrong/Desktop/ST2195')


# In[4]:


# Create DataBase
conn = sqlite3.connect('airline2.db')
c = conn.cursor()


# In[5]:


# Create Tables
airports = pd.read_csv("airports.csv")
carriers = pd.read_csv("carriers.csv")
planes = pd.read_csv("plane-data.csv")

airports.to_sql('airports', con = conn, index = False)
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False)

c.execute('''
CREATE TABLE ontime (
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier varchar(5),
  FlightNum int,
  TailNum varchar(8),
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin varchar(3),
  Dest varchar(3),
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
''')
conn.commit()

for year in range(1998, 2000):
    ontime = pd.read_csv(str(year)+".csv")
    ontime.to_sql('ontime', con = conn, if_exists = 'append', index = False)

conn.commit()


# In[8]:


# Best Time of the Day to Fly
q1 = pd.read_sql_query('''SELECT CRSDepTime, ontime.DepDelay
FROM ontime
WHERE ontime.Cancelled = 0 
AND ontime.Diverted = 0 
AND ontime.DepDelay > 0
GROUP BY CRSDepTime
ORDER BY ontime.DepDelay''', conn)

q1.dropna(inplace = True)


# In[10]:


# formatting time
q1a = q1['CRSDepTime'].astype('datetime64[m]').dt.time


# In[423]:


# Best Day of the Week to Fly
q2 = pd.read_sql_query ('''SELECT DayOfWeek AS Day, AVG(ontime.DepDelay) AS avg_delay
FROM ontime
WHERE ontime.Cancelled = 0 
AND ontime.Diverted = 0 
AND ontime.DepDelay > 0
GROUP BY Day
ORDER BY Day''', conn)


# In[424]:


# bar graph
plt.bar(q2.Day, q2.avg_delay, color = 'maroon', width = 0.04)
plt.xlabel("Days")
plt.ylabel("Average Delay")
plt.title("Best Day of the Week to Fly")
plt.show()


# In[574]:


# Best Month of the Year to Fly
q3 = pd.read_sql_query('''SELECT month, AVG(ontime.DepDelay) AS avg_delay
FROM ontime
WHERE ontime.Cancelled = 0 
AND ontime.Diverted = 0 
AND ontime.DepDelay > 0
GROUP BY Month
ORDER BY Month''', conn)


# In[575]:


# bar graph
plt.bar(q3.Month, q3.avg_delay, color = 'maroon', width = 0.04)
plt.xlabel("Days")
plt.ylabel("Average Delay")
plt.title("Best Month of the Year to Fly")
plt.show()


# In[ ]:


# Do older planes suffer more delays?
q4 = pd.read_sql_query('''SELECT planes.year AS engineyear, AVG(ontime.ArrDelay) AS avg_delay
From planes JOIN ontime USING(tailnum)
WHERE ontime.Cancelled = 0
AND ontime.Diverted = 0
AND ontime.ArrDelay > 0
AND ontime.CarrierDelay IS NULL
AND ontime.WeatherDelay IS NULL
AND ontime.NASDelay IS NULL
AND ontime.SecurityDelay IS NULL
AND ontime.LateAircraftDelay IS NULL
AND engineyear != 'None'
AND engineyear > 0
AND engineyear < 2000
GROUP BY engineyear
ORDER BY engineyear''', conn)

#find the age and converting year to numeric
q4['Age'] =  1999 - pd.to_numeric(q4.engineyear)


# In[425]:


# scatter plot
plt.scatter(q4.Age, q4.avg_delay)
plt.xlabel("Average Delay")
plt.ylabel("Age")
plt.title("Correlation between Age and Delays")


# In[11]:


# How does the number of people flying between different locations change over time?
q5 = pd.read_sql_query('''SELECT Month, Year
FROM ontime
WHERE ontime.Cancelled = 0 
AND ontime.Diverted = 0 
AND ontime.DepDelay > 0
ORDER BY Month, Year''', conn)


# In[12]:


#find the number of flights each month
nf = q5.groupby(q5.columns.tolist(),as_index=False).size()
# dataframe
nf = pd.DataFrame(nf)

#sort by year and month
nf = nf.sort_values(["Year","Month"])

#combine year and month
nf["date"] = nf["Month"].astype(str) + ' ' + nf["Year"].astype(str)


# In[13]:


#plot
plt.scatter(nf.date, nf.size)
plt.xlabel("Date")
plt.ylabel("Flights")
plt.title("Changes over Time")


# In[576]:


#Can you detect cascading failures as delays in one airport create delays in others?
q6 = pd.read_sql_query('''SELECT CRSDepTime, DepDelay, Origin, Dest, Year, Month, DayofWeek
FROM ontime
WHERE Cancelled = 0 
AND Diverted = 0 
AND DepDelay > 0
ORDER BY DayofWeek, Month, Year''', conn)


# In[579]:


#filtering out columns to check 
q6a = q6[q6.Dest == ('PHX')]
q6b = q6[q6.Origin == ('PHX')]

#show 
q6a


# In[578]:


q6b

