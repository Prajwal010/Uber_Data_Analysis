# Uber_Data_Analysis
In this project I have used Uber-open source data to build data pipeline using mage and Pyspark to build dashboard in looker studio for analysis

# Steps
1) Took open source taxi data in csv flat file
2) Data modelling in Draw.io
3) created fact and dimension dataframes out of it in Pyspark.
4) Dim ratecode has diff rates ie 1-6, created dictionary and assign dictionary values by map funct
5) created final fact table by merging Dimensions table with joining matching columns
6) uploaded csv data in Google cloud storage bucket
11) set up Virtual machine (VM) instance and open SSH for computing
12) set up Mage instance in that compute VM
13) Used mage to write transformation logic
14) Loded transformed data into Big-Query warehouse
15) written SQL to fetch required columns for the analysis
16) Finally Created Dashboard on Looker studio
