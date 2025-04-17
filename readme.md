# NYC TLC Data Analytics Project

This project is a challenge that aims to look at NYC TLC data which are publicly available on their website and using data engineering and data analysis to do some 
EDA, derive insights, and propose new policy that can result from looking at the data.

## Contents:
- Selected Dataset
- Tech Stack
- Research Question
- Data Analysis Summary


## Selected Dataset:
- Yellow Taxi Trip Records(2024) https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

## Tech Stack:
- Python 3.9
- Pyspark
- Pandas
- Microsoft Excel
- Github
- Google Slides (Reporting/Data Viz)
- DataCamp(Pyspark Course)

## Research Questions:
- How can we increase profit by maximizing tip earnings for TLC drivers and introduce/update policies without increasing fare prices? How does payment type affect tip amount? What are the other leading factors to paying a higher tip?

## Data Analytics Summary
- There is a obvious positive trend between trip fare and trip distance, which has a intermediate affect on tip amount.
- Shorter trips tend receive no to lower tips, between the $0 - $5 range.
- Medium to longer trips receive higher tips often between and over the $10 - $15 range.
- The median fare generally falls within the $10 - $15 range.
- Tipping doesn’t seem to always be consistent in the higher fares as there are some nuances.
- Cash riders tend to take shorter trips, which can be viewed in the lower interquartile range of the boxplot, while longer trips are typically paid by card possibly due to convenience. 
- Trips around the $40-$50 range tend to be more disputed, which may need to be further studied and investigated.
