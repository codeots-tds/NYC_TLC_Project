## Project Guidelines

### Table of Contents:
- Datasets
- Project Thesis
- Hypothesis
- Tools
- Strategies
- Additional Extra Credit

Datasets:
a. Yellow Taxi Trip Records (PARQUET)
b. Green Taxi Trip Records (PARQUET)
c. For-Hire Vehicle Trip Records (PARQUET)
d. High Volume For-Hire Vehicle Trip Records (PARQUET)

1. Project Thesis "Tipping Behavior Insights":
Questions:
 a. What the factors that affect tip amounts in NYC Taxis?
 b. Are these the same factors between green taxis and yellow taxis?
 c. Do longer trips affect tip amount?
 d. Which zones in NYC are more proposed to give higher tips?
 e. Do more passengers equate to a higher tip?
 f. Do riders who take trips to and from airports, tip more?
 g. Best times of day for taxi drivers:
    -  (7 AM – 10 AM):
        - commuters heading to work
        - trips to transportation hubs (penn station, port authority, grand central, jfk/lga)
        - business travelers (can get decent tips)
    - (4 PM – 7 PM):
        - commuters heading home
        - longer trips from manhattan to other boroughs
        - less ride hailing competiton
    - (10 PM – 2 AM):
        - bars, restaurants, night life (people tip more when socializing)
        - surge demand as fewer cabs are on the road
        - fewer traffic delays = faster trip turnaround

    - Fridays - Saturdays (6pm - 2am)
        - Higher volume, longer trips, and tips are more generous

2. Hypothesis:
- trips with longer travel time = bigger tips
- trips with higher fares = bigger tips
- trips in wealthier neighborhoods = bigger tips
- more passengers != bigger tip
- yes I believe people going to and from airports tend to tip more.

3. Tools:
- python 3.9
- pyspark
- excel
- pandas
- github
- powerpoint/google slides

4. Strategies:
a. Important Columns:
    - passenger_count(How many passengers per trip)
    - trip_distance (total distance between pick up location/drop off location in miles)
    - payment_type (whether cash, card, no charge, dispute, unknown, voided trip)
    - extra (miscellaneous extras and surcharges)
    - fare amount (The time-and-distance fare calculated by the meter. For additional information on the following column)
    - tip_amount (rider tip amount given for each travel)
    - total_amount (total amount charged to passengers. Doesn't include cash tips)
    - ratecodeID (for filtering trips that are to/from airport)

b. Calculate tip % on total fare per trip.

c. Check passenger count and correlate to fare amount and tip

d. check average fare, and average tip amount and tip % by payment type


5. Policies:
 a. Encouraging/Enforcing cab drivers to accept debit/credit cards.
    - Card payers tend to tip higher.
    - Not everyone carries cash nowadays. For tourists, younger riders who don't really
    know the prices till after their ride they may only have enough cash for the ride.
    - Minimizes risk of theft and danger for driver and rider carrying cash.
    - Increase ridership; this might be a preferred option over rideshare if card transactions are allowed.
    - Business travelers get electronic recipts for expense reporting.
    - Increases the likelihood of competition with rideshare apps.


6. Possible Issues:
   - Transaction fees. 2.5%?
   - Learning how to adapt new technology.

7. How can this impact other TLC policies?
  a. Improve driver earnings without changing base fair prices for customers.
  b. Tie into wider cashless economy initatives like allowing crypto payments.
  c. Financial Transparency & Tax Compliance Policies.
  d. Modernizing TLC cab technology for a more seamless experience:
  - Enforcing more digital meters, screens etc. Further tying into NYC's green vehicle program as an incentive upgrade.
  e. Allow for better oversight on rider experience through real-time tracking.
  f. Equitable Customer Protection:
  - allows for under priviledged people using prepaid cards(EBT, TAP) to get fair access to cabs.


8. Additional Extra Credit:
  a. Does weather impact tip amount?

  b. Is tip amount impacted by different seasons or time of the year?

  c. Doing a monthly analysis, possibly even the previous 5 years.

  d. get zone median income data and look up corresponding pickup/drop off location i.ds to look at factors zoning pickup impact on tip amount:
  e. Compare top 10 highest paid zones vs. top 10 lowest paid zones by fare and tip amount or tip percentage.

  f. Can holidays affect tip amount?




