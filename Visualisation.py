# Import libraries
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Snowflake connection parameters (as a dictionary)
conn_params = {
    'user': 'Impana',
    'password': 'Snowflake@2025',
    'account': 'YRTMAHQ-AO40908',  
    'warehouse': 'F1_WH',
    'database': 'F1_DATA_WAREHOUSE',
    'schema': 'FACTS'  
}

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(**conn_params)  # Pass the dictionary using **
    print("✅ Connected to Snowflake successfully!")
except Exception as e:
    print(f"❌ Failed to connect to Snowflake. Error: {e}")
    exit()  # Exit the script if connection fails

# Create a cursor
cursor = conn.cursor()

 # Query 1: Top 10 Drivers by Total Points
query1 = """
SELECT 
    dd.full_name AS driver_name,
    SUM(fr.points) AS total_points
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
GROUP BY dd.full_name
ORDER BY total_points DESC
LIMIT 10;
"""
cursor.execute(query1)

# Fetch data as a list of tuples
data = cursor.fetchall()

# Convert to Pandas DataFrame
df1 = pd.DataFrame(data, columns=['DRIVER_NAME', 'TOTAL_POINTS'])

# Visualization for Query 1: Pie Chart
plt.figure(figsize=(8, 8))
plt.pie(df1['TOTAL_POINTS'], labels=df1['DRIVER_NAME'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('viridis'))
plt.title('Top 10 Drivers by Total Points', fontsize=16)
plt.show()

# Query 2: Races with the Most Overtakes (Grid Position vs. Final Position)
query2 = """
SELECT 
    dr.name AS race_name,
    dr.race_date,
    dd.full_name AS driver_name,
    (fr.grid - fr.position) AS overtakes
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_RACES dr ON fr.race_sk = dr.race_sk
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
WHERE fr.grid > fr.position
ORDER BY overtakes DESC
LIMIT 10;
"""
cursor.execute(query2)

# Fetch data as a list of tuples
data2 = cursor.fetchall()

# Convert to Pandas DataFrame
df2 = pd.DataFrame(data2, columns=['RACE_NAME', 'RACE_DATE', 'DRIVER_NAME', 'OVERTAKES'])

# Visualization for Query 2: Scatter Plot
plt.figure(figsize=(12, 8))
sns.scatterplot(x='RACE_DATE', y='OVERTAKES', data=df2, hue='DRIVER_NAME', palette='viridis', s=100)
plt.title('Top 10 Races with the Most Overtakes', fontsize=16)
plt.xlabel('Race Date', fontsize=14)
plt.ylabel('Number of Overtakes', fontsize=14)
plt.legend(title='Driver Name', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# Query 3: Total Points Earned by Each Constructor in a Specific Year
query3 = """
SELECT 
    dc.name AS constructor_name,
    SUM(fr.points) AS total_points
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_CONSTRUCTORS dc ON fr.constructor_sk = dc.constructor_sk
JOIN DIMENSIONS.DIM_RACES dr ON fr.race_sk = dr.race_sk
WHERE dr.year = 2022  -- Filter for a specific year
GROUP BY dc.name
ORDER BY total_points DESC;
"""
cursor.execute(query3)

# Fetch data as a list of tuples
data3 = cursor.fetchall()

# Convert to Pandas DataFrame
df3 = pd.DataFrame(data3, columns=['CONSTRUCTOR_NAME', 'TOTAL_POINTS'])

# Visualization for Query 3: Treemap
import squarify  # For treemap visualization

plt.figure(figsize=(12, 8))
squarify.plot(sizes=df3['TOTAL_POINTS'], label=df3['CONSTRUCTOR_NAME'], color=sns.color_palette('viridis', len(df3)))
plt.title('Total Points Earned by Each Constructor in 2022', fontsize=16)
plt.axis('off')  # Remove axes
plt.show()

# Query 4: Average Finishing Position of Drivers by Nationality
query4 = """
SELECT 
    dd.nationality,
    AVG(fr.position) AS avg_finishing_position
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
GROUP BY dd.nationality
ORDER BY avg_finishing_position ASC;
"""
cursor.execute(query4)

# Fetch data as a list of tuples
data4 = cursor.fetchall()

# Convert to Pandas DataFrame
df4 = pd.DataFrame(data4, columns=['NATIONALITY', 'AVG_FINISHING_POSITION'])

# Visualization for Query 4: Violin Plot
plt.figure(figsize=(12, 8))
sns.violinplot(x='AVG_FINISHING_POSITION', y='NATIONALITY', data=df4, palette='viridis')
plt.title('Average Finishing Position of Drivers by Nationality', fontsize=16)
plt.xlabel('Average Finishing Position', fontsize=14)
plt.ylabel('Nationality', fontsize=14)
plt.show()

# Query 5: Most Consistent Drivers (Lowest Standard Deviation in Finishing Positions)
query5 = """
SELECT 
    dd.full_name AS driver_name,
    STDDEV(fr.position) AS position_stddev
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
GROUP BY dd.full_name
HAVING COUNT(*) > 10 
ORDER BY position_stddev ASC;
"""
cursor.execute(query5)

# Fetch data as a list of tuples
data5 = cursor.fetchall()

# Convert to Pandas DataFrame
df5 = pd.DataFrame(data5, columns=['DRIVER_NAME', 'POSITION_STDDEV'])

# Query 5: Most Consistent Drivers (Lowest Standard Deviation in Finishing Positions)
query5 = """
SELECT 
    dd.full_name AS driver_name,
    STDDEV(fr.position) AS position_stddev
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
GROUP BY dd.full_name
HAVING COUNT(*) > 10  -- Only consider drivers with more than 10 races
ORDER BY position_stddev ASC;
"""
cursor.execute(query5)

# Fetch data as a list of tuples
data5 = cursor.fetchall()

# Convert to Pandas DataFrame
df5 = pd.DataFrame(data5, columns=['DRIVER_NAME', 'POSITION_STDDEV'])

# Visualization for Query 5: Box Plot
plt.figure(figsize=(12, 8))
sns.boxplot(x='POSITION_STDDEV', y='DRIVER_NAME', data=df5, hue='DRIVER_NAME', palette='viridis', legend=False)
plt.title('Most Consistent Drivers (Lowest Standard Deviation in Finishing Positions)', fontsize=16)
plt.xlabel('Standard Deviation of Finishing Positions', fontsize=14)
plt.ylabel('Driver Name', fontsize=14)
plt.show()

# Query 6: Average Points per Race by Constructor
query6 = """
SELECT 
    dc.name AS constructor_name,
    AVG(fr.points) AS avg_points_per_race
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_CONSTRUCTORS dc ON fr.constructor_sk = dc.constructor_sk
GROUP BY dc.name
ORDER BY avg_points_per_race DESC;
"""
cursor.execute(query6)

# Fetch data as a list of tuples
data6 = cursor.fetchall()

# Convert to Pandas DataFrame
df6 = pd.DataFrame(data6, columns=['CONSTRUCTOR_NAME', 'AVG_POINTS_PER_RACE'])

# Visualization for Query 6: Average Points per Race by Constructor
plt.figure(figsize=(12, 8))
plt.stem(df6['AVG_POINTS_PER_RACE'], df6['CONSTRUCTOR_NAME'], basefmt=" ")
plt.title('Average Points per Race by Constructor', fontsize=16)
plt.xlabel('Average Points per Race', fontsize=14)
plt.ylabel('Constructor Name', fontsize=14)
plt.show()

# Query 7: Drivers with the Most Fastest Laps
query7 = """
SELECT 
    dd.full_name AS driver_name,
    COUNT(*) AS fastest_laps
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
WHERE fr.positionOrder = 1  -- Assuming positionOrder = 1 indicates fastest lap
GROUP BY dd.full_name
ORDER BY fastest_laps DESC
LIMIT 10;
"""
cursor.execute(query7)

# Fetch data as a list of tuples
data7 = cursor.fetchall()

# Convert to Pandas DataFrame
df7 = pd.DataFrame(data7, columns=['DRIVER_NAME', 'FASTEST_LAPS'])

# Visualization for Query 7: Donut Chart
plt.figure(figsize=(8, 8))
plt.pie(df7['FASTEST_LAPS'], labels=df7['DRIVER_NAME'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('viridis'))
plt.title('Drivers with the Most Fastest Laps', fontsize=16)
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)
plt.show()


# Query 8: Most Improved Drivers (Year-over-Year Points Growth)
query8 = """
WITH DriverYearlyPoints AS (
    SELECT 
        dd.full_name AS driver_name,
        dr.year,
        SUM(fr.points) AS total_points
    FROM FACTS.FACT_RESULTS fr
    JOIN DIMENSIONS.DIM_RACES dr ON fr.race_sk = dr.race_sk
    JOIN DIMENSIONS.DIM_DRIVERS dd ON fr.driver_sk = dd.driver_sk
    GROUP BY dd.full_name, dr.year
)
SELECT 
    driver_name,
    MAX(total_points) - MIN(total_points) AS points_growth
FROM DriverYearlyPoints
GROUP BY driver_name
ORDER BY points_growth DESC
LIMIT 10;
"""
cursor.execute(query8)

# Fetch data as a list of tuples
data8 = cursor.fetchall()

# Convert to Pandas DataFrame
df8 = pd.DataFrame(data8, columns=['DRIVER_NAME', 'POINTS_GROWTH'])

# Convert POINTS_GROWTH to float
df8['POINTS_GROWTH'] = df8['POINTS_GROWTH'].astype(float)

# Visualization for Query 8: Bar Chart
plt.figure(figsize=(12, 8))
sns.barplot(x='POINTS_GROWTH', y='DRIVER_NAME', data=df8, hue='DRIVER_NAME', palette='viridis', legend=False)
plt.title('Most Improved Drivers (Year-over-Year Points Growth)', fontsize=16)
plt.xlabel('Points Growth', fontsize=14)
plt.ylabel('Driver Name', fontsize=14)
plt.show()

# Query 9: Qualifying Performance vs. Race Performance for Drivers
query9 = """
WITH QualifyingPerformance AS (
    SELECT 
        fq.driver_sk,
        AVG(fq.position) AS avg_qualifying_position
    FROM FACTS.FACT_QUALIFYING fq
    WHERE fq.position IS NOT NULL  -- Exclude NULL qualifying positions
    GROUP BY fq.driver_sk
),
RacePerformance AS (
    SELECT 
        fr.driver_sk,
        AVG(fr.position) AS avg_race_position
    FROM FACTS.FACT_RESULTS fr
    WHERE fr.position IS NOT NULL  -- Exclude NULL race positions
    GROUP BY fr.driver_sk
)
SELECT 
    dd.full_name AS driver_name,
    qp.avg_qualifying_position,
    rp.avg_race_position,
    abs(rp.avg_race_position - qp.avg_qualifying_position) AS performance_difference
FROM QualifyingPerformance qp
JOIN RacePerformance rp ON qp.driver_sk = rp.driver_sk
JOIN DIMENSIONS.DIM_DRIVERS dd ON qp.driver_sk = dd.driver_sk
WHERE qp.avg_qualifying_position IS NOT NULL  -- Ensure no NULL values in final result
  AND rp.avg_race_position IS NOT NULL
ORDER BY performance_difference DESC;
"""
cursor.execute(query9)

# Fetch data as a list of tuples
data9 = cursor.fetchall()

# Convert to Pandas DataFrame
df9 = pd.DataFrame(data9, columns=['DRIVER_NAME', 'AVG_QUALIFYING_POSITION', 'AVG_RACE_POSITION', 'PERFORMANCE_DIFFERENCE'])

# Convert columns to float
df9['AVG_QUALIFYING_POSITION'] = df9['AVG_QUALIFYING_POSITION'].astype(float)
df9['AVG_RACE_POSITION'] = df9['AVG_RACE_POSITION'].astype(float)
df9['PERFORMANCE_DIFFERENCE'] = df9['PERFORMANCE_DIFFERENCE'].astype(float)

# Visualization for Query 9: Scatter Plot
plt.figure(figsize=(12, 8))
sns.scatterplot(x='AVG_QUALIFYING_POSITION', y='AVG_RACE_POSITION', data=df9, hue='PERFORMANCE_DIFFERENCE', palette='viridis', s=100)
plt.title('Qualifying Performance vs. Race Performance for Drivers', fontsize=16)
plt.xlabel('Average Qualifying Position', fontsize=14)
plt.ylabel('Average Race Position', fontsize=14)
plt.legend(title='Performance Difference', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

import textwrap  # For wrapping text into multiple lines

# Query 10: Circuits with the Most Diverse Winners
query10 = """
SELECT 
    dc.name AS circuit_name,
    COUNT(DISTINCT fr.driver_sk) AS unique_winners
FROM FACTS.FACT_RESULTS fr
JOIN DIMENSIONS.DIM_RACES dr ON fr.race_sk = dr.race_sk
JOIN DIMENSIONS.DIM_CIRCUITS dc ON dr.circuitId = dc.circuitId
WHERE fr.position = 1  -- Only consider race winners
GROUP BY dc.name
ORDER BY unique_winners DESC;
"""
cursor.execute(query10)

# Fetch data as a list of tuples
data10 = cursor.fetchall()

# Convert to Pandas DataFrame
df10 = pd.DataFrame(data10, columns=['CIRCUIT_NAME', 'UNIQUE_WINNERS'])

# Convert UNIQUE_WINNERS to int
df10['UNIQUE_WINNERS'] = df10['UNIQUE_WINNERS'].astype(int)

# Wrap circuit names into multiple lines (e.g., max 2 lines)
df10['CIRCUIT_NAME'] = df10['CIRCUIT_NAME'].apply(lambda x: '\n'.join(textwrap.wrap(x, width=15)))  # Adjust width as needed

# Visualization for Query 10: Treemap
plt.figure(figsize=(12, 8))
squarify.plot(
    sizes=df10['UNIQUE_WINNERS'],
    label=df10['CIRCUIT_NAME'],
    color=sns.color_palette('viridis', len(df10)),
    text_kwargs={'fontsize': 8}  # Decrease font size
)
plt.title('Circuits with the Most Diverse Winners', fontsize=16)
plt.axis('off')  # Remove axes
plt.show()

# Close the cursor and connection
cursor.close()
conn.close()
print("✅ Connection closed.")