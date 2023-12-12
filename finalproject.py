# %% [markdown]
# 

# %%
with open('amenity.csv', 'r', encoding='utf-8') as data:
    for row in data:
        row = row.strip().split(',')
        print(row)

# %%
import pandas as pd
df = pd.read_csv('amenity.csv')
print("Basic Information about the DataFrame:")
print(df.info())
print("\nFirst Few Rows of the DataFrame:")
print(df.head())
missing_values = df['longitude-lattitude'].isnull().sum()
print(f"\nNumber of Missing Values in 'longitude-lattitude' column: {missing_values}")
df[['longitude', 'latitude']] = df['longitude-lattitude'].str.extract(r'\((.*?),\s*(.*?)\)')
print("\nDataFrame with Separated Longitude and Latitude:")
print(df[['longitude', 'latitude']].head())

# %%
import csv
import pymysql
import ast
with open('amenity.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f, skipinitialspace=True)
    columns = reader.fieldnames
    data = [row for row in reader]
conn = pymysql.connect(
    host='mysql.clarksonmsda.org',
    port=3306,
    user='ia626',
    passwd='ia626clarkson',
    db='ia626',
    autocommit=True
)
cur = conn.cursor(pymysql.cursors.DictCursor)
sql_create_amenity = '''
    CREATE TABLE IF NOT EXISTS `amenity` (
        `aid` INT AUTO_INCREMENT PRIMARY KEY,
        `name` VARCHAR(100) DEFAULT NULL,
        `amenity_type` VARCHAR(50) DEFAULT NULL,
        `longitude` FLOAT DEFAULT NULL,
        `latitude` FLOAT DEFAULT NULL,
        `tags` TEXT DEFAULT NULL
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
'''
cur.execute(sql_create_amenity)
sql_insert_amenity = '''
    INSERT INTO `amenity` (`name`, `amenity_type`, `longitude`, `latitude`, `tags`)
    VALUES (%s, %s, %s, %s, %s);
'''
tokens = []
for row in data:
    name = row.get('name', '')
    amenity_type = row.get('amenity', '')
    longitude, latitude = ast.literal_eval(row.get('longitude-lattitude', (0, 0)))
    tags = row.get('All_tags', '')
    tokens.append([name, amenity_type, longitude, latitude, tags])
    if len(tokens) >= 20:
        cur.executemany(sql_insert_amenity, tokens)
        conn.commit()
        tokens = []
cur.close()
conn.close()


# %%
with open('building.csv', 'r', encoding='utf-8') as data:
    for line_number, row in enumerate(data, start=1):
        row = row.strip().split(',')
        print(row)
        if line_number == 100:
            break

# %%
import pandas as pd
df = pd.read_csv('building.csv')
print("Basic Information about the DataFrame:")
print(df.info())
print("\nFirst Few Rows of the DataFrame:")
print(df.head())
missing_values = df['longitude-lattitude'].isnull().sum()
print(f"\nNumber of Missing Values in 'longitude-lattitude' column: {missing_values}")
df[['longitude', 'latitude']] = df['longitude-lattitude'].str.extract(r'\((.*?),\s*(.*?)\)')
print("\nDataFrame with Separated Longitude and Latitude:")
print(df[['longitude', 'latitude']].head())

# %%
import csv
import pymysql
import re
with open('building.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f, skipinitialspace=True)
    columns = reader.fieldnames
conn = pymysql.connect(
    host='mysql.clarksonmsda.org',
    port=3306,
    user='ia626',
    passwd='ia626clarkson',
    db='ia626',
    autocommit=True
)
cur = conn.cursor(pymysql.cursors.DictCursor)
sql_create_buildings = '''
    CREATE TABLE IF NOT EXISTS `buildings` (
        `bid` INT AUTO_INCREMENT PRIMARY KEY,
        `name` VARCHAR(100) DEFAULT NULL,
        `building_type` VARCHAR(50) DEFAULT NULL,
        `longitude` FLOAT DEFAULT NULL,
        `latitude` FLOAT DEFAULT NULL,
        `tags` TEXT DEFAULT NULL
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
'''
cur.execute(sql_create_buildings)
sql_insert_building = '''
    INSERT INTO `buildings` (`name`, `building_type`, `longitude`, `latitude`, `tags`)
    VALUES (%s, %s, %s, %s, %s);
'''
tokens = []
with open('building.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f, skipinitialspace=True)
    for row in reader:
        name = row['name']
        building_type = row['building']
        match = re.search(r'\(([-+]?\d*\.\d+), ([-+]?\d*\.\d+)\)', row['longitude-lattitude'])
        if match:
            longitude, latitude = map(float, match.groups())
        else:
            print(f"Error extracting longitude and latitude for row: {row}")
            continue
        tags = row['All_tags']
        tokens.append([name, building_type, longitude, latitude, tags])
        if len(tokens) >= 20:
            cur.executemany(sql_insert_building, tokens)
            conn.commit()
            tokens = []
if tokens:
    cur.executemany(sql_insert_building, tokens)
    conn.commit()
cur.close()
conn.close()


