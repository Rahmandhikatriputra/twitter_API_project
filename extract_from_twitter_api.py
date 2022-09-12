#%% extract

import snscrape.modules.twitter as sntwitter
import pandas as pd
import psycopg2
import datetime
from datetime import datetime as dt

tweets = []

# Using TwitterSearchScraper to scrape data and append tweets to list
for tweet in list(sntwitter.TwitterSearchScraper('(from:ptjasamarga) until:2022-09-04 since:2022-08-28').get_items()):
    if len(tweets) == 500000:
        break
    tweets.append([tweet.date, tweet.user.username, tweet.content, tweet.hashtags])
    
tweets_df = pd.DataFrame(tweets, columns=['Datetime', 'Username', 'Tweet', 'Hashtags'])

#%% transform

df = tweets_df.copy()

#drop all rows that contain null values

df = df.dropna()

#unzip all the list data in the Hashtags column

df["Hashtag"] = df["Hashtags"].apply(lambda x : ",".join(x))

#Create a new column contains actual time when the tweet been uploaded
df["Actual Datetime Data"] = df["Datetime"].apply(lambda x : x + datetime.timedelta(minutes = 419))

#Structure the dataset
df = df[["Actual Datetime Data", "Username", "Tweet", "Hashtag"]].reset_index()

#%%Load

#Connect to the database

conn = psycopg2.connect(database="twitter_project",
                        user='postgres', password='Dika100196', 
                        host='localhost', port='5432')


#Create a cursor object
cursor = conn.cursor()

#First create a table inside the database

create_db = """
            CREATE TABLE tweets (
            ID int primary key, 
            Datetime timestamp,
            Username varchar (20),
            Tweet text,
            Hashtag varchar (30)
            );
            """

cursor.execute(create_db)


insert_data = "INSERT INTO tweets VALUES (%s, %s, %s, %s, %s)"

for value in df.values:
    x = list(value)
    cursor.execute(insert_data, (x[0], x[1], x[2], x[3], x[4]))

conn.commit()
    
cursor.close()
conn.close()




        
        
        

