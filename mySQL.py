#imports
import mysql.connector
import pandas as pd

#create a connection to the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456789",
  database="covid")

#assigning cursor
mycursor = mydb.cursor()

#create table
mycursor.execute("CREATE TABLE IF NOT EXISTS covid_cases ( \
    Date_reported DATE, \
    Country VARCHAR(50), \
    Continent VARCHAR(50), \
    New_cases Float, \
    Cumulative_cases Float, \
    New_deaths Float, \
    Cumulative_deaths Float)")


#reading csv
data = pd.read_csv('C:/Users/VK/Desktop/owid-covid-data.csv')
data_stock = data[['date', 'location', 'continent', 'new_cases', 'total_cases', 'new_deaths', 'total_deaths']]

#new data insertion
insert = "INSERT INTO covid_cases (Date_reported, Country, Continent, New_cases, Cumulative_cases, New_deaths, Cumulative_deaths) \
        VALUES (%s, %s, %s, %s, %s, %s, %s)"
for i, row in data_stock.iterrows():
    mycursor.execute(insert, list(row))

#final commit    
mydb.commit()