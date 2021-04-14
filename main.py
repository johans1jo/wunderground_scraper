import os
import re

from numpy.lib.function_base import iterable
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
from dateutil import parser, rrule
from datetime import datetime, time, date
from tqdm import tqdm
import time

def getWundergroundData(station, day, month, year):
    """
    Function to return a data frame of hour-level weather data for a single Wunderground PWS station.
    
    Args:
        station (string): Station code from the Wunderground website
        day (int): Day of month for which data is requested
        month (int): Month for which data is requested
        year (int): Year for which data is requested
    
    returns:
        Pandas Dataframe with weather data for specified station and date.
    """
    url = "https://www.wunderground.com/history/daily/np/kathmandu/{station}/date/{year}-{month}-{day}"
    full_url = url.format(station=station, day=day, month=month, year=year)

    chrome_options = Options()  
    chrome_options.add_argument("--headless") 
    driver = webdriver.Chrome("/usr/bin/chromedriver", options=chrome_options)
    driver.get(full_url)

    tables = WebDriverWait(driver,20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table")))
    for table in tables:
        dataframe = pd.read_html(table.get_attribute('outerHTML'))

    # Adding date to the data
    date_insert = "{year}-{month}-{day}".format(day=day, month=month, year=year)
    dataframe[0].insert(0, "Date", date_insert)

    return dataframe[0][:24]

def getDates(start_date, end_date):
    """Generates a list of dates.
    
    Args:
        start_date (string): yyyy-mm-dd
        end_date (string): yyyy-mm-dd

    returns:
        list withh dates as strings
    """
    # Generate a list of all of the dates we want data for
    start = parser.parse(start_date)
    end = parser.parse(end_date)
    dates = list(rrule.rrule(rrule.DAILY, dtstart=start, until=end))
    return dates

def weatherStaionToCSV(data, station):
    """Saves data from one weather station to a Comma Separated Values file (CSV file)

    Args:
        data (2D list): a list with weather stations and their data
        station (string): the station in question
    """
    # Combine all of the individual days and output to CSV for analysis.
    outname = '{}_raw_weather.csv'.format(station)
    outdir = './data/'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, outname) 
    pd.concat(data[station]).to_csv(fullname)

def weatherStaionToEXCEL(data, station):
    """Saves data from one weather station to an Excel file for use in Unscrambler

    Args:
        data (2D list): a list with weather stations and their data
        station (string): the station in question
    """
    # Combine all of the individual days and output to CSV for analysis.
    outname = '{}_raw_weather.csv'.format(station)
    outdir = './data/'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, outname) 
    pd.concat(data[station]).to_excel(fullname)


def scrapeDataToCSV(station, dates):
    """A function to scrape all data corresponding to "dates" and save the result

    Args:
        station (string): the station in question
        dates (list of stirings): a list of dates 
    """
    # Set a backoff time in seconds if a request fails
    backoff_time = 10
    data = {}

    # Gather data for each station in turn and save to CSV.
    print("Working on {}".format(station))
    data[station] = []
    for date in tqdm(dates):
        # Print period status update messages
        if date.day % 10 == 0:
            print("\nWorking on date: {} for station {}".format(date, station))
        done = False
        while done == False:
            try:
                weather_data = getWundergroundData(station, date.day, date.month, date.year)
                #print(weather_data)
                done = True
            except ConnectionError as e:
                # May get rate limited by Wunderground.com, backoff if so.
                print("Got connection error on {}".format(date))
                print("Will retry in {} seconds".format(backoff_time))
                time.sleep(10)
        # Add each processed date to the overall data
        data[station].append(weather_data)

    #print(data)
    weatherStaionToCSV(data, station)
    weatherStaionToEXCEL(data, station)

def processData(station):
    """Prosessing the data to make it suitable to further analysis.

    Args:
        station (string): the station in question
    """
    csv_name = '{}_raw_weather.csv'.format(station)
    # Loading CSV
    data_raw = pd.read_csv('data/' + csv_name)

    # Changing column names to something nice
    cols = {'Date' : 'Date', 
        'Temperature' : 'Temperature [F]', 
        'Dew Point' : 'Dew Point [F]', 
        'Humidity' : 'Humidity [%]', 
        'Wind' : 'Wind Direction', 
        'Wind Speed' : 'Wind Speed [mph]', 
        'Wind Gust' : 'Wind Gust [mph]', 
        'Pressure' : 'Pressure [in]', 
        'Precip.' : 'Precipitation [inch]'}
    data_raw.rename(columns = cols, inplace=True)

    # Converting the data from string with unit to only a float number
    data_raw['Time'] = data_raw['Time'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Time'] = data_raw['Time'].str.replace(':', '.')
    data_raw['Temperature [F]'] = data_raw['Temperature [F]'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Dew Point [F]'] = data_raw['Dew Point [F]'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Humidity [%]'] = data_raw['Humidity [%]'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Wind Speed [mph]'] = data_raw['Wind Speed [mph]'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Wind Gust [mph]'] = data_raw['Wind Gust [mph]'].str.split(expand = True)[:][0]#.astype(float)
    data_raw['Pressure [in]'] = data_raw['Pressure [in]'].str.split(expand = True)[:][0].str.replace('.', ',')#.astype(float)
    data_raw['Pressure [in]'] = data_raw['Pressure [in]'].str.replace('.', ',')
    data_raw['Precipitation [inch]'] = data_raw['Precipitation [inch]'].str.split(expand = True)[:][0]#.astype(float)

    # Katmandu has no presipitation sensor; droppping the column
    data_raw = data_raw.drop('Precipitation [inch]', axis = 1)

    # Updating CSV with prosecced data
    data_raw.to_csv('data/{}_processed_weather.csv'.format(station))
    data_raw.to_excel('data/{}_processed_weather.xlsx'.format(station))

def oneHotEncode(station):
    """One HHot encodes the string variables that Wunderground returns.
    """
    csv_name = '{}_processed_weather.csv'.format(station)
    # Loading CSV
    data_raw = pd.read_csv('data/' + csv_name)

    one_hot = pd.get_dummies(data_raw['Wind Direction'])
    data_raw = data_raw.drop('Wind Direction', axis = 1)
    data_raw = data_raw.join(one_hot)
    
    # Possible conditions : 
    # ['Cloudy', 'Drizzle', 'Fair', 'Fog', 'Heavy Rain', 'Heavy T-Storm', 'Light Rain', 'Light Rain with Thunder', 'Mostly Cloudy', 'Partly Cloudy', 'Rain', 'T-Storm', 'Thunder']

    generalized_conditions = []

    for item in data_raw['Condition']:
        if type(item) != str:
            generalized_conditions.append("Other")
        elif "rain" in item.lower() or "drizzle" in item.lower() or "misty" in item.lower() or "storm" in item.lower() or "thunder" in item.lower():
            generalized_conditions.append("Rain")
        elif "cloudy" in item.lower() or "fog" in item.lower():
            generalized_conditions.append("Cloudy")
        elif "fair" in item.lower():
            generalized_conditions.append("Sun")
        else:
            generalized_conditions.append("Other")

    one_hot = pd.get_dummies(generalized_conditions)
    data_raw = data_raw.drop('Condition', axis = 1)
    data_raw = data_raw.join(one_hot)

    # Updating CSV with prosecced data
    data_raw.to_csv('data/{}_onehot_weather.csv'.format(station))
    data_raw.to_excel('data/{}_onehot_weather.xlsx'.format(station))

def main():
    start_date = "2019-01-01"
    end_date = "2019-12-28"
    station = 'VNKT' # Khatmandu

    dates = getDates(start_date, end_date)
    #scrapeDataToCSV(station, dates)
    processData(station)
    oneHotEncode(station)

    

if __name__== "__main__":
    main()