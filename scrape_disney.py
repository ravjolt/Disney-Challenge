#import dependencies
from bs4 import BeautifulSoup as bs
from webdriver_manager.chrome import ChromeDriverManager
from splinter import Browser
import pandas as pd
from selenium import webdriver
import json
import pymongo
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert, MetaData
import sqlalchemy
import numpy as np
from time import sleep
import requests
from pprint import pprint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Table 



def park_info(browser):
    engine = create_engine('sqlite:///disney_park.sqlite')
    engine.execute('DROP TABLE IF EXISTS disney_park')
    conn = engine.connect
    meta=MetaData()    
    disney_park = Table(
    'disney_park', meta,
    Column('index', Integer, primary_key = True),
    Column("Disney Park", String(255)),
    Column("Park Status", String(255)),)

    meta.create_all(engine)


    executable_path = {'executable_path': ChromeDriverManager().install()}
    browser = Browser('chrome', **executable_path, headless=True)

    #connect to website
    url_initial = 'https://queue-times.com/en-US/parks/17/queue_times'
    browser.visit(url_initial)

    html = browser.html

    #parse HTML with Beautiful Soup
    soup = bs(html, 'html.parser')


    ride_status_list = []
        #gets the current statuses of the each disney park

        #pages of each park page
    pages = (4,5,6,7,8,16,17,28,30,274,275)
    status_dict = {}
        #loop through each page to get the park name and current park status
    #pages of each park page
    pages = (4,5,6,7,8,16,17,28,30,274,275)

#loop through each page to get the park name and current park status
    for page in pages:
        url = 'https://queue-times.com/en-US/parks/' +str(page) + '/queue_times'
        browser.visit(url)
        html = browser.html
        soup = bs(html, 'html.parser')
        park_status = soup.find("p",class_= "subtitle").text.replace('\n',' ')
        status_dict = {}
        ride_status_list.append(status_dict)
   
    
    for i in soup.find_all("div", class_="buttons"):
        
            park = soup.find("h1", class_="title").text.strip('\n')
            park = park.replace('live wait times','')
            v= {'Disney Park':park, 'Park Status':park_status}
            status_dict.update(v)
    
    status_df=pd.DataFrame(ride_status_list)
    status_df['Disney Park']=status_df['Disney Park'].str.replace('live wait times','')
    status_df.to_sql('disney_park', con=engine, if_exists="append")
    
    return
    
def ride_info():

    engine_2 = create_engine('sqlite:///disney_ride_wait.sqlite')
    engine_2.execute('DROP TABLE IF EXISTS disney_ride_wait')
    conns = engine_2.connect
    meta_2=MetaData()    
    disney_ride_wait = Table(
    'disney_ride_wait', meta_2,
    Column('index', Integer, primary_key = True),
    Column("Disney Park", String(255)),
    Column("Ride", String(255)),
    Column("Currently Open", String(255)),
    Column("Wait Time (minutes)", Float),)


    meta_2.create_all(engine_2)

    lands_info = []
    rides_info =[]

    land_pages = ["4","5","6","7","8","16","17", "28", "30"]

    #get rides from disney japan that do not exist in a land
    path = 'https://queue-times.com/en-US/parks/274/queue_times.json'
    ride_json = (requests.get(path))

    ride_data = ride_json.json()['rides']
    ride_1 = pd.json_normalize(ride_data)
    rides_info.append(ride_1)
    
    #get rides from disney parks that exist in lands
    for i in land_pages:
        path_3 = 'https://queue-times.com/en-US/parks/' + i + '/queue_times.json'
        response = (requests.get(path_3))
        for x in response:
            land_data = response.json()['lands']
 
            land = pd.json_normalize(land_data,
                              record_path = ['rides'])
        lands_info.append(land)

    #creates dataframes to concatenate later for ride statuses
    lands_df = pd.DataFrame()
    lands_df['Disney Park/Resort'] = []
    lands_df['Land']= []
    lands_df["Ride"]= []
    lands_df["Currently Open"] = []
    lands_df['Current Wait Time'] = []

    jp_rides_no_land=rides_info[0]
    jp_rides_no_land = jp_rides_no_land.drop(columns=['id','last_updated'])
    jp_rides_no_land= jp_rides_no_land.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    jp_rides_no_land["Disney Park"]= "Tokyo Disney Resort Magic Kingdom"

    lands_paris=lands_info[0]
    lands_paris = lands_paris.drop(columns=['id','last_updated'])
    lands_paris = lands_paris.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    lands_paris["Disney Park"]= "DisneyLand Park Paris"
    
    epcot = lands_info[1]

    epcot = epcot.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    epcot = epcot.drop(columns=['id','last_updated'])
    epcot["Disney Park"]= "DisneyWorld Epcot"

    dis_world = lands_info[2]
    dis_world = dis_world.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    dis_world = dis_world.drop(columns=['id','last_updated'])
    dis_world["Disney Park"]= "Disney Magic Kingdom"

    hollywood = lands_info[3]
    hollywood = hollywood.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    hollywood = hollywood.drop(columns=['id','last_updated'])
    hollywood["Disney Park"]= "Disney Hollywood Studios"

    animal_kg = lands_info[4]
    animal_kg = animal_kg.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    animal_kg= animal_kg.drop(columns=['id','last_updated'])
    animal_kg["Disney Park"]= "Disney Animal Kingdom"

    cali_dis = lands_info[5]
    cali_dis = cali_dis.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    cali_dis= cali_dis.drop(columns=['id','last_updated'])
    cali_dis["Disney Park"]= "Disneyland"

    cali_adv = lands_info[6]
    cali_adv = cali_adv.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    cali_adv= cali_adv.drop(columns=['id','last_updated'])
    cali_adv["Disney Park"]= "Disney Califomrnia Adventure"

    paris_sds = lands_info[7]
    paris_sds = paris_sds.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    paris_sds= paris_sds.drop(columns=['id','last_updated'])
    paris_sds["Disney Park"]= "Walt Disney Studios Paris"

    shanghai = lands_info[8]
    shanghai  = shanghai.rename(columns={'name':'Ride',
                              'is_open':'Currently Open',
                              'wait_time':'Wait Time (minutes)'})
    shanghai = shanghai .drop(columns=['id','last_updated'])
    shanghai ["Disney Park"]= "Shanghai Disney Resort"

    disney_rides_df=pd.concat([lands_paris,
                          epcot,
                          dis_world,
                          hollywood,
                          animal_kg,
                          cali_dis,
                          cali_adv,
                          paris_sds,
                          shanghai,
                          jp_rides_no_land])
    disney_rides_df=disney_rides_df[["Disney Park",
    "Ride","Currently Open", "Wait Time (minutes)"]]

    disney_rides_df.to_sql('disney_ride_wait', con=engine_2, if_exists="append")

print('You did it!Part 1 Complete')
 #create table for database
def movie_info():
    engine_4 = create_engine('sqlite:///disney_movie_performance.sqlite')
    table_drop_4 = engine_4.execute('DROP TABLE IF EXISTS disney_movie_performance')
    Conn_4 = engine_4.connect
    meta_4=MetaData()    
    disney_movie_performance = Table(
    'disney_movie_performance', meta_4,
    Column('index', Integer, primary_key = True),
    Column('No. 1 Movie', String(255)),
    Column('Total Movies Released', String(255)),
    Column('Average Production Budget', String(255)),
    Column('Combined Worldwide Box Office', String(255)),)

    meta_4.create_all(engine_4)

    engine_5 = create_engine('sqlite:///best_seller.sqlite')
    table_drop_5 = engine_5.execute('DROP TABLE IF EXISTS best_seller')
    conn_5 = engine_5.connect
    meta_5=MetaData()   
    best_seller = Table(
    'best_seller', meta_5,
    Column('index', Integer, primary_key = True),
    Column('Overall Best Selling Movie', String(255)),)
    meta_5.create_all(engine_5)

 
    #setup splinter
    executable_path = {'executable_path':ChromeDriverManager().install()}
    browser = Browser('chrome', **executable_path, headless = False)
        #connect to website
    url = 'https://www.the-numbers.com/movies/distributor/Walt-Disney#tab=year'
    browser.visit(url)
    #get data from url
    page = requests.get(url)


    #reterieve table data
    dfs = pd.read_html(page.text)
    dfs
    box_office_stats = dfs[1]
    total_stats = dfs[0]
    disney_data = pd.merge(total_stats,box_office_stats,how='left')


    disney_data.drop(labels=['AnnualStats','Index'],axis=1,inplace=True)



    disney_data['Total Movies Released'] = disney_data['TotalMovies']
    disney_data['Average Production Budget'] = disney_data['AverageProductionBudget']
    disney_data['Combined Worldwide Box Office'] = disney_data['CombinedWorldwideBox Office']
    disney_data = disney_data[['Year',
                           'No. 1 Movie',
                           'Total Movies Released',
                           'Average Production Budget', 
                           'Combined Worldwide Box Office']]
    disney_data= disney_data[disney_data['No. 1 Movie'].notna()] 
    disney_data=disney_data.set_index('Year')
    disney_data= disney_data.drop(['Total'])
    best_seller_info = disney_data.loc["Total", "No. 1 Movie"]
    best_dict = {'Overall Best Selling Movie': best_seller_info}
    best_seller_df = pd.DataFrame(best_dict, index = [0])
    
    disney_data.to_sql('disney_movie_performance', con=engine_4, if_exists="append")
    best_seller_df.to_sql('best_seller', con=engine_5, if_exists="append")

def movie_summary():
    #create table for database
    engine_6 = create_engine('sqlite:///disney_movies_overall.sqlite')
    table_drop_6 = engine_6.execute('DROP TABLE IF EXISTS disney_movies_overall')
    conn__6 = engine_6.connect()
    meta_6=MetaData()
    disney_movies_overall = Table(
    'disney_movies_overall', meta_6,
    Column('index', Integer, primary_key = True),
    Column('Release Year Rank', Float),
    Column('Movie', String(255)),
    Column('ReleaseDate', String(255)),
    Column('Genre', String(255)),
    Column('MPAARating', String(255)),
    Column('Genre', String(255)),
    Column('MPAARating', String(255)),  
    Column('Year Released', Float),
    Column('Gross for Release Year', String(255)),)
    meta_6.create_all(engine_6)
    
    
    
    years = ['2021',
    '2020',
    '2019',
    '2018',
    '2017',
    '2016',
    '2015',
    '2014',
    '2013',
    '2012',
    '2011',
    '2010',
    '2009',
    '2008',
    '2007',
    '2006',
    '2005',
    '2004',
    '2003',
    '2002',
    '2001',
    '2000',
    '1999',
    '1998',
    '1997',
    '1996',
    '1995',
    '1994',
    '1993',
    '1992',
    '1991',
    '1990',
    '1989',
    '1988',
    '1987',
    '1986',
    '1985',
    '1984',
    '1983',
    '1982',
    '1981',
    '1980',
    '1979',
    '1977',
    '1975',
    '1973',
    '1971',
    '1970',
    '1968',
    '1967',
    '1964',
    '1963',
    '1962',
    '1961',
    '1959',
    '1955',
    '1954',
    '1950',
    '1946',
    '1940',
    '1937']

    executable_path = {'executable_path':ChromeDriverManager().install()}
    browser = Browser('chrome', **executable_path, headless = False)
    url_2 = 'https://www.the-numbers.com/market/2022/distributor/Walt-Disney'
    browser.visit(url_2)
    html = browser.html
    soup = bs(html, 'html.parser')

    data = []


    for year in years:
        url_2 = 'https://www.the-numbers.com/market/' +str(year) + '/distributor/Walt-Disney'
        pg = requests.get(url_2)
        dd = pd.read_html(pg.text)
        data.append(dd)
    #reterieve table data
    data

    #don't run more than once or else removes half of list information
    data_1=data
    remove = [x.pop(0) for x in data_1]

    #modifies all dataframes beofre concatinating
    data21=pd.DataFrame(data_1[0][0])
    data21['Year Released']= 2021
    data21['Gross for Release Year']=data21['2021 Gross']
    data21=data21.drop(columns=['2021 Gross','Tickets Sold'])
    data21=data21.drop(labels=[9,10],axis=0)
    data21

    data20=pd.DataFrame(data_1[1][0])
    data20['Year Released']=2020
    data20['Gross for Release Year']=data20['2020 Gross']
    data20=data20.drop(columns=['2020 Gross','Tickets Sold'])
    data20=data20.drop(labels=[14,15],axis=0)
    data20

    data19=pd.DataFrame(data_1[2][0])
    data19['Year Released']=2019
    data19['Gross for Release Year']=data19['2019 Gross']
    data19=data19.drop(columns=['2019 Gross','Tickets Sold'])
    data19=data19.drop(labels=[13,14],axis=0)

    data19


    data18=pd.DataFrame(data_1[3][0])
    data18['Year Released']=2018
    data18['Gross for Release Year']=data18['2018 Gross']
    data18=data18.drop(columns=['2018 Gross','Tickets Sold'])
    data18=data18.drop(labels=[13,14],axis=0)


    data18


    data17=pd.DataFrame(data_1[4][0])
    data17['Year Released']=2017
    data17['Gross for Release Year']=data17['2017 Gross']
    data17=data17.drop(columns=['2017 Gross','Tickets Sold'])
    data17=data17.drop(labels=[13,14],axis=0)

    data17


    data16=pd.DataFrame(data_1[5][0])
    data16['Year Released']=2016
    data16['Gross for Release Year']=data16['2016 Gross']
    data16=data16.drop(columns=['2016 Gross','Tickets Sold'])
    data16=data16.drop(labels=[18,19],axis=0)

    data16


    data15=pd.DataFrame(data_1[6][0])
    data15['Year Released']=2015
    data15['Gross for Release Year']=data15['2015 Gross']
    data15=data15.drop(columns=['2015 Gross','Tickets Sold'])
    data15=data15.drop(labels=[15,16],axis=0)

    data15

    data14=pd.DataFrame(data_1[7][0])
    data14['Year Released']=2014
    data14['Gross for Release Year']=data14['2014 Gross']
    data14=data14.drop(columns=['2014 Gross','Tickets Sold'])
    data14=data14.drop(labels=[18,19],axis=0)

    data14


    data13=pd.DataFrame(data_1[8][0])
    data13['Year Released']=2013
    data13['Gross for Release Year']=data13['2013 Gross']
    data13=data13.drop(columns=['2013 Gross','Tickets Sold'])
    data13=data13.drop(labels=[17,18],axis=0)
    data13

    data12=pd.DataFrame(data_1[9][0])
    data12['Year Released']=2012
    data12['Gross for Release Year']=data12['2012 Gross']
    data12=data12.drop(columns=['2012 Gross','Tickets Sold'])
    data12=data12.drop(labels=[18,19],axis=0)
    data12

    data11=pd.DataFrame(data_1[10][0])
    data11['Year Released']=2011
    data11['Gross for Release Year']=data11['2011 Gross']
    data11=data11.drop(columns=['2011 Gross','Tickets Sold'])
    data11=data11.drop(labels=[18,19],axis=0)
    data11

    data10=pd.DataFrame(data_1[11][0])
    data10['Year Released']=2010
    data10['Gross for Release Year']=data10['2010 Gross']
    data10=data10.drop(columns=['2010 Gross','Tickets Sold'])
    data10=data10.drop(labels=[17,18],axis=0)
    data10

    data09=pd.DataFrame(data_1[12][0])
    data09['Year Released']=2009
    data09['Gross for Release Year']=data09['2009 Gross']
    data09=data09.drop(columns=['2009 Gross','Tickets Sold'])
    data09=data09.drop(labels=[23,24],axis=0)
    data09

    data08=pd.DataFrame(data_1[13][0])
    data08['Year Released']=2008
    data08['Gross for Release Year']=data08['2008 Gross']
    data08=data08.drop(columns=['2008 Gross','Tickets Sold'])
    data08=data08.drop(labels=[19,20],axis=0)
    data08

    data07=pd.DataFrame(data_1[14][0])
    data07['Year Released']=2007
    data07['Gross for Release Year']=data07['2007 Gross']
    data07=data07.drop(columns=['2007 Gross','Tickets Sold'])
    data07=data07.drop(labels=[21,22],axis=0)
    data07

    data06=pd.DataFrame(data_1[15][0])
    data06['Year Released']=2006
    data06['Gross for Release Year']=data06['2006 Gross']
    data06=data06.drop(columns=['2006 Gross','Tickets Sold'])
    data06=data06.drop(labels=[25,26],axis=0)
    data06


    data05=pd.DataFrame(data_1[16][0])
    data05['Year Released']=2005
    data05['Gross for Release Year']=data05['2005 Gross']
    data05=data05.drop(columns=['2005 Gross','Tickets Sold'])
    data05=data05.drop(labels=[24,25],axis=0)
    data05

    data04=pd.DataFrame(data_1[17][0])
    data04['Year Released']=2004
    data04['Gross for Release Year']=data04['2004 Gross']
    data04=data04.drop(columns=['2004 Gross','Tickets Sold'])
    data04=data04.drop(labels=[26,27],axis=0)
    data04

    data03=pd.DataFrame(data_1[18][0])
    data03['Year Released']=2003
    data03['Gross for Release Year']=data03['2003 Gross']
    data03=data03.drop(columns=['2003 Gross','Tickets Sold'])
    data03=data03.drop(labels=[31,32],axis=0)
    data03

    data02=pd.DataFrame(data_1[19][0])
    data02['Year Released']=2002
    data02['Gross for Release Year']=data02['2002 Gross']
    data02=data02.drop(columns=['2002 Gross','Tickets Sold'])
    data02=data02.drop(labels=[29,29],axis=0)
    data02

    data01=pd.DataFrame(data_1[20][0])
    data01['Year Released']=2001
    data01['Gross for Release Year']=data01['2001 Gross']
    data01=data01.drop(columns=['2001 Gross','Tickets Sold'])
    data01=data01.drop(labels=[19,20],axis=0)
    data01

    data00=pd.DataFrame(data_1[21][0])
    data00['Year Released']=2000
    data00['Gross for Release Year']=data00['2000 Gross']
    data00=data00.drop(columns=['2000 Gross','Tickets Sold'])
    data00=data00.drop(labels=[28,29],axis=0)
    data00

    data99=pd.DataFrame(data_1[22][0])
    data99['Year Released']=1999
    data99['Gross for Release Year']=data99['1999 Gross']
    data99=data99.drop(columns=['1999 Gross','Tickets Sold'])
    data99=data99.drop(labels=[30,31],axis=0)
    data99

    data98=pd.DataFrame(data_1[23][0])
    data98['Year Released']=1998
    data98['Gross for Release Year']=data98['1998 Gross']
    data98=data98.drop(columns=['1998 Gross','Tickets Sold'])
    data98=data98.drop(labels=[28,29],axis=0)
    data98

    data97=pd.DataFrame(data_1[24][0])
    data97['Year Released']=1997
    data97['Gross for Release Year']=data97['1997 Gross']
    data97=data97.drop(columns=['1997 Gross','Tickets Sold'])
    data97=data97.drop(labels=[34,35],axis=0)
    data97

    data96=pd.DataFrame(data_1[25][0])
    data96['Year Released']=1996
    data96['Gross for Release Year']=data96['1996 Gross']
    data96=data96.drop(columns=['1996 Gross','Tickets Sold'])
    data96=data96.drop(labels=[37,38],axis=0)
    data96

    data95=pd.DataFrame(data_1[26][0])
    data95['Year Released']=1995
    data95['Gross for Release Year']=data95['1995 Gross']
    data95=data95.drop(columns=["1995 Gross",'Tickets Sold'])
    data95=data95.drop(labels=[38,39],axis=0)

    #creates list to concatenate
    list_disney_df=[
    data21,
    data20,
    data19,
    data18,
    data17,
    data16,
    data15,
    data14,
    data13,
    data12,
    data11,
    data10,
    data09,
    data08,
    data07,
    data06,
    data05,
    data04,
    data03,
    data02,
    data01,
    data00,
    data20,
    data99,
    data98,
    data97,
    data96,
    data95]

    #merge dataframes
    overall_movie_data=pd.concat(list_disney_df)
    overall_movie_data=overall_movie_data.rename(columns={"Rank":"Release Year Rank"})

    #append to database
    overall_movie_data.to_sql('disney_movies_overall', con=engine_6, if_exists="append")
print('You did it!')    





