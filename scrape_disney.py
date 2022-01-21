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
    

print('You did it!')    





