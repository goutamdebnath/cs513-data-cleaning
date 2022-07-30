"""
The Data Cleaning work targets to do the following:
1. Remove the records which is no longitude(x) and latitude(y) populated.
2. Find all the records which matches any condition below:
    1. Either CITY, COUNTY, STATE, ZIP is Blank.
    2. Either CITY, COUNTY, STATE has a Number in it.
    3. The ZIP has alphabets in it.
3. Rename x,y colums to Longitude and Latitude.

--Records to Remove
SELECT FMID, CITY, COUNTY, STATE, ZIP, X,Y FROM FARMERSMARKETDATA WHERE x= '' or y=''; --29

--Records to Cix
SELECT FMID, CITY, COUNTY, STATE, ZIP, typeof(zip), X,Y  FROM FARMERSMARKETDATA
WHERE x > '' AND y >'' AND (CITY = '' OR STATE = '' OR  COUNTY = '' OR ZIP = ''
OR zip not GLOB '*[0-9]*' OR CITY GLOB '*[0-9]*'  OR STATE GLOB '*[0-9]*'  OR COUNTY GLOB '*[0-9]*' ); --1372
"""

import pandas as pd
from uszipcode import SearchEngine

search = SearchEngine()


def getZipCodeByCity(city):
    result = search.by_city(city=city)
    return result[0].zipcode


def getCountyByZipCode(zipcode):
    result = search.by_zipcode(zipcode=zipcode)
    return result.county


def getGISByCoordinates(queryType, lat, lon):
    result = search.by_coordinates(lat=lat, lng=lon, returns=20)

    if result:
        if queryType == "zip":
            return result[0].zipcode
        if queryType == "state":
            return result[0].state
        if queryType == "city":
            return result[0].city
        if queryType == "county":
            return result[0].county
    else:
        print(F'NOT FOUND {queryType}: lat= {lat}, lng={lon}')
        return ''


_farmersData = pd.read_csv("farmersmarkets_clean_openrefine.csv", encoding='utf-8', sep=',')

# _missing_coordinates = _farmersData.loc[pd.isnull(_farmersData['x']) & pd.isnull(_farmersData['y'])]
# print(_missing_coordinates)

_invalid_zip = _farmersData.loc[
    (_farmersData.zip.str.contains(r'[0-9]{5}(?:-[0-9]{4})?', na=False) == False) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]
_invalid_city = _farmersData.loc[
    (_farmersData['city'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['city'])) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]
_invalid_state = _farmersData.loc[
    (_farmersData['State'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['State'])) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]
_invalid_county = _farmersData.loc[
    (_farmersData['County'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['County'])) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]

for invalid_zip in _invalid_zip.iterrows():
    indx = invalid_zip[0]
    longitude = invalid_zip[1].x
    latitude = invalid_zip[1].y
    zipcode = getGISByCoordinates('zip', latitude, longitude)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('zip')] = zipcode
    # print(f'Latitude : {latitude} and Longitude {longitude} --> {zipcode}')

for invalid_city in _invalid_city.iterrows():
    indx = invalid_city[0]
    longitude = invalid_city[1].x
    latitude = invalid_city[1].y
    city = getGISByCoordinates('city', latitude, longitude)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('city')] = city
    # print(f'Latitude : {latitude} and Longitude {longitude} --> {city}')

for invalid_state in _invalid_state.iterrows():
    indx = invalid_state[0]
    longitude = invalid_state[1].x
    latitude = invalid_state[1].y
    state = getGISByCoordinates('state', latitude, longitude)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('State')] = state
    # print(f'Latitude : {latitude} and Longitude {longitude} --> {state}')

for invalid_county in _invalid_county.iterrows():
    indx = invalid_county[0]
    longitude = invalid_county[1].x
    latitude = invalid_county[1].y
    county = getGISByCoordinates('county', latitude, longitude)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('County')] = county
    # print(f'Latitude : {latitude} and Longitude {longitude} --> {county}')

# Phase 2 Cleanup
x1 = getCountyByZipCode(99576)
y1 = getCountyByZipCode(99683)
z1 = getZipCodeByCity('Anchorage')

# remove the invalid
_farmersData = _farmersData.loc[pd.notnull(_farmersData['x']) & pd.notnull(_farmersData['y'])]

# Again Basic Clean up, find the invalid data using 1st pass.
# Attempt to resolve FMID (1001904, 1002348, 1001903)
_missing_zip = _farmersData.loc[pd.isnull(_farmersData['zip'])]
for row in _missing_zip.iterrows():
    indx = row[0]
    city = row[1].city
    zipcode = getZipCodeByCity(city)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('zip')] = zipcode

_missing_county = _farmersData.loc[pd.isnull(_farmersData['County'])]
for row in _missing_county.iterrows():
    indx = row[0]
    zip = row[1].zip
    county = getCountyByZipCode(zip)
    _farmersData.iloc[indx, _farmersData.columns.get_loc('County')] = county

_farmersData.rename(columns={'x': 'longitude', 'y': 'latitude'}, inplace=True)
_farmersData.to_csv("farmersmarkets_clean_openrefine_python.csv")
