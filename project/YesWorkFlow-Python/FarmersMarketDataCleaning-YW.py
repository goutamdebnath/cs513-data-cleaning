import sys
import os
import pandas as pd
from uszipcode import SearchEngine

search = SearchEngine()

# @BEGIN PYTHON-DATA-CLEANING-PROCESS
# @PARAM filepath
# @IN farmers_market_missing_zip_city  @URI file:{filepath}\farmersmarkets_clean_openrefine.csv
# @OUT Clean_Farmers_Market_Data  @URI file:{filepath}\farmersmarkets_clean_openrefine_python.csv


def main(filepath):

    # @BEGIN TransformToPandasDF
    # @IN farmers_market_missing_zip_city
    # @OUT PANDAS_DATAFRAME
    _farmersData = pd.read_csv("farmersmarkets_clean_openrefine.csv", encoding='utf-8', sep=',')
    # @END TransformToPandasDF

    # @BEGIN FindInvalidZipCode
    # @IN PANDAS_DATAFRAME
    # @OUT INVALID_ZIPCODE_RECORDS
    _invalid_zip = _farmersData.loc[
    (_farmersData.zip.str.contains(r'[0-9]{5}(?:-[0-9]{4})?', na=False) == False) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]
    # @END FindInvalidZipCode

    # @BEGIN FindInvalidCity
    # @IN PANDAS_DATAFRAME
    # @OUT INVALID_CITY_RECORDS
    _invalid_city = _farmersData.loc[
        (_farmersData['city'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['city'])) & pd.notnull(
            _farmersData['x']) & pd.notnull(_farmersData['y'])]
    # @END FindInvalidCity

    # @BEGIN FindInvalidState
    # @IN PANDAS_DATAFRAME
    # @OUT INVALID_STATE_RECORDS
    _invalid_state = _farmersData.loc[
    (_farmersData['State'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['State'])) & pd.notnull(
        _farmersData['x']) & pd.notnull(_farmersData['y'])]
    # @END FindInvalidCity

    # @BEGIN FindInvalidCounty
    # @IN PANDAS_DATAFRAME
    # @OUT INVALID_COUNTY_RECORDS
    _invalid_county = _farmersData.loc[
        (_farmersData['County'].str.contains(r'[0-9]', na=False) | pd.isnull(_farmersData['County'])) & pd.notnull(
            _farmersData['x']) & pd.notnull(_farmersData['y'])]
    # @END FindInvalidCounty


    # @BEGIN Iterate:Missing_Zip
    # @IN INVALID_ZIPCODE_RECORDS
    # @OUT getGISByCoordinates(Zip)
    for invalid_zip in _invalid_zip.iterrows():
        indx = invalid_zip[0]
        longitude = invalid_zip[1].x
        latitude = invalid_zip[1].y
        # @BEGIN GISByCoordinates:ZIP
        zipcode = getGISByCoordinates('zip', latitude, longitude)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('zip')] = zipcode
        # @END GISByCoordinates:ZIP
        # print(f'Latitude : {latitude} and Longitude {longitude} --> {zipcode}')
    # @END Iterate:Missing_Zip

    # @BEGIN Iterate:Missing_City
    # @IN INVALID_CITY_RECORDS
    # @OUT getGISByCoordinates(City)
    for invalid_city in _invalid_city.iterrows():
        indx = invalid_city[0]
        longitude = invalid_city[1].x
        latitude = invalid_city[1].y
        # @BEGIN GISByCoordinates:CITY
        city = getGISByCoordinates('city', latitude, longitude)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('city')] = city
        # @END GISByCoordinates:CITY
        # print(f'Latitude : {latitude} and Longitude {longitude} --> {city}')
    # @END Iterate:Missing_City


    # @BEGIN Iterate:Missing_State
    # @IN INVALID_STATE_RECORDS
    # @OUT getGISByCoordinates(State)
    for invalid_state in _invalid_state.iterrows():
        indx = invalid_state[0]
        longitude = invalid_state[1].x
        latitude = invalid_state[1].y
        state = getGISByCoordinates('state', latitude, longitude)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('State')] = state
        # print(f'Latitude : {latitude} and Longitude {longitude} --> {state}')
    # @END Iterate:Missing_State


    # @BEGIN Iterate:Missing_County
    # @IN INVALID_COUNTY_RECORDS
    # @OUT getGISByCoordinates(County)
    for invalid_county in _invalid_county.iterrows():
        indx = invalid_county[0]
        longitude = invalid_county[1].x
        latitude = invalid_county[1].y
        county = getGISByCoordinates('county', latitude, longitude)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('County')] = county
        # print(f'Latitude : {latitude} and Longitude {longitude} --> {county}')
    # @END Iterate:Missing_County


    


    # @BEGIN getGISByCoordinates(Zip)
    # @PARAM y @AS Latitude
    # @PARAM x @AS Longitude
    # @END getGISByCoordinates(Zip)

    # @BEGIN getGISByCoordinates(City)
    # @PARAM y @AS Latitude
    # @PARAM x @AS Longitude
    # @END getGISByCoordinates(City)

    # @BEGIN getGISByCoordinates(State)
    # @PARAM y @AS Latitude
    # @PARAM x @AS Longitude
    # @END getGISByCoordinates(State)
    
    # @BEGIN getGISByCoordinates(County)
    # @PARAM y @AS Latitude
    # @PARAM x @AS Longitude
    # @END getGISByCoordinates(County)
    

    #-------------------------------------

    # @BEGIN UpdatedZip
    # @IN getGISByCoordinates(Zip)
    # @END UpdatedZip
    
    # @BEGIN UpdatedCity
    # @IN getGISByCoordinates(City)
    # @END UpdatedCity    

    # @BEGIN UpdatedState
    # @IN getGISByCoordinates(State)
    # @END UpdatedState    
    
    # @BEGIN UpdatedCounty
    # @IN getGISByCoordinates(County)
    # @END UpdatedCounty    

    # @BEGIN JoinResults
    # @IN UpdatedZip
    # @IN UpdatedCity
    # @IN UpdatedState
    # @IN UpdatedCounty
    # @OUT COMBINED_DATASET
    # @END JoinResults  
    
    #-------------------------------------
    
    # @BEGIN PHASE2-CLEANING
    # @IN COMBINED_DATASET
    # @END PHASE2-CLEANING
    
    # PHASE 2 Clean up, find the invalid data using 1st pass.
    # Attempt to resolve FMID (1001904, 1002348, 1001903)
    
    # @BEGIN PHASE2:FindInvalidZip
    # @IN PHASE2-CLEANING
    # @OUT PHASE2:INVALID_ZIP_RECORDS
    _missing_zip = _farmersData.loc[_farmersData['zip'] == '']
    # @END PHASE2:FindInvalidZip
    
    
    # @BEGIN PHASE2:Iterate:Missing_Zip
    # @IN PHASE2:INVALID_ZIP_RECORDS
    for row in _missing_zip.iterrows():
        indx = row[0]
        city = row[1].city
        # @END PHASE2:Iterate:Missing_Zip
        
        # @BEGIN PHASE2:getZipCodeByCity
        # @PARAM CITY
        # @IN PHASE2:Iterate:Missing_Zip
        # @OUT PHASE2:UpdateMissingZip
        zipcode = getZipCodeByCity(city)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('zip')] = zipcode
        # @END PHASE2:getZipCodeByCity
        
    
    # @BEGIN PHASE2:FindInvalidCounty
    # @IN PHASE2:UpdateMissingZip
    # @OUT PHASE2:INVALID_COUNTY_RECORDS
    _missing_county = _farmersData.loc[_farmersData['County'] == '']
    # @END PHASE2:FindInvalidCounty

    
    # @BEGIN PHASE2:Iterate:Missing_County
    # @IN PHASE2:INVALID_COUNTY_RECORDS
    for row in _missing_county.iterrows():
        indx = row[0]
        zip = row[1].zip
    # @END PHASE2:Iterate:Missing_County
    
    
    # @BEGIN PHASE2:getCountyByZipCode
    # @IN PHASE2:Iterate:Missing_County
    # @PARAM Zip
    # @OUT PHASE2:UpdateMissingCounty
        county = getCountyByZipCode(zip)
        _farmersData.iloc[indx, _farmersData.columns.get_loc('County')] = county
    # @END PHASE2:getCountyByZipCode
    

    # @BEGIN RemoveBlankLatitude/Longitude
    # @IN PHASE2:UpdateMissingCounty
    _farmersData = _farmersData.loc[pd.notnull(_farmersData['x']) & pd.notnull(_farmersData['y'])]
    # @END RemoveBlankLatitude/Longitude
    
    
    # @BEGIN Rename_x&y_To_Longitude&Latutude
    # @IN RemoveBlankLatitude/Longitude
    # @OUT CLEAN_DATASET
    _farmersData.rename(columns={'x': 'longitude', 'y': 'latitude'}, inplace=True)
    # @END Rename_x&y_To_Longitude&Latutude

    # @BEGIN ConvertDataframeToCSV
    # @IN CLEAN_DATASET
    # @OUT Clean_Farmers_Market_Data @URI file:{filepath}\farmersmarkets_clean_openrefine_python.csv
    _farmersData.to_csv("farmersmarkets_clean_openrefine_python.csv")
    # @END ConvertDataframeToCSV


# @END PYTHON-DATA-CLEANING-PROCESS


if __name__ == '__main__':
    # RUN THE CODE AS python3 FarmersMarketDataCleaning-YW.py PATH_0F_FARMERS_MARKET_DATA.CSV
    args = sys.argv[1:]
    filepath = os.path.join(f'{args}/', 'farmersmarkets_clean_openrefine.csv')

    print(filepath)
    main()