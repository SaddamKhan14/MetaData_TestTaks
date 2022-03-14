#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Script contains function definition to make API request to
#       pull and load data from weather API
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/03/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import json
import os
import requests
import sys
import time
import traceback

from pprint import pprint

PATH = os.getcwd()

# write api response
def writeToJSONFile(
    dir_name,
    fileName,
    data
) -> None:
    """
        Explanation: Write API response as JSON file
        :param dir_name str: Directory name i.e. JSON file path
        :param fileName str: File name
        :param data dict: Response object with actual data
        :return: None
    """
    dict = {}
    dict['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data['dt']))
    dict['temp'] = data['main']['temp']
    dict['temp_max'] = data['main']['temp_max']
    dict['temp_min'] = data['main']['temp_min']
    dict['latitude'] = data['coord']['lat']
    dict['longitude'] = data['coord']['lon']
    dict['city'] = data['name']
    try:
        if os.path.exists(PATH):
            full_path = os.path.join(PATH, dir_name, fileName+'.json')
            with open(full_path, 'a') as fp:
                json.dump(dict, fp)
                fp.write(",\n")
                fp.close()
        else:
            print("Successfully validated local file input path")
    except (IOError, OSError) as err:
        print("ERROR: occured validate file path")
        print(err)
        traceback.print_exception(*sys.exc_info())
        sys.exit(0)

# display Data
def show_data(
    data
) -> None:
    """
        Explanation: Write API response as JSON file
        :param data dict: Response object with actual data
        :return: None
    """
    timestamp = data['dt']
    temp = data['main']['temp']
    temp_max = data['main']['temp_max']
    temp_min = data['main']['temp_min']
    latitude = data['coord']['lat']
    longitude = data['coord']['lon']
    city = data['name']

    print()
    print('Timestamp : {}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))))
    print('Temperature : {} degree celcius'.format(temp))
    print('Temperature MAX : {} degree celcius'.format(temp_max))
    print('Temperature MIN : {} degree celcius'.format(temp_min))
    print('Latitude : {}'.format(latitude))
    print('Longitude : {}'.format(longitude))
    print('Longitude : {}'.format(city))

# get data by location
def get_data_by_location() -> None:
    """
        Explanation: API request to fetch data by location i.e. latitude and longitude
        :return: None
    """
    lat = input('Enter the latitude : ')
    lon = input('Enter the longitude : ')
    try:
        url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid=7cfd0d31f9639c5d17d68bf672f35dd4&units=metric'.format(lat, lon)
        res = requests.get(url)
        res.raise_for_status()
        print(res)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err.response.txt)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh.response.txt)
        raise SystemExit(errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc.response.txt)
        raise SystemExit(errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt.response.txt)
        raise SystemExit(errt)
    except requests.exceptions.TooManyRedirects as errr:
        print("Timeout Error:", errr.response.txt)
        raise SystemExit(errr)
    data = res.json()
    pprint(data)
    show_data(data)
    writeToJSONFile('input','weather_api_by_location',data)

# get data by city name
def get_data_by_city():
    """
        Explanation: API request to fetch data by city name
        :return: None
    """
    city = input('Enter the City : ')
    try:
        url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid=7cfd0d31f9639c5d17d68bf672f35dd4&units=metric'.format(city)
        res = requests.get(url)
        res.raise_for_status()
        print(res)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err.response.txt)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh.response.txt)
        raise SystemExit(errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc.response.txt)
        raise SystemExit(errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt.response.txt)
        raise SystemExit(errt)
    except requests.exceptions.TooManyRedirects as errr:
        print("Timeout Error:", errr.response.txt)
        raise SystemExit(errr)
    data = res.json()
    pprint(data)
    show_data(data)
    writeToJSONFile('input','weather_api_by_city',data)

# weather api driver
def weather_api_data_pull():
    """
        Explanation: Driver method for making API requests
        :return: None
    """
    print('1. Get data By city')
    print('2. Get data By location')
    choice = input('Enter your choice : ')

    if choice == '1':
        num_input = int(input('Enter number of entries you want to make : '))
        for i in range(num_input):
            get_data_by_city()

    elif choice == '2':
        num_input = int(input('Enter number of entries you want to make : '))
        for i in range(num_input):
            get_data_by_location()

    else:
        print('Verify your input!')

