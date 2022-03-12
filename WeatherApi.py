
import json
import os
import requests
import time
from pprint import pprint

PATH = os.getcwd()

# write api response
def writeToJSONFile(dir_name, fileName, data):
    #filePathNameWExt = './' + dir_name + '/' + fileName + '.json'
    full_path = os.path.join(PATH, dir_name, fileName+'.json')
    with open(full_path, 'a') as fp:
        json.dump(data, fp)
        fp.write(",\n")
        fp.close()

# display Data
def show_data(data):
    date = data['dt']
    temp = data['main']['temp']
    temp_max = data['main']['temp_max']
    temp_min = data['main']['temp_min']
    latitude = data['coord']['lat']
    longitude = data['coord']['lon']

    print()
    print('Date : {}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(date))))
    print('Temperature : {} degree celcius'.format(temp))
    print('Temperature MAX : {} degree celcius'.format(temp_max))
    print('Temperature MIN : {} degree celcius'.format(temp_min))
    print('Latitude : {}'.format(latitude))
    print('Longitude : {}'.format(longitude))

# get data by location
def get_data_by_location():
    lat = input('Enter the latitude : ')
    lon = input('Enter the longitude : ')
    url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid=7cfd0d31f9639c5d17d68bf672f35dd4&units=metric'.format(lat, lon)
    res = requests.get(url)
    print(res)

    data = res.json()
    pprint(data)
    show_data(data)
    writeToJSONFile('input','weather_api_by_location',data)

# get data by city name
def get_data_by_city():
    city = input('Enter the City : ')
    url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid=7cfd0d31f9639c5d17d68bf672f35dd4&units=metric'.format(city)
    res = requests.get(url)
    print(res)

    data = res.json()
    pprint(data)
    show_data(data)
    writeToJSONFile('input','weather_api_by_city',data)

def main():
    print('1. Get data By city')
    print('2. Get data By location')
    choice = input('Enter your choice : ')

    if choice == '1':
        num_input = int(input('Enter number of entries you want to make : '))
        for i in range(num_input):
            get_data_by_city()

    else:
        num_input = int(input('Enter number of entries you want to make : '))
        for i in range(num_input):
            get_data_by_location()

if __name__ == '__main__':
    main()

