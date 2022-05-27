import requests
import lxml.html
import backoff as backoff
import pandas as pd
import re

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
def scrape_housing_website(url):
    # This code creates an xml tree from the input url
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    tree = lxml.html.fromstring(response.text)
    # CREATES A LIST OF ALL OF THE PRICES
    my_prices = []
    for element in tree.xpath("//li[@name='Price']"):
        result = re.sub("\r\n", "", element.text)
        result = re.sub(" ", "", result)
        my_prices.append(result)
    # CREATES A LIST OF ADDRESS
    my_addresses = []
    for element in tree.xpath("//li[@name='Price']/parent::ul/parent::div/parent::div/parent::div/header/div[@class='header-col']/h4/a"):
       my_addresses.append(element.text)
    # CREATES A LIST OF THE CITIES
    my_dumb_cities = []
    my_cities = []
    for element in tree.xpath("//li[@name='Price']/parent::ul/parent::div/parent::div/parent::div/a"):
       my_dumb_cities.append(element.get("title"))
    i = 0
    j = 0
    for item in my_dumb_cities:
        if(i % 2 == 0):
            j = i / 2
            result = re.sub(my_addresses[int(j)] + ", ", "", item)
            my_cities.append(result)
        i = i + 1
    # All three of these lists are returned as the output of this function
    return(my_prices, my_addresses, my_cities)

def housing_data_frame_creator(county):
    # This code creates 3 lists of lists containing all of the listing names, prices, and buying options
    # The input search phrase is edited, so that the spaces are replaced by "+". I thought it would be weird if the search phrase originally contained "+" instead of spaces, so I made it this way. 
    # It uses a while loop paired with a try and except statement in order to utilize the backoff statement I linked to the scrape_ebay_website function in order to prevent my connection from being blocked by eBay.
    # I used a for loop and an f string to load different pages after I saw this pattern for eBay urls. The pattern is what I input into the scrape_ebay_website function. 
    final_prices = []
    final_addresses = []
    final_cities = []
    county_adjusted = county.lower().replace(" ", "-")
    condition = True
    page = 1
    while(condition == True):
        prices = []
        addresses = []
        cities = []
        while True:
            try:
                prices, addresses, cities = scrape_housing_website(f'https://www.loopnet.com/search/retail-space/{county_adjusted}-county-in/for-lease/{page}/')
                break
            except:
                pass
        if(len(prices) > 0):
            final_prices.append(prices)
            final_addresses.append(addresses)
            final_cities.append(cities)
            page = page + 1
        if(len(prices) == 0):
            condition = False
    prices_column = []
    addresses_column = []
    cities_column = []
    counties_column = []
    for list in final_prices:
        for value in list:
            prices_column.append(value)
    for list in final_addresses:
        for value in list:
            addresses_column.append(value)
    for list in final_cities:
        for value in list:
            cities_column.append(value)
    for i in range(0, len(prices_column)):
        counties_column.append(county)
    # This code creates a dict my_data containing the three lists that contain all of our data
    my_data = {'County': counties_column, 'Prices': prices_column, 'Address': addresses_column, 'City': cities_column}
    # This code creates and returns a data.frame created from the my_data dict
    myDF = pd.DataFrame(my_data)
    return(myDF)

counties_list1 = ["Adams", "Allen", "Bartholomew", "Benton", "Blackford", "Boone", "Brown", "Carroll", "Cass", "Clark", "Clay", "Clinton", "Crawford", "Daviess", "Dearborn", "Decatur", "De Kalb", "Delaware", "Dubois", "Elkhart", "Fayette", "Floyd", "Fountain", "Franklin", "Fulton", "Gibson", "Grant", "Greene", "Hamilton", "Hancock", "Harrison", "Hendricks", "Henry", "Howard", "Huntington", "Jackson", "Jasper", "Jay", "Jefferson", "Jennings", "Johnson", "Knox", "Kosciusko", "La Porte", "Lagrange", "Lake", "Lawrence", "Madison", "Marion", "Marshall", "Martin", "Miami", "Monroe", "Montgomery", "Morgan", "Newton", "Noble", "Ohio", "Orange", "Owen", "Parke", "Perry", "Pike", "Porter", "Posey", "Pulaski", "Putnam", "Randolph", "Ripley", "Rush", "St Joseph", "Scott", "Shelby", "Spencer", "Starke", "Steuben", "Sullivan", "Switzerland", "Tippecanoe", "Tipton", "Union", "Vanderburgh", "Vermillion", "Vigo", "Wabash", "Warren", "Warrick", "Washington", "Wayne", "Wells", "White", "Whitley"]
len(counties_list1)
myDF1 = pd.DataFrame()
for county in counties_list1:
    myDummyDF = pd.DataFrame()
    myDummyDF = housing_data_frame_creator(county)
    myDF1 = pd.concat([myDF1, myDummyDF], ignore_index=True)
myDF1.tail()
myDF1.shape[0]


compression_opts = dict(method='zip', archive_name='out.csv')  
myDF1.to_csv('Indiana_Retail_Lease.zip', index=False, compression=compression_opts)  
