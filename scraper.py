#! /usr/bin/env python3.5

import configparser
import csv
import json
import logging
import os.path
import requests
import re
import time
from bs4 import BeautifulSoup
from pprint import pprint


class CityScrape:

    BASE_URL = "https://www.vrbo.com"
    BASE_SEARCH_URL = BASE_URL + "/vacation-rentals"
    AJAX_URL = BASE_URL + "/ajax/review/unit/{}/getAllReviews"
    # if this number is greater than the
    # number of reviews for a listing, we will fetch them all
    LARGE_PAGE_SIZE = 100000


    CITY_LIST = [ ("New York", "NY",),
                  ("Bridgeport", "CT",),
                  ("Stamford", "CT",),
                  ("Kingston", "NY",),
                  ("Newark", "NJ",),
                  ("Edison", "NJ",),
                  ("Torrington", "CT",),
                  ("Trenton", "NJ",),
                  ("Ewing", "NJ",),
                  ("San Francisco", "CA",),
                  ("San Jose", "CA",),
                  ("Oakland", "CA",),
                  ("Napa", "CA",),
                  ("Fremont", "CA",),
                  ("Sunnyvale", "CA",),
                  ("Santa Clara", "CA",),
                  ("Santa Cruz", "CA",),
                  ("Watsonville", "CA",),
                  ("Santa Rosa", "CA",),
                  ("Petaluma", "CA",),
                  ("Vallejo", "CA",),
                  ("Fairfield", "CA",),
                  ("Philidelphia", "PA",),
                  ("Camden", "PA",),
                  ("Vineland", "PA",),
                  ("Los Angeles", "CA",),
                  ("Long Beach", "CA",),
                  ("Santa Ana", "CA",),
                  ("Oxnard", "CA",),
                  ("Thousand Oaks", "CA",),
                  ("Ventura", "CA",),
                  ("Riverside", "CA",),
                  ("San Bernadino", "CA",),
                  ("Ontario", "CA",),
                  ("Phoenix", "AZ",),
                  ("Portland", "OR",),
                  ("Vancouver", "WA",),
                  ("Hillsboro", "OR",),
                  ("Cleaveland", "OH",),
                  ("Akron", "OH",),
                  ("Elyria", "OH",),
                  ("Boulder", "CO",),
                  ("Denver", "CO",),
                  ("Aurora", "CO",),
                  ("Boulder", "CO",),
                  ("New Orleans", "LA",),
                  ("Metairie", "LA",),
                  ("Hammond", "MS",),
                  ("Charlotte", "NC",),
                  ("Concord", "SC",),
                  ("Gastonia", "NC",),
                  ("Albemarle", "NC",),
                  ("Durham", "NC",),
                  ("Rraleigh", "NC",),
                  ("Myrtle Bech", "SC",),
                  ("Conway", "SC",),
                  ("Charleston", "SC",),
                  ("Wilmington", "NC",),
                  ("Georgetown", "SC",),
                  ("North Myrtle Beach", "SC",),
                  ("Virginia Beach", "VA",),
                  ("Norfolk", "VA",),
                  ("Newport News", "VA",),
                  ("Hampton", "VA",),
                  ("Elizabeth City", "NC",),
                  ("Kill Devil Hills", "NC",),
                  ("Savannah", "GA",),
                  ("Hinesville", "GA",),
                  ("Fort Stewart", "GA",),
                  ("Dallas", "TX",),
                  ("Fort Worth", "TX",),
                  ("Austin", "TX",),
                  ("Houston", "TX",),
                  ("Washington", "DC",),
                  ("Chicago", "IL",),
                  ("Evanston", "IL",)]

    def __init__(self):
        self.set_logging_config()
        self.set_csv_config()
        self.read_config()

    def read_config(self):
        self.config = configparser.ConfigParser()
        self.config.read('last_info.ini')
        self.last_city_num = self.config.getint('info', 'last_city_num')
        self.last_href_num = self.config.getint('info','last_href_num')

    def update_last_city_num(self, idx, city, state):
        self.config['info']['last_city_num'] = str(idx + 1)
        self.config['info']['last_city'] = city + ',' + state
        self.update_config_file()

    def update_last_href_num(self, idx, href):
        self.config['info']['last_href_num'] = str(idx + 1)
        self.config['info']['last_href'] = str(href)
        self.update_config_file()

    def update_config_file(self):
        with open('last_info.ini', 'w') as configfile:
            self.config.write(configfile)


    def set_csv_config(self):
        listing_fieldnames = ['listing_id', 'listing_title', 'latitude',
                'longitude', 'location_name', 'number_reviews',
                'average_rating', 'average_nightly_price', 'min_stay', 'sleeps',
                'bedrooms', 'bathrooms', 'property_type', 'internet',
                'member_since', 'response_time', 'response_rate',
                'calendar_last_updated', 'type', 'floor', 'sq_footage',
                'max_occupancy', 'building_type']

        exists = False
        if os.path.isfile('listing.csv'):
            exists = True

        listing_csv = open('listing.csv', 'a')
        self.listing_csv = csv.DictWriter(listing_csv,
                fieldnames=listing_fieldnames, quoting=csv.QUOTE_MINIMAL)

        if not exists:
            self.listing_csv.writeheader()


        review_fieldnames = ['listing_id', 'total_number_reviews', 'n_review',
                'reviewer_name', 'title', 'stars', 'stayed', 'source',
                'submitted']
        exists = False

        if os.path.isfile('review.csv'):
            exists = True

        review_csv = open('review.csv', 'a')
        self.review_csv = csv.DictWriter(review_csv,
                fieldnames=review_fieldnames, quoting=csv.QUOTE_MINIMAL)

        if not exists:
            self.review_csv.writeheader()

    def set_logging_config(self):
        # logs to file
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='logfile.log',
                            filemode='w')
        # define a Handler which writes INFO messages or higher to the sys.stderr
        self.console = logging.StreamHandler()
        self.console.setLevel(logging.DEBUG)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
        self.console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(self.console)

        # suppress requests and urllib3 logging messages
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    def scrape(self):
        """ Goes through all the cities and collects data for each city """
        for idx, locTuple in enumerate(self.CITY_LIST[self.last_city_num:]):
            city = locTuple[0]
            state = locTuple[1]
            # collects all the pages of results when searching for a city
            listingHrefs = self.get_all_listings_for_city(city, state)
            # visits each result to collect the data
            self.get_all_listing_data_for_city(listingHrefs)
            self.update_last_city_num(idx + self.last_city_num, city, state)

    def get_all_listings_for_city(self, city, state):
        """ Determines how many pages of results there are for a city, and
            then goes through that many pages and collects the href of each
            listing
            :returns: hrefs of all the listings for a city
        """
        listingHrefs = [] # will be a list of '/XXXXXXha' or '/XXXXXX' to visit
        # find out how many pages there are, and get all the listings
        # for all those pages
        pageCount = self.get_page_count(city, state)
        for page in range(1, pageCount + 1):
            listingHrefs += self.get_city_listing(city, state, page)
        return listingHrefs

    def get_all_listing_data_for_city(self, listingHrefs):
        # return because the last href we visited was the last
        # one of the city
        if self.last_href_num >= len(listingHrefs) - 1:
            return

        for idx, href in enumerate(listingHrefs[self.last_href_num:]):
            listing_data = self.get_data_for_listing(href)
            self.listing_csv.writerow(listing_data)
            logging.info("Writing listing data to file")
            logging.debug(listing_data)
            # add one because it's 0 based indexing
            self.update_last_href_num(idx + self.last_href_num, href)

    def get_data_for_listing(self, href):
        """ Scrape the data for a specific listing
            :returns: listing data to be saved to csv or txt file
        """
        logging.info("Getting data for {}".format(self.get_base_url() + href))
        soup = self.request_listing_data(href)
        soup = self.get_loaded_page(soup, href)
        numReviews = self.get_all_reviews_from_listing(soup, href)
        row = {}
        LISTING_ATTRS = {
            'listing_id': "href[1:]",
            'listing_title':"soup.find('span', class_='listing-headline-text').text.strip()",
            'latitude':"float(soup.find('meta', attrs={'property': 'homeaway:location:latitude'})['content'])",
            'longitude':"float(soup.find('meta', attrs={'property': 'homeaway:location:longitude'})['content'])",
            'location_name':"soup.find('a', class_='js-breadcrumbLink').text.strip()",
            'number_reviews':"numReviews",
            'average_rating':"float(re.search('(\d*\.\d*)', soup.find('div', class_='rating')['title']).group(0))",
            'average_nightly_price':"int(soup.find('div', class_='price-large').text.strip()[1:])",
            'min_stay':"int(re.search('\d+', soup.find(text = 'Minimum Stay').parent.nextSibling.nextSibling.text).group())",
            'sleeps':"int(soup.find(text = 'Sleeps').parent.nextSibling.text)",
            'bedrooms':"soup.find(text = 'Bedrooms').parent.nextSibling.nextSibling.text",
            'bathrooms':"int(soup.find(text = 'Bathrooms').parent.nextSibling.text)",
            'property_type':"soup.find('div', id='propertyType').nextSibling.nextSibling.find('li').text.strip()",
            'internet':"'Yes' if soup.find(text=re.compile('Internet')) else 'No'",
            'member_since':"re.search('(\d+)', soup.find('div', class_='advertiser-date').text.strip()).group(1)",
            'response_time':"soup.find(text=re.compile('response time', re.IGNORECASE)).parent.find('strong').text",
            'response_rate':"soup.find(text=re.compile('Response rate')).parent.find('strong').text",
            'calendar_last_updated':"soup.find(text=re.compile('Calendar last updated')).parent.find('strong').text",
            'type':"soup.find('div', id='propertyType').nextSibling.nextSibling.find('li').text.strip()",
            'floor':"re.search('(\d+)', soup.find('div', id='propertyType').nextSibling.nextSibling.nextSibling.nextSibling.find('li').text).group(1)",
            'sq_footage':"re.search('\d+', soup.find('div', text='Floor Area:').nextSibling.nextSibling.find('li').text).group(0)",
            'max_occupancy':"int(soup.find(text=re.compile('Max. occupancy')).parent.find('span').text.strip())",
            'building_type':"soup.find('div', id='buildingtype').nextSibling.nextSibling.find('li').text.strip()"
        }

        for attr, code in LISTING_ATTRS.items():
            try:
                row[attr] = eval(code)
            except:
                logging.error("Could not retrieve {} attribute!!".format(attr))
                row[attr] = ''

        return row

    def get_all_reviews_from_listing(self, soup, href):
        """ Makes an api call to the website to get the json of all the reviews
        """
        logging.info("Finding the listing data we need to make the ajax call for the reviews")
        soup = self.get_loaded_page(soup, href)
        data_spu = soup.select('li.dropdown.favorite-button.js-favoriteButtonView')
        apiListingID = data_spu[0]['data-spu']

        return self.request_all_review_data(apiListingID, href)

    def get_loaded_page(self, soup, href):
        """ Sometimes it seems the page isn't fully loaded before being
            returned, so we test again to make sure we get a fully loaded page
        """
        notSuccessful = True
        while notSuccessful:
            # find the data we need to make the ajax api call ourselves
            # use the select method here because we want multiple classes
            data_spu = soup.select('li.dropdown.favorite-button.js-favoriteButtonView')
            try:
                # finds the "vrbo-XXXXXX-XXXXXXX' or 'trips-XXXXXX-XXXXXXX'
                apiListingID = data_spu[0]['data-spu']
                notSuccessful = False
            except IndexError:
                logging.error("Failed to find the api-id information for a listing. Probably didn't have enough time for page to load. Retrying...")
                time.sleep(1)
                soup = self.request_listing_data(href)
        return soup


    def request_all_review_data(self, apiListingId, href):
        """ Gets the json data for all the reviews from a listing """
        apiReviewURL = self.get_ajax_url().format(apiListingId)
        logging.info("Getting all review data for {}".format(apiReviewURL))
        apiParams = {"pageNum": 1, "pageSize": self.LARGE_PAGE_SIZE}
        reviews = self.request_url(apiReviewURL, params=apiParams)
        reviewsJson = reviews.json()
        for reviewNum, review in enumerate(reviewsJson['list']):
            row = {}
            # we use the first character through the end since it's a url
            row['listing_id'] = href[1:]
            row['total_number_reviews'] = reviewsJson['pagingContext']['totalResults']
            row['n_review'] = reviewNum + 1 # adjust for zero indexed
            row['reviewer_name'] = review['reviewer']['nickname']
            row['title'] = review['headline']
            row['stars'] = review['rating']
            row['stayed'] = review['arrivalDate']
            row['source'] = 'VRBO'
            row['submitted'] = review['createdDate']
            logging.debug(row)
            self.review_csv.writerow(row)

        logging.info("Wrote review data to file")

        return reviewsJson['pagingContext']['totalResults']

    def get_city_listing(self, city, state, pageNum):
        """ Fetches and parses the listing hrefs for a page of
            results for a given city.
            :returns: hrefs for all the listings on one page of results
        """
        soup = self.request_city_listing(city, state, pageNum)
        return self.get_hrefs(soup)

    def request_listing_data(self, href):
        """ Fetches the page for a specific listing
            :returns: a BeautifulSoup object of the listing page
        """
        logging.info("Fetching listing for {}".format(self.get_base_url() + href))
        resp = self.request_url(self.get_base_url() + href)
        return self.soupify(resp)

    def request_city_listing(self, city, state, pageNum = 1):
        """ Gets a specific page number of the results for a city
            :returns: a BeautifulSoup object of the results page
        """
        logging.info("Getting page #{} of results for {}, {}".format(
                pageNum, city, state))
        cityParam = {"q": "{}, {}, USA".format(city, state), "page": pageNum}
        resp = self.request_url(self.get_base_search_url(), params=cityParam)
        return self.soupify(resp)

    def soupify(self, resp):
        """ Creates and returns a BeautifulSoup object for an html page """
        return BeautifulSoup(resp.text, "html.parser")

    def get_hrefs(self, soup):
        """ Finds all the hrefs of listings from a search result page
            :returns: the hrefs of all the listings on a search result page
        """
        rawListingIDs = soup.find_all('div', attrs={"data-spu": True})
        hrefs = [ id.find('a')['href'] for id in rawListingIDs if id.find('a') ]
        return hrefs

    def get_base_search_url(self):
        return self.BASE_SEARCH_URL

    def get_base_url(self):
        return self.BASE_URL

    def get_ajax_url(self):
        return self.AJAX_URL

    def get_page_count(self, city, state):
        """ Returns the number of pages of results for a city """
        logging.info("Determining page count for {}, {}".format(city, state))
        soup = self.request_city_listing(city, state)
        scripts = soup.find_all('script')
        scripts = list(filter(lambda script: script.attrs == {} and 'pageCount' in script.text,
                              scripts))

        assert(len(scripts) == 1 and 'Should only have 1 script with the pageCount: {}, {}'.format(city, state))
        script = scripts[0]
        pageCountNum = re.findall('"pageCount":(\d*),', script.text)
        assert(len(pageCountNum) != 0 and "Didn't find pageCount in script tag: {}, {}".format(city, state))
        # findall returns a list, so we want the first item
        # in the list and make it an int
        logging.info("Page count is {}".format(pageCountNum[0]))
        return int(pageCountNum[0])

    def request_url(self, url, params=None):
        notSuccessful = True
        resp = None
        while notSuccessful:
            try:
                resp = requests.get(url, timeout=5, allow_redirects=True, params=params)
                notSuccessful = False
            except ConnectionError:
                logging.error("Could not get page {}. Re-trying...".format(url))
                time.sleep(2)
            except Exception as e:
                logging.error("Unknown error occurred when requesting {}.  {}".format(url, e))
                time.sleep(2)
        return resp





def main():
    cityScraper = CityScrape()
    cityScraper.scrape()

if __name__ == "__main__":
    main()
