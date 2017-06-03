#! /usr/bin/env python3.5

import csv
import requests
import re
import logging
import time
from bs4 import BeautifulSoup
from pprint import pprint


class CityScrape:

    BASE_URL = "https://www.vrbo.com"
    BASE_SEARCH_URL = BASE_URL + "/vacation-rentals"
    AJAX_URL = BASE_URL + "/ajax/review/unit/{}/getAllReviews"
    # if this number is greater than the
    # number of reviews, we will fetch them all
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

    def set_logging_config(self):
        # logs to file
        logging.basicConfig(level=logging.DEBUG,
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
        for city, state in self.CITY_LIST:
            # collects all the pages of results when searching for a city
            listingHrefs = self.get_all_listings_for_city(city, state)
            # visits each result to collect the data
            self.get_all_listing_data_for_city(listingHrefs)
            break

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
            # TODO: remove when done testing
            break
        return listingHrefs

    def get_all_listing_data_for_city(self, listingHrefs):
        data = []
        for href in listingHrefs:
            data.append(self.get_data_for_listing(href))
            # TODO: remove when done testing
            break

        # TODO: write data to relevant file format/structure
        logging.debug("Writing data to file...")
        logging.debug(data)

    def get_data_for_listing(self, href):
        """ Scrape the data for a specific listing
            :returns: listing data to be saved to csv or txt file
        """
        logging.debug("Getting data for {}".format(self.get_base_url() + href))
        soup = self.request_listing_data(href)
        self.get_all_reviews_from_listing(soup, href)
        # TODO: parse and scrape all the relevant data from the page
        return soup.head.title

    def get_all_reviews_from_listing(self, soup, href):
        """ Makes an api call to the website to get the json of all the reviews
        """
        logging.debug("Finding the listing data we need to make the ajax call for the reviews")
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

        self.request_all_review_data(apiListingID)

    def request_all_review_data(self, apiListingId):
        """ Gets the json data for all the reviews from a listing """
        apiReviewURL = self.get_ajax_url().format(apiListingId)
        logging.debug("Getting all review data for {}".format(apiReviewURL))
        apiParams = {"pageNum": 1, "pageSize": self.LARGE_PAGE_SIZE}
        reviews = requests.get(apiReviewURL, params=apiParams)
        reviewJson = reviews.json()
        pprint(reviewJson)

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
        logging.debug("Fetching listing for {}".format(self.get_base_url() + href))
        resp = requests.get(self.get_base_url() + href)
        return self.soupify(resp)

    def request_city_listing(self, city, state, pageNum = 1):
        """ Gets a specific page number of the results for a city
            :returns: a BeautifulSoup object of the results page
        """
        logging.debug("Getting page #{} of results for {}, {}".format(
                pageNum, city, state))
        cityParam = {"q": "{}, {}, USA".format(city, state), "page": pageNum}
        resp = requests.get(self.get_base_search_url(), params=cityParam)
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
        logging.debug("Determining page count for {}, {}".format(city, state))
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
        logging.debug("Page count is {}".format(pageCountNum[0]))
        return int(pageCountNum[0])


def main():
    cityScraper = CityScrape()
    cityScraper.scrape()

if __name__ == "__main__":
    main()
