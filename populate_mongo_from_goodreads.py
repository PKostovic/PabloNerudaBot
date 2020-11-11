import requests
import hashlib
import pymongo
import logging
import sys
from bs4 import BeautifulSoup, SoupStrainer

def find_number_of_pages(url): 
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    page_link_hrefs = soup.findAll('a', href=lambda href: href and 'Pablo_Neruda?page' in href) 
    tags = []
    for tag in page_link_hrefs:
        if not tag.has_attr('rel'):
            tags.append(tag) 
    return int(tags[-1].contents[0])


def update_mongo(page_soup):
    # create an int that'll track the number of quotes pulled from this page
    num_quotes_from_page = 0
    num_collisions_on_page = 0
    
    # Take the parsed text and pull out just the quote text
    raw_quotes = page_soup.findAll('div', {'class': 'quoteText'}) 
    for quote in raw_quotes:
        num_quotes_from_page += 1
        # this pulls out all of the quotes, preserving line break elements between them. 
        pretty_quote = quote.prettify()[quote.prettify().index('>')+1:quote.prettify().index('<span class')].replace('<br>', '').replace('<br/>','')
       
        # process the quote and normalize it by removing the random - characters at the end, and put them into a nice pretty string. 
        word_array = pretty_quote.split()
        if '\x9d' not in word_array[-1]:
            word_array.pop()
        clean_string = ''
        for x in range(len(word_array)):
            if x == range(len(word_array)):
                clean_string += word_array[x]
            else:
                clean_string += word_array[x] + ' '
        cleaned_quote_hash = hashlib.md5(clean_string.encode('utf-8')).hexdigest()
    
        # Now somehow add the thing to mongo if its not already there
        search_result = list(mongo_collection.find( \
                {'hash_value': str(cleaned_quote_hash)}))
        if len(search_result) == 0:
            insertion_dict = {'author': 'Pablo Neruda', 'quote_text': clean_string,  \
                            'hash_value': str(cleaned_quote_hash), \
                            'recently_used':False}

            x = mongo_collection.insert_one(insertion_dict)
        else:
            num_collisions_on_page += 1
    return num_quotes_from_page, num_collisions_on_page


def mongo_is_running(mongo_client, logger):
    try:
        mongo_client.server_info()
        return True
    except Exception as E:
        return False


def create_logger():
    # Define the logging format, set up the logger
    logger = logging.getLogger('Neruda_Puller')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('logs/neruda_bot.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

if __name__ == '__main__':
    # Make the logger
    logger = create_logger()
    logger.info('Starting run...')

    # Connect to the mongo instance, check that it's running
    logger.debug('Connecting to Mongo.')
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    if not mongo_is_running(mongo_client, logger):
        logger.error('MongoDB instance is not in running state, aborting.')
        sys.exit(1)
    mongo_db = mongo_client['neruda_quotes']
    mongo_collection = mongo_db['neruda_quote_collection']
    logger.debug('\tConnection established.')

    # set up the basics to query and pull the information from the page
    url = 'https://www.goodreads.com/author/quotes/4026.Pablo_Neruda'

    # Find number of pages
    num_pages = find_number_of_pages(url)
    logger.debug('Found ' + str(num_pages) + ' pages of quotes at URL: ' + url)

    ## For each page, loop over and run through the quote_collection function
    quote_nums = {}
    for x in range(num_pages):
        numerical_url = 'https://www.goodreads.com/author/quotes/4026.Pablo_Neruda?page=' + str(x+1)
        page = requests.get(numerical_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        quote_nums[x] = update_mongo(soup)
    logger.info('Database update complete. Summary of the changes:')
    new_quotes = 0
    for k in quote_nums:
        logger.info('\t Page: ' + str(k) + ' - Quotes, Collisions: ' + str(quote_nums[k]))
        new_quotes += (quote_nums[k][0] - quote_nums[k][1])
    logger.info('There were ' + str(new_quotes) + 'new quotes found during this run.')
