import logging
import os
import csv
from celery.task import task
from celery.utils.log import get_task_logger
import requests
import celeryconfig

log_location = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'tasks.log')
logger = get_task_logger(__name__)
handler = logging.FileHandler(log_location)
logger.addHandler(handler)

@task(max_retries=3, default_retry_delay=30)
def get_twitter_followers(handle_list):
    """Get number of twitter followers for a given list of handles. Makes request
    in chunks of 100 to conserve API calls. 
    Return a dict with key: handle, value: followers"""

    twitter_url = r'https://api.twitter.com/1/users/lookup.json?include_entities=true&screen_name='
    handle_text = ",".join(handle_list)
    return_dict = {}

    t = requests.post(twitter_url + handle_text)
    if t:
        #logger.debug(t)
        logger.debug("Status Code: %s" % t.status_code)
        #logger.debug("JSON: %s" % t.json)
        if t.status_code == 200:
            #return t.json
            #Update output list
            for user_json in t.json:
                return_dict[user_json['screen_name']] = user_json['followers_count']
        else:
            #return "Error: %s" % t.status_code
            get_twitter_followers.retry()

    #Call write_to_file subtask to write results to csv
    print return_dict

@task
def write_to_file(chunk):
    with open(celeryconfig.csv_output, 'ab') as of:

        headers = ['Behance User Name', 'Behance Views', 'Twitter Handle',
            'Twitter Followers']

        #Determine if we have header row
        has_header = csv.Sniffer().sniff(of.read(1024)).has_header()

        #Setup writer object
        writer = csv.DictWriter(of, headers)

        #Write the header row if it doesnt exist
        if not has_header:
            writer.Writeheader()

        for user, user_data in chunk.items():

            outrow = {}
            outrow['Behance User Name'] = user
            outrow['Behance Views'] = user_data['behance_views']
            outrow['Twitter Handle'] = user_data['twitter_handle']
            #Handle those that didn't have results
            try:
                outrow['Twitter Followers'] = user_data['twitter_followers']
            except KeyError:
                outrow['Twitter Followers'] = 'N/A'

            writer.writerow(outrow)

