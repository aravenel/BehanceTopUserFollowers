import logging
import os
import csv
from celery.task import task
from celery.utils.log import get_task_logger
import requests
import celeryconfig

#Setup logging
log_location = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'tasks.log')
logger = get_task_logger(__name__)
handler = logging.FileHandler(log_location)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

@task(max_retries=3, default_retry_delay=30)
def get_twitter_followers(chunk):
    """Get number of twitter followers for a given list of handles. Makes request
    in chunks of 100 to conserve API calls. 
    Return a dict with key: handle, value: followers"""

    #Mapping of handle to username to associate followers back to behance usernames
    #Can't pass Twitter Behance usernames, so need to split them out and put 
    #them back together again at the end
    handle_mapping = dict((v['twitter_handle'].upper(), k) for (k, v) in chunk.items() if v['twitter_handle'] is not None)
    handle_list = [v['twitter_handle'] for v in chunk.values() if v['twitter_handle'] is not None]

    twitter_url = r'https://api.twitter.com/1/users/lookup.json?include_entities=true&screen_name='
    handle_text = ",".join(handle_list)
    twitter_followers = {}

    t = requests.post(twitter_url + handle_text)
    if t:
        #logger.debug(t)
        logger.debug("Twitter status: %s" % t.status_code)
        #logger.debug("JSON: %s" % t.json)
        if t.status_code == 200:
            #return t.json
            #Update output list
            for user_json in t.json:
                twitter_followers[user_json['screen_name']] = user_json['followers_count']
        else:
            logger.error("Twitter returned code %s." % t.status_code)
            get_twitter_followers.retry()

    #Put it all back together again!
    for handle, followers in twitter_followers.items():
        try:
            chunk[handle_mapping[handle.upper()]]['twitter_followers'] = followers
        except KeyError, e:
            print "Unable to match twitter handle to username. Moving onto next."
            logger.error(e)
            logger.error("Unable to match twitter handle %s to username." % handle.upper())
            logger.error("chunk[] keys: \n%s" % '\n\t'.join(chunk.keys()))

    logger.debug("Done, passing chunk to write_to_file task.")
    write_to_file.subtask().delay(chunk)

@task()
def write_to_file(chunk):
    headers = ['Behance User Name', 'Behance Views', 'Twitter Handle',
        'Twitter Followers']

    #Determine if we need to write a header
    if not os.path.isfile(celeryconfig.csv_output):
        logger.debug("File doesnt exist, has_header = False")
        has_header = False
    elif os.path.getsize(celeryconfig.csv_output) == 0:
        logger.debug("Filesize is zero, has_header = False")
        has_header = False
    else:
        has_header = True

    #Do the writing
    with open(celeryconfig.csv_output, 'ab') as of:

        logger.debug("Writing chunk to csv file...")

        #Setup writer object
        writer = csv.DictWriter(of, headers)

        #Write the header row if it doesnt exist
        if not has_header:
            logger.debug("CSV file doesn't have header, writing one...")
            writer.writeheader()

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
                logger.debug("User didn't have twitter name.")

            writer.writerow(outrow)

