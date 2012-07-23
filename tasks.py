import logging
import os
from celery.task import task
from celery.utils.log import get_task_logger
import requests

log_location = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'tasks.txt')
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
            #print t.status_code
            #print "Twitter returned status of %s." % (t.status_code)
            return "Error: %s" % t.status_code

    #return return_dict
    print return_dict
