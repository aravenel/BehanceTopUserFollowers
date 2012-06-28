from celery.task import task
import requests

@task(max_retries=3, default_retry_delay=30)
def get_twitter_follwers(handle_list):
    """Get number of twitter followers for a given list of handles. Makes request
    in chunks of 100 to conserve API calls. 
    Return a dict with key: handle, value: followers"""
    twitter_url = r'https://api.twitter.com/1/users/lookup.json?include_entities=true&screen_name='
    handle_text = ",".join(handle_list)
    return_dict = {}

    t = requests.post(twitter_url + handle_text)
    if t.status_code == 200:
        #Update output list
        for user_json in t.json:
            return_dict[user_json['screen_name']] = user_json['followers_count']
    else:
        #Retry
        print "Twitter returned status of %s. %s more retries..." % (t.status_code)

    #return return_dict
    print return_dict
