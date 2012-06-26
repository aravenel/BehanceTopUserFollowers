import requests
import csv
import time

##################################################
#
#   Config Settings
#
##################################################
#URL of listing of top users. Page num will be appended.
root_url = 'http://www.behance.net/?content=users&sort=views&time=all&page='
#Root user of users. User name will be appended.
user_page_root_url = r'http://www.behance.net'
#Number of pages to check
number_of_pages = 2
#Page number to start on
start_page = 1
#File location to place output
outfile = r'/home/ravenel/code/BehanceTopUserFollowers/output.csv'
#Location of csv file with behance names, twitter handles, views
src = r'users.csv'

def chunks(l, n=100):
    """Yield n-sized chunks from list l"""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def chunk_dict(d, n=100):
    """Yield n-sized chunks of dict d"""
    keys = d.keys()
    for i in xrange(0, len(d), n):
        yield dict((k, v) for (k, v) in d.items() if k in keys[i:i+n])

def RateLimited(maxPerSecond):
    """Rate limit decorator"""
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args, **kwargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                print "Rate limited: sleep for %s seconds..." % leftToWait
                time.sleep(leftToWait)
            ret = func(*args, **kwargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate

def _scrub_twitter_handle(handle):
    """Clean a twitter handle to remove any extraneous symbols."""
    if handle == '-' or handle == '#!' or handle == '':
        return None
    handle.replace('@', '').replace('#!', '').replace('/', '')
    return handle

#Rate limit to 150 requests/hour for Twitter
@RateLimited(0.04)
def _get_twitter_followers_chunked(handle_list):
    """Get number of twitter followers for a given list of handles. Makes request
    in chunks of 100 to conserve API calls. 
    Return a dict with key: handle, value: followers"""
    print "Getting twitter data for 100 users...",
    twitter_url = r'https://api.twitter.com/1/users/lookup.json?include_entities=true&screen_name='
    handle_text = ",".join(handle_list)
    return_dict = {}

    t = requests.post(twitter_url + handle_text)
    if t.status_code == 200:
        #Update output list
        for user_json in t.json:
            return_dict[user_json['screen_name']] = user_json['followers_count']
    else:
        #Update with errors
        return_dict = dict((k, "Twitter Error: %s" % t.status_code) for k in handle_list)

    print "Done."

    return return_dict

def _twitterfy_chunk(chunk):
    """Take a chunk of the user dict and update it with twitter data."""

    #Mapping of handle to username to associate followers back to behance usernames
    handle_mapping = dict((v['twitter_handle'].upper(), k) for (k, v) in chunk.items() if v['twitter_handle'] is not None)
    handle_list = [v['twitter_handle'] for v in chunk.values() if v['twitter_handle'] is not None]

    #Call twitter API
    twitter_followers = _get_twitter_followers_chunked(handle_list)

    #Update chunk
    for handle, followers in twitter_followers.items():
        chunk[handle_mapping[handle.upper()]]['twitter_followers'] = followers

    return chunk

def parse_from_csv(csv_location):
    """Get user twitter information from a csv file that contains list of
    behance users and twitter names."""

    #Dict to hold user data
    users = {}

    #Parse list of Behance users and twitter handles
    with open(csv_location, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            users[row[0]] = {}
            users[row[0]]['twitter_handle'] = _scrub_twitter_handle(row[1])
            users[row[0]]['behance_views'] = row[2]

    with open(outfile, 'wb') as of:
        #Setup output csv
        headers = ['Behance User Name', 'Behance Views', 'Twitter Handle',
            'Twitter Followers']
        writer = csv.DictWriter(of, headers)
        writer.writerow(dict((v, v) for v in headers))

        #Walk through the csv values
        for chunk in chunk_dict(users):

            #Update chunk with twitter follower counts
            twitterfied_chunk = _twitterfy_chunk(chunk)

            print "Writing chunk to csv...",
            for user, user_data in twitterfied_chunk.items():

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
            print "Done."


if __name__ == "__main__":
    parse_from_csv(src)
