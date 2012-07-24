from __future__ import division #WTF, python. Really?
import csv
import time
import re
import os
import logging
import tasks

##################################################
#
#   Config Settings
#
##################################################
#File location to place output
outfile = os.path.join(os.path.dirname(__file__), 'output.csv')
#Location of csv file with behance names, twitter handles, views
src = r'users.csv'
#Rate limit per hour for twitter
rate_limit = 150

##################################################
#
#   Don't change these
#
##################################################
_rate_per_second = (rate_limit - 5) / 60 / 60
_twitter_match = re.compile(r'([A-Za-z0-9_]+)')

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
                print "Rate limited: sleep for %s seconds..." % leftToWait,
                time.sleep(leftToWait)
            ret = func(*args, **kwargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate

def _scrub_twitter_handle(handle):
    """Clean a twitter handle to remove any extraneous symbols."""
    #Clean it up--dirty data
    handle = handle.strip().replace('@', '').replace('#!', '').replace('/', '')
    #Check that it uses valid twitter characters--else return None
    match = _twitter_match.match(handle)
    #if handle == '-' or handle == '#!' or handle == '':
        #return None
    if match:
        return handle
    else:
        return None

def parse_from_csv(csv_location):
    """Get user twitter information from a csv file that contains list of
    behance users and twitter names."""

    #Dict to hold user data
    users = {}

    #Parse list of Behance users and twitter handles
    with open(csv_location, 'rb') as f:
        print "Parsing csv of user data...",
        reader = csv.reader(f)
        for row in reader:
            users[row[0]] = {}
            users[row[0]]['twitter_handle'] = _scrub_twitter_handle(row[1])
            users[row[0]]['behance_views'] = row[2]
        print "Done."

    #Walk through the csv values
    for chunk in chunk_dict(users):

        print "parsing 100 users..."
        tasks.get_twitter_followers.delay(chunk)

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log.txt'),
            format='%(asctime)s\t%(message)s', level=logging.INFO)
    parse_from_csv(src)
