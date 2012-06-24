from BeautifulSoup import BeautifulSoup
import requests
import csv
import re

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
#File location to place output
outfile = r'/home/ravenel/code/behancescraper/output.csv'

def parse_user_page(url):
    """Parse a single user profile page and return their twitter handle if 
    found on their page."""
    u = requests.get(user_page_root_url + url)
    user_soup = BeautifulSoup(u.text)
    social_links = user_soup.findAll('a', {'class':'be-font-inline social-icon'})
    try:
        for link in social_links:
            match = re.search('twitter', link['href'])
            if match:
                return link['href'].split('/')[-1] 
    except:
        #If no social links, return None
        return None

    #If have social links but not twitter, return None
    return None

def get_twitter_followers(handle):
    """Return number of twitter handlers for a given handle."""
    pass

def parse():
    """Parse the top users page and get list of users. For each user,
    call the parse_user_page method to get their twitter handle if exists.
    """

    #For each page, request page contents and parse
    with open(outfile, 'wb') as of:

        #Setup the output csv
        writer = csv.writer(of)
        #Write csv header
        writer.writerow(['User Name', 'Twitter Handle', 'Number of Followers'])

        #Walk through the user pages
        for page_num in range(1, number_of_pages + 1):
            page_url = root_url + str(page_num)
            #Get the list of users from the page
            r = requests.get(page_url)
            soup = BeautifulSoup(r.text)
            #For each user, get their link and parse this out for username
            user_links = soup.findAll('a', {'class':'user-name'})
            for user_link in user_links:
                user_url = user_link['href']
                user_name = user_url.replace('/', '')
                user_twitter = parse_user_page(user_url)
                user_twitter_followers = get_twitter_followers(user_twitter)

                print "User name: %s\tTwitter Handle: %s" % (user_name, user_twitter)

if __name__ == "__main__":
    parse()
