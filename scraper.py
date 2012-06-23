from BeautifulSoup import BeautifulSoup
import requests
import csv

root_url = 'http://www.behance.net/?content=users&sort=views&time=all&page='
number_of_pages = 1

def parse_user_page(url):
    pass

def parse():
#For each page, request page contents and parse
    for page_num in range(0, number_of_pages):
        page_url = root_url + str(page_num)

if __name__ == "__main__":
    parse()
