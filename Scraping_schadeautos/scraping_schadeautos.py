# Importing libraries
import time
import os
from selenium import webdriver 
from selenium.webdriver.common.by import By 
import time
import random
import pandas as pd
import concurrent.futures
import threading

# Number of pages to scrape
driver = webdriver.Chrome()

url = 'https://www.schadeautos.nl/en/search/damaged/passenger-cars/1/1/0/0/0/0/1/0'
driver.get(url)
# Get the maximum number of pages

max_page = driver.find_element(By.XPATH, '/html/body/section[2]/div/div/div[2]/div/div[1]/ul/li[13]/a').get_attribute('href')
# max_page = max_page.text
max_page = int(max_page.split("/")[-1])
car_posts = []
i = 1
car_data = []

import concurrent.futures
import threading

# Create a lock
lock = threading.Lock()

def scrape_cars_posts(page):
    url = f'https://www.schadeautos.nl/en/search/damaged/passenger-cars/1/1/0/0/0/0/1/{page}'
    driver.get(url)
    car_items = driver.find_elements(By.CSS_SELECTOR, '.car-inner.flexinner')

    # Local list to store car data for this page
    car_data_batch = []

    # Collect links of the cars
    for item in car_items:
        title = item.find_element(By.TAG_NAME, 'h2')
        car_link = title.find_element(By.TAG_NAME, 'a')
        car_title = car_link.text
        car_href = car_link.get_attribute("href")
        car_data_batch.append({'title': car_title, 'link': car_href})

    # Acquire the lock before modifying the shared list
    with lock:
        car_data.extend(car_data_batch)

    print(f"Scraped {len(car_data_batch)} car posts from page {page}")

# Scraping car posts links for each page using multithreading
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(scrape_cars_posts, range(1, max_page + 1))

# Create a DataFrame from the list of car data
df = pd.DataFrame(car_data)
df.to_csv('./CSVs/car_links.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv("./CSVs/car_data.csv")


# Create a new directory for each make if it doesn't exist
if not os.path.exists("schadeautos"):
    os.makedirs("shcadeautos")

img_links = []

# Create a lock
lock = threading.Lock()

def scrape_imgs_links(link):
    # collecting images of each car
    driver.get(link)
    time.sleep(random.uniform(1, 5))

    # select all the images
    imgs = driver.find_elements(By.CSS_SELECTOR, '.thumbs img')
    
    # get all the links of the imgs
    img_links_batch = []  
    for img in imgs:
        img_src = img.get_attribute('src')
        img_links_batch.append(img_src)

    # Acquire the lock before modifying the shared list
    with lock:
        img_links.extend(img_links_batch)
    
    print(f"Downloaded {len(img_links_batch)} images")

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(scrape_imgs_links, df['link'])



img_links_df = pd.DataFrame(img_links, columns=['img_links'])
img_links_df.to_csv('./CSVs/img_links.csv', index=False)


