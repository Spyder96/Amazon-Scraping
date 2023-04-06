from  bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re
import datetime


URL="https://www.amazon.in/s?k=laptops&crid=1SOV30PVZQH87&sprefix=laptops%2Caps%2C273&ref=nb_sb_noss_1"

#request headers
Headers=({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0' , 'Accept-language':'en-US , en;q=0.5'})
current_datetime = datetime.datetime.now()
#first page

pages_available = True
page=1
while pages_available:
    webpage=requests.get(URL,headers=Headers)
    #Creating initial soup file
    soup = BeautifulSoup(webpage.content,"html.parser")
    #searching for product links available in the page
    links=soup.find_all("a",attrs={'class' : 'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})
    all_product_data=[]
    #getting reference link for next website
    for link in links:
        sublink=link.get('href')
        product_link = "https://www.amazon.in" + sublink
        product_webpage=requests.get(product_link,headers=Headers)
        prod_soup = BeautifulSoup(product_webpage.content,"html.parser")

        #product name
        try:
            product_name=prod_soup.find("span", attrs={'class': "a-size-large product-title-word-break"}).text.strip()
        except (AttributeError,ValueError) as e:
            continue
        #price
        try:    
            price_span=prod_soup.find("span",attrs={'class': "a-price aok-align-center reinventPricePriceToPayMargin priceToPay"})
            price_class=price_span.find("span",attrs={'class': "a-price-whole"}).text
            #price
            price =  int(''.join(re.findall(r'\d+',price_class)))
        except (AttributeError,ValueError) as e:
            price = 0

        #description
        try :
            descriptions_list= prod_soup.find("ul",attrs={'class': "a-unordered-list a-vertical a-spacing-mini"}).text.strip().split("    ")
            description='\n'.join(descriptions_list)
        except:
            description = "No Description available"


        #rating
        try:
            rating= prod_soup.find("a",attrs={ 'id':"acrCustomerReviewLink" ,'class':"a-link-normal"})

            #text format eg: 1,765 ratings 
            rating= rating.text.strip()
            number_of_ratings=  int(''.join(re.findall(r'\d+', rating)))
        except (AttributeError,ValueError) as e:
            number_of_ratings = 0


        #number of stars
        try:
            stars= prod_soup.find("a",attrs={'class':"a-popover-trigger a-declarative"}).text

            number_of_stars= float(stars.strip().split()[0])
        except (AttributeError,ValueError) as e:
            number_of_stars = 0



        #num of answered questions
        try: 
            answered_questions= (prod_soup.find("a",attrs={ 'class':"a-link-normal askATFLink"}).text.strip()).split()
            num_answered_questions = answered_questions[0]
        except (AttributeError,ValueError) as e:
            num_answered_questions = 0

        try:
            features = prod_soup.find_all("a",attrs={ 'class': "a-size-small a-link-normal a-text-normal"})
            feature_list = ", ".join([feature.text.strip() for feature in features])
        except (AttributeError,ValueError) as e:
            feature_list = "Not Available"

        final_product_data = [product_name, price, number_of_stars, number_of_ratings, num_answered_questions, feature_list, description, page, current_datetime ]
        
        all_product_data.append(final_product_data)
        
        #print(f'Product Completed = {product_name}')
    #checking for next page
    try :
        next_page=soup.find("a",attrs={ 'class': "s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"})
        next_page_link ="https://www.amazon.in"+ next_page.get('href')
        page = page+1
        URL = next_page_link
        print(f"Page :{page}")
    except:
        pages_available = False