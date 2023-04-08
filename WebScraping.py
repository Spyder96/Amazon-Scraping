from  bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import json
import re
import datetime
import psycopg2 as pg
import time
from psycopg2 import extras  as pgextras

def create_database(connection):
    conn=connection
    cur = conn.cursor()
    # Execute the SQL query
    try:
        cur.execute("CREATE Database amazon")
    except pg.errors.DuplicateDatabase:
        print("Database Amazon Exists")
    except Exception as e:
        print (f'Error Creating Database: {e}')
        conn.rollback()
    cur.close()


def create_table(connection,name):
    # 
    cursor = connection.cursor()
    try:
        create_table_query= """
              Create table %s (
                  id bigserial Primary Key,
                  Name varchar not null,
                  Price real  ,
                  Stars real ,
                  Number_of_Ratings int, 
                  Number_of_Answered_Questions int, 
                  Amazon_offerings text, 
                  Brief_Description text,
                  product_link text,
                  Page  int ,
                  Date timestamp
                  )         
                       """
        cursor.execute(create_table_query,(pg.extensions.AsIs(name),))
    except pg.errors.DuplicateTable:
        print("table exists ")
    except Exception as e:
        print(f'Error creating table: {e}')
        
    cursor.close()
    #connection.close()

def table_inserts_df(name,df,connection):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols=','.join(list(df.columns))
    cursor = connection.cursor()
    insert_query = """insert into %s(%s) values %%s  """ % (name,cols)
    try:
        pgextras.execute_values(cursor, insert_query, tuples)
        connection.commit()
    except(Exception, pg.DatabaseError) as error:
        print("Error inserting values: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    print('values inserted to db')
    cursor.close()
    return 0

def soup_table_data(table):
    data={}
    for row in table.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        key = cells[0].text.strip()
        value = cells[1].text.strip()
        value = value.replace('\u200e', '')
        # clean the value by removing unnecessary characters
        value = re.sub('\n', '', value)
        value = re.sub('\s+', ' ', value)

        data[key] = value
    return data


      
        
search = input("enter")
URL= f"https://www.amazon.in/s?k={search}&crid=2C9GZV1PQRGTM&sprefix=abc%2Caps%2C501&ref=nb_sb_noss_2"
#request headers
Headers=({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0' , 'Accept-language':'en-US , en;q=0.5'})


#postgres connection
conn = pg.connect("host=localhost dbname=postgres user=postgres password=admin")
conn.autocommit = True
create_database(conn)

#connecting to Amazon database
connection = pg.connect("host=localhost dbname=amazon user=postgres password=admin")
connection.autocommit = True

#Crateing table
table_name = search
create_table(connection,table_name)

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
        
        
        #dates
        date_time = datetime.datetime.now()
        current_datetime = date_time.strftime("%Y-%m-%d %H:%M:%S")
        
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
            price =  float(''.join(re.findall(r'\d+',price_class)))
        except (AttributeError,ValueError) as e:
            price = 0

        #description
        try :
            descriptions_list= prod_soup.find("ul",attrs={'class': "a-unordered-list a-vertical a-spacing-mini"}).text.strip().split("    ")
            #description='\n'.join(descriptions_list)
            description= json.dumps(descriptions_list)
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
            num_answered_questions = int(''.join(re.findall(r'\d+', answered_questions[0])))
        except (AttributeError,ValueError) as e:
            num_answered_questions = 0
        
        #Amazon highlights
        try:
            features = prod_soup.find_all("a",attrs={ 'class': "a-size-small a-link-normal a-text-normal"})
            #feature_list = ", ".join([feature.text.strip() for feature in features])
            feature_list = json.dumps([feature.text.strip() for feature in features])
        except (AttributeError,ValueError) as e:
            feature_list = "Not Available"
            
        #Scraping techincal data
        tech_table = prod_soup.find('table', attrs={'class':'a-keyvalue prodDetTable'})
        technical_details= soup_table_data(tech_table)
        
        #Scraping Addional data
        add_div = prod_soup.find( 'div', attrs={ 'id':"productDetails_db_sections", 'class':"a-section"})
        add_table = add_div.find( 'table', attrs={ 'id':"productDetails_detailBullets_sections1", \
                                                      'class':"a-keyvalue prodDetTable"})
        
        additional_details= soup_table_data(add_table) 
        
        
        
        final_product_data = {
            'ASIN'               : additional_details['ASIN'],
            'Name'               : product_name,
            'Price'              : price, 
            'Stars'              : number_of_stars, 
            'Ratings'            : number_of_ratings,
            'Answered_Questions' : num_answered_questions,
            'Amazon_Services'    : feature_list,
            'Description'        : description,
            'Technical_details'  : technical_details,
            'Additional_details' : additional_details,
            'Link'               : product_link,
            'Page'               : page,
            'Date'               : current_datetime }
        
        all_product_data.append(final_product_data)
        time.sleep(1)
        
        #creating a dataframe of the products
    #df = pd.DataFrame(all_product_data, columns=['Name', 'Price' , 'Stars', 'Number_of_Ratings', 'Number_of_Answered_Questions', 'Amazon_offerings', 'Brief_Description', 'product_link', 'Page' , 'Date'])
     
    #inserting data
    #table_inserts_df(search, df, connection)
    #checking for next page
    try :
        next_page=soup.find("a",attrs={ 'class': "s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"})
        next_page_link ="https://www.amazon.in"+ next_page.get('href')
        page = page+1
        URL = next_page_link
        print(f"Page : {page} ")
    except:
        pages_available = False