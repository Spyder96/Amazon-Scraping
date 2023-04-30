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
import pymongo
from tqdm import tqdm
import os
import logging

def main_postgres_connection(dbname):
    
    if dbname == "postgres":  #postgres connection
        connection = pg.connect("host=localhost dbname=postgres user=postgres password=admin")
        connection.autocommit = True
        create_database(connection)
        return 0
    else:
        #connecting to Amazon database
        connection = pg.connect("host=localhost dbname=amazon user=postgres password=admin")
        connection.autocommit = True
        return connection

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


def Mongodb_connection():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    #myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["Amazondb"]
    mycol = mydb["Products"]
    return mycol


def soup_table_data(table):
    data = {}
    if table is None:
        data = {"No Data Available" : "No Data"}
    else:
        rows = table.find_all('tr')
        if rows:
            for row in rows:
                cells = row.find_all(['th', 'td'])
                key = cells[0].text.strip()
                value = cells[1].text.strip()
                value = value.replace('\u200e', '')
                # clean the value by removing unnecessary characters
                value = re.sub('\n', '', value)
                value = re.sub('\s+', ' ', value)
                key = re.sub(' ', '_' , key)

                data[key] = value
        else:
            data = {"No Data Available" : "No data"}
    return data


def product_details(link,search,page,Headers,table):
    date_time = datetime.datetime.now()
    current_datetime = date_time.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        product_webpage=requests.get(link,headers=Headers)
    except Exception as e:
        logging.error(f"Invalid Product link : {e}")
        logging.debug(f"Product_link= {link}")
        return 0
    
    prod_soup = BeautifulSoup(product_webpage.content,"html.parser")

    #product name
    try:
        product_name=prod_soup.find("span", attrs={'class': "a-size-large product-title-word-break"}).text.strip()
        logging.debug(f"Product Name= {product_name}")
    except Exception as e:
        logging.error(f" Product Name Unavailable : {e}")
        return 0
    
    #price
    try:    
        price_span=prod_soup.find("span",attrs={'class': "a-price aok-align-center reinventPricePriceToPayMargin priceToPay"})
        price_class=price_span.find("span",attrs={'class': "a-price-whole"}).text
        #price
        price =  float(''.join(re.findall(r'\d+',price_class)))
        logging.debug(f"Product Price= {price}")
    except Exception as e:
        logging.error(f"Product price Unavailable : {e}")
        price = 0

    #description
    try :
        descriptions_list= prod_soup.find("ul",attrs={'class': "a-unordered-list a-vertical a-spacing-mini"}).text.strip().split("    ")
        #description='\n'.join(descriptions_list)
        description= json.dumps(descriptions_list)
        logging.debug(f"Product description= {description}")
        
    except Exception as e:
        logging.error(f"Product description Unavailable : {e}")
        description="No Description"

    #rating
    try:
        rating= prod_soup.find("a",attrs={ 'id':"acrCustomerReviewLink" ,'class':"a-link-normal"})
        #text format eg: 1,765 ratings 
        rating= rating.text.strip()
        number_of_ratings=  int(''.join(re.findall(r'\d+', rating)))
        logging.debug(f"Product number_of_ratings= {number_of_ratings}")
    except Exception as e:
        logging.error(f"Product ratings Unavailable : {e}")
        number_of_ratings = 0
        
        
    #number of stars
    try:
        stars= prod_soup.find("a",attrs={'class':"a-popover-trigger a-declarative"}).text
        number_of_stars= float(stars.strip().split()[0])
        logging.debug(f"Product number_of_stars= {number_of_stars}")
    except Exception as e:
        logging.error(f"Product Stars Unavailable : {e}")
        number_of_stars = 0


    #num of answered questions
    try: 
        answered_questions= (prod_soup.find("a",attrs={ 'class':"a-link-normal askATFLink"}).text.strip()).split()
        num_answered_questions = int(''.join(re.findall(r'\d+', answered_questions[0])))
        logging.debug(f"Product num_answered_questions= {num_answered_questions}")
    except Exception as e:
        logging.error(f"Product Answered question Unavailable : {e}")
        num_answered_questions = 0
    
    #Amazon highlights
    try:
        features = prod_soup.find_all("a",attrs={ 'class': "a-size-small a-link-normal a-text-normal"})
        #feature_list = ", ".join([feature.text.strip() for feature in features])
        feature_list = json.dumps([feature.text.strip() for feature in features])
        logging.debug(f"Product feature_list= {feature_list}")
    except Exception as e:
        logging.error(f"Product highlights Unavailable : {e}")
        feature_list = "Not Available"
        
    #Scraping techincal data
    try:
        tech_table = prod_soup.find('table', attrs={'class':'a-keyvalue prodDetTable'})
        technical_details= soup_table_data(tech_table)
        logging.debug(f"Product technical_details= {technical_details}")
    except Exception as e:
        logging.error(f"Product techincal data Unavailable : {e}")
        technical_details = "Not Available"
    
    
    #Scraping Addtional data
    try :
        add_div = prod_soup.find( 'div', attrs={ 'id':"productDetails_db_sections", 'class':"a-section"})
        add_table = add_div.find( 'table', attrs={ 'id':"productDetails_detailBullets_sections1", \
                                                      'class':"a-keyvalue prodDetTable"})
        additional_details= soup_table_data(add_table)
        logging.debug(f"Product additional_details= {additional_details}")
    except Exception as e:
        logging.error(f"Product Addtional data Unavailable : {e}")
        additional_details="Not Available"
        
    final_product_data = {
        #'ASIN'               : additional_details['ASIN'],
        'Name'               : product_name,
        'Price'              : price, 
        'Stars'              : number_of_stars, 
        'Ratings'            : number_of_ratings,
        'Answered_Questions' : num_answered_questions,
        'Amazon_Services'    : feature_list,
        'Description'        : description,
        'Technical_details'  : technical_details,
        'Additional_details' : additional_details,
        'Link'               : link,
        'Page'               : page,
        'Date'               : current_datetime,
        'Search'             : search
        }
    
    x = table.insert_one(final_product_data)
    return final_product_data
      
def Amazon_search(search):
    Headers=({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0' , 'Accept-language':'en-US , en;q=0.5'})
    dbtable=Mongodb_connection()
    URL= f"https://www.amazon.in/s?k={search}&crid=2C9GZV1PQRGTM&sprefix=abc%2Caps%2C501&ref=nb_sb_noss_2"
    pages_available = True
    page=1
    
    while pages_available:
        
        try: 
            webpage=requests.get(URL,headers=Headers)
        except Exception as e:
            logging.error(f"{page}-Page Unavailable : {e}") 
        #Creating initial soup file
        soup = BeautifulSoup(webpage.content,"html.parser")
        #searching for product links available in the page
        links=soup.find_all("a",attrs={'class' : 'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})
        all_product_data=[]
        #iterating the reference links for the listed products
        
        for link in tqdm(links,desc="Processing items"):
            #dates
            sublink=link.get('href')
            product_link = "https://www.amazon.in" + sublink
            
            data = product_details (product_link,search,page,Headers,dbtable)
            

            if data == 0 :
                continue
            else:
                all_product_data.append(data)
                global_prod_list.append(data)
            time.sleep(1)
            
            #creating a dataframe of the products
        #df = pd.DataFrame(all_product_data, columns=['Name', 'Price' , 'Stars', 'Number_of_Ratings', 'Number_of_Answered_Questions', 'Amazon_offerings', 'Brief_Description', 'product_link', 'Page' , 'Date'])
        #table.insert_many(all_product_data)
        
        #inserting data
        #table_inserts_df(search, df, connection)
        #checking for next page
        try :
            next_page=soup.find("a",attrs={ 'class': "s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"})
            next_page_link ="https://www.amazon.in"+ next_page.get('href')
            page = page+1
            URL = next_page_link
            print(f"Page : {page} ")
        except Exception as e:
            logging.error(f"No More Pages Available : {e}")            
            pages_available = False

        # if page>10:
        #     pages_available = False

     
    
def main():
    search = input("Enter what you want to search in Amazon : ")
    date_time = datetime.datetime.now()
    date = date_time.strftime("%Y-%m-%d")
    log_dir = 'log'
    data_dir = 'prod_data'
    # create the log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    # configure the logging system to write logs to a file
    log_file_name= f'myapp_{date}.log'
    log_file = os.path.join(log_dir, log_file_name)
    logging.basicConfig(filename=log_file, level=logging.DEBUG, \
                        format='%(asctime)s %(levelname)s: %(message)s')
    
    #confirguring for data file
    data_file_name=f"{search}_{date}.json"
    data_dir_path = os.path.join(data_dir, data_file_name)
    
    #checking if file already exists and loading data if available
    if os.path.isfile(data_dir_path):
        with open(data_dir_path, "r") as f:
            # Read the file line by line
            for line in f:
                # Strip any whitespace and parse the JSON document
                data = json.loads(line.strip())
                # Append the data to the global_prod_list list
                global_prod_list.append(data)

    Amazon_search(search)
    

    with open(data_dir_path,"a") as f:
        if len(global_prod_list) < 2:
            print("No Data")
        else:
            json.dump( global_prod_list,f,default=str)
#request headers
   
   
if __name__ == "__main__" :
        
    global_prod_list=[]
    main()




# connection=main_postgres_connection("amazon")
# #Creating table
# table_name = search
# create_table(connection,table_name)

#first page
