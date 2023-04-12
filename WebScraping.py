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

    with open(data_dir_path,"a") as f:
        if len(global_prod_list) < 2:
            print("No Data")
        else:
            json.dump( global_prod_list,f,default=str)
#request headers



global_prod_list=[]
main()
# connection=main_postgres_connection("amazon")
# #Creating table
# table_name = search
# create_table(connection,table_name)

#first page
