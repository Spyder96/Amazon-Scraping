from tqdm import tqdm
import logging
import datetime
import os
import Htmlpage
import Database

class Date:
    def __init__(self):
        self.date = None
    
    def get_date(self):
        date_time = datetime.datetime.now()
        self.date = date_time.strftime("%Y-%m-%d")
        return self.date


class SearchParameters():
    baselink = "https://www.amazon.in/s?k="

    def __init__(self):
        self.next_page_available = True

    def set_search_attributes(self):
        self.search = input("Enter what you want to search in Amazon : ")
        #checking for sale
        sale = input ("Is there any Sale currently ? \n Press Y for yes :  ")
        sale = str(sale).lower()
        if sale == 'y':
            self.salename = input ("Name of Sale : ")
        self.page = 1
        self.link = self.baselink + self.search
        

def main():
    # Creating Date object
    day = Date()

    #Creating Search object
    amazon = SearchParameters()
    amazon.set_search_attributes()

    #search = amazon_search.search 
   
    while amazon.next_page_available:
        

        # if amazon_search.salename != None :
            
        # else:
        #     current_page = Htmlpage.AmazonPage(search, amazon_search.page)
        # Creating Amazon page object
        try: 
            current_page = Htmlpage.AmazonPage(amazon.link)
            logging.debug("Amazon page created successfully")
        except Exception as e :
            logging.error(f"Error occured while creating Amazon Page : {e}")

        current_page.get_product_links()

        for link in tqdm(current_page.product_links):
            try:
                prd_page = Htmlpage.Product(link)
                logging.debug("Amazon page created successfully")
            except Exception as e :
                logging.error(f"Error occured while creating Amazon Page : {e}")
            
            prd_page.scrape_product_details()

            final_product_data = {
                
                    'Name'               : prd_page.product_name,
                    'Price'              : prd_page.price, 
                    'Stars'              : prd_page.stars, 
                    'Ratings'            : prd_page.rating,
                    'Answered_Questions' : prd_page.answered_questions,
                    'Amazon_Services'    : prd_page.feature_list,
                    'Description'        : prd_page.description,
                    'Technical_details'  : prd_page.technical_details,
                    'Additional_details' : prd_page.additional_details,
                    'Link'               : prd_page.link,
                    'Page'               : amazon.page,
                    'Date'               : day.get_date(),
                    'Search'             : amazon.search,
                    "Sale"               : amazon.sale
                    }
            print(final_product_data)
            exit(1)
                
