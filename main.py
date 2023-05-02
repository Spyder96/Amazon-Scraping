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
d= Date()


search = input("Enter what you want to search in Amazon : ")
#checking for sale
sale = input ("Is there any Sale currently ? \n Press Y for yes :  ")
sale = str(sale).lower() 
page = 1
if sale == 'y':
    salename = input ("Name of Sale : ")
    current_page = Htmlpage.AmazonPage(search, page, salename)
else:
    current_page = Htmlpage.AmazonPage(search, page)

current_page.get_product_links()

for link in tqdm(current_page.product_links):
    
    prd_page = Htmlpage.Product(link)
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
            'Page'               : current_page.page,
            'Date'               : d.get_date(),
            'Search'             : search,
            "Sale"               : current_page.sale
            }
    print(final_product_data)
    exit(1)
        
