
import requests
import logging
from  bs4 import BeautifulSoup
from tqdm import tqdm
import re
import json

class HtmlPage:

    default_Headers = ({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0' , 'Accept-language':'en-US , en;q=0.5'})

    def __init__(self, link, headers = None ) :
        self.link = link
        self.headers = headers or self.default_Headers
        try:
            webpage = requests.get( self.link, headers = self.headers )
            logging.debug(f"Requests Success")
        except Exception as e:
            logging.error(f"Link Unavailable : {e} :: {self.link}") 
        
        self.soup = BeautifulSoup( webpage.content, "html.parser")
    
    def soup_table_data(self,table):
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
 

class AmazonPage( HtmlPage ):
    
    def __init__(self, link ):
        self.link = link
        super().__init__(self.link)
        self.product_links = [] 

# To extract the Product links from the Page
    
    def get_product_links(self):
        self.links = self.soup.find_all("a",attrs={'class' : 'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})
        
        for link in tqdm( self.links, desc="Processing items"):
            sublink = link.get('href')
            product_link = "https://www.amazon.in" + sublink
            self.product_links.append(product_link)
    
    def get_next_page(self):
        try :
            next_page = self.soup.find("a",attrs={ 'class': "s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"})
            next_page_link = "https://www.amazon.in"+ next_page.get('href')
            self.page = self.page + 1
            URL = next_page_link
            print(f"Page : {self.page} ")
            return URL
        except Exception as e:
            logging.error(f"No More Pages Available : {e}")            
            return False
        
        

class Product(HtmlPage):

    def __init__(self, link):
        super().__init__(link)
    

    def scrape_product_details(self):
        prod_soup = self.soup 

        #product name
        try:
            product_name = prod_soup.find("span", attrs={'id' : "productTitle"}).text.strip()
            logging.debug(f"Product Name= {product_name}")
        except Exception as e:
            logging.error(f" Product Name Unavailable : {e}")
            product_name = None
        self.product_name = product_name

        #price
        try:    
            price_span=prod_soup.find("span",attrs={'class': "a-price aok-align-center reinventPricePriceToPayMargin priceToPay"})
            price_class=price_span.find("span",attrs={'class': "a-price-whole"}).text
            #price
            price =  float(''.join(re.findall(r'\d+',price_class)))
            logging.debug(f"Product Price= {price}")
        except Exception as e:
            logging.error(f"Product price Unavailable : {e}")
            price = None
        self.price = price

        #description
        try :
            descriptions_list= prod_soup.find("ul",attrs={'class': "a-unordered-list a-vertical a-spacing-mini"}).text.strip().split("    ")
            #description='\n'.join(descriptions_list)
            description= json.dumps(descriptions_list)
            logging.debug(f"Product description= {description}")
            
        except Exception as e:
            logging.error(f"Product description Unavailable : {e}")
            description = None
        self.description = description

        #rating's received
        try:
            rating= prod_soup.find("a",attrs={ 'id':"acrCustomerReviewLink" ,'class':"a-link-normal"})
            #text format eg: 1,765 ratings 
            rating= rating.text.strip()
            number_of_ratings=  int(''.join(re.findall(r'\d+', rating)))
            logging.debug(f"Product number_of_ratings= {number_of_ratings}")
        except Exception as e:
            logging.error(f"Product ratings Unavailable : {e}")
            number_of_ratings = None
        self.rating = number_of_ratings
            
        # stars
        try:
            stars= prod_soup.find("a",attrs={'class':"a-popover-trigger a-declarative"}).text
            number_of_stars= float(stars.strip().split()[0])
            logging.debug(f"Product number_of_stars= {number_of_stars}")
        except Exception as e:
            logging.error(f"Product Stars Unavailable : {e}")
            number_of_stars = None
        self.stars = number_of_stars

        #num of answered questions
        try: 
            answered_questions= (prod_soup.find("a",attrs={ 'class':"a-link-normal askATFLink"}).text.strip()).split()
            num_answered_questions = int(''.join(re.findall(r'\d+', answered_questions[0])))
            logging.debug(f"Product num_answered_questions= {num_answered_questions}")
        except Exception as e:
            logging.error(f"Product Answered question Unavailable : {e}")
            num_answered_questions = None
        self.answered_questions = num_answered_questions

        #Amazon highlights
        try:
            features = prod_soup.find_all("a",attrs={ 'class': "a-size-small a-link-normal a-text-normal"})
            #feature_list = ", ".join([feature.text.strip() for feature in features])
            feature_list = json.dumps([feature.text.strip() for feature in features])
            logging.debug(f"Product feature_list= {feature_list}")
        except Exception as e:
            logging.error(f"Product highlights Unavailable : {e}")
            feature_list = None
        self.feature_list = feature_list
            
        #Scraping technical data table
        try:
            tech_table = prod_soup.find('table', attrs={'class':'a-keyvalue prodDetTable'})
            technical_details= self.soup_table_data(tech_table)
            logging.debug(f"Product technical_details= {technical_details}")
        except Exception as e:
            logging.error(f"Product techincal data Unavailable : {e}")
            technical_details = None
        self.technical_details = technical_details
        
        #Scraping Addtional data table
        try :
            add_div = prod_soup.find( 'div', attrs={ 'id':"productDetails_db_sections", 'class':"a-section"})
            add_table = add_div.find( 'table', attrs={ 'id':"productDetails_detailBullets_sections1", \
                                                        'class':"a-keyvalue prodDetTable"})
            additional_details= self.soup_table_data(add_table)
            logging.debug(f"Product additional_details= {additional_details}")
        except Exception as e:
            logging.error(f"Product Addtional data Unavailable : {e}")
            additional_details = None
        self.additional_details = additional_details
            


# final_product_data = {
#             #'ASIN'               : additional_details['ASIN'],
#             'Name'               : product_name,
#             'Price'              : price, 
#             'Stars'              : number_of_stars, 
#             'Ratings'            : number_of_ratings,
#             'Answered_Questions' : num_answered_questions,
#             'Amazon_Services'    : feature_list,
#             'Description'        : description,
#             'Technical_details'  : technical_details,
#             'Additional_details' : additional_details,
#             'Link'               : self.link,
#             'Page'               : self.page,
#             'Date'               : current_datetime,
#             'Search'             : search,
#             "Sale"               : sale
#             }
        




#iterating the reference links for the listed products