import pandas as pd
from pandas.io import sql
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup
import mysql.connector
#Scraping Data From Books Website
name1 = []
price1 = []
def Scrape(url):
    max_page = 50
    current_page = 1
    while current_page<=max_page:
        new_url = f'{url}/catalogue/page-{current_page}.html'
        response = requests.get(new_url)
        soup = BeautifulSoup(response.content,"html.parser")
        content = soup.find_all(class_='col-sm-8 col-md-9')
        name = soup.find_all("h3")
        price = soup.find_all(class_="price_color")
        for i,j in zip(name,price):
            name = i.text
            price = j.text
            name1.append(name)
            price1.append(price)
        current_page=current_page+1
def connect(hostname,username,database):
    try:
        cnx = mysql.connector.connect(
            host = hostname,
            user = username,
            db = database
            )
        return cnx
    except mysql.connector.Error as err:
        print("An Error Occured while connecting to Database: ",err)
        return None
def create_Table():
    global cnx
    global cursor
    cnx = connect("localhost","root","books_pipeline")
    if cnx is not None:
        cursor = cnx.cursor()
        cursor.execute("CREATE Table if not exists Books(id INT AUTO_INCREMENT PRIMARY KEY, Book_Name VARCHAR(300), Price VARCHAR (30),created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
def load_Data():
    if cnx is not None:
       for item1, item2 in zip(name1, price1):
           query = "INSERT INTO Books(Book_Name,Price) VALUES (%s,%s)"
           values = (item1,item2)
           cursor.execute(query,values)
    cnx.commit()
                
if __name__ == '__main__':   
    url = "http://books.toscrape.com"   
    Scrape(url)
    create_Table()
    df = pd.DataFrame(list(zip(name1, price1)),columns =['Name', 'Price'])
    load_Data()
    print(df) 
    df.to_csv('Books.csv', index=False)
