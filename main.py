from datetime import date
from datetime import datetime
from datetime import timedelta

import mysql.connector
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

mydb = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="db"
)
mycursor = mydb.cursor(buffered=True)


def etsy():
    s = "CREATE TABLE IF NOT EXISTS ETSY(Shop_Name TEXT,Sales_Data TEXT)"
    mycursor.execute(s)
    mydb.commit()

    sql = "DELETE FROM ETSY"
    mycursor.execute(sql)
    mydb.commit()

    sql = "INSERT INTO ETSY(Shop_Name,Sales_Data) VALUES( %s, %s)"
    for x in range(1, 1250):
        r = requests.get(f"https://www.etsy.com/in-en/search/shops?order=most_relevant&page={x}")
        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        shop_names = soup.find_all(class_="wt-text-title-01 wt-text-truncate")
        for i in shop_names:
            shop_name = i.text
            try:
                r1 = requests.get(f"https://www.etsy.com/in-en/shop/{shop_name}")
                data1 = r1.text
                soup1 = BeautifulSoup(data1, 'html.parser')
                sales_num = soup1.find(class_="wt-text-caption wt-no-wrap")
                sales = sales_num.text.split()[0]
            except:
                p_data = (shop_name, "No sales data")
            else:
                p_data = (shop_name,
                          sales)
            mycursor.execute(sql, p_data)
            mydb.commit()
            print(mycursor.rowcount, "lines were inserted.")


def track():
    try:
        global date, d
        date = date.today()
        d = f"{date.day}_{date.month}_{date.year}"
        sql = f"alter table ETSY add column {d} TEXT"
        mycursor.execute(sql)
        mydb.commit()
        print("a")
    except:
        print("A")
        for x in range(1, 1250):
            r = requests.get(f"https://www.etsy.com/in-en/search/shops?order=most_relevant&page={x}")
            data = r.text
            soup = BeautifulSoup(data, 'html.parser')
            shop_names = soup.find_all(class_="wt-text-title-01 wt-text-truncate")
            for i in shop_names:
                shop_name = i.text
                try:
                    r1 = requests.get(f"https://www.etsy.com/in-en/shop/{shop_name}")
                    data1 = r1.text
                    soup1 = BeautifulSoup(data1, 'html.parser')
                    sales_num = soup1.find(class_="wt-text-caption wt-no-wrap")
                    sales = sales_num.text.split()[0]
                except:
                    pass
                else:
                    sql1 = f"insert into etsy (SELECT (COUNT(sales_data) - {sales}))"
                    mycursor.execute(sql1)
                    mydb.commit()
    else:
        pass


track()
# # taking input as the current date
# # today() method is supported by date
# # class in datetime module
# Begindatestring = date.today()
#
# # print begin date
# print("Beginning date")
# print(Begindatestring)
#
# # calculating end date by adding 4 days
# Enddate = Begindatestring - timedelta(days=4)
#
# # printing end date
# print("Ending date")
# print(Enddate)
