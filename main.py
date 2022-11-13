from datetime import date
from datetime import timedelta
import mysql.connector
import requests
from bs4 import BeautifulSoup

mydb = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="db"
)
mycursor = mydb.cursor(buffered=True)


def create_table():
    s = "CREATE TABLE IF NOT EXISTS ETSY(id INT AUTO_INCREMENT PRIMARY KEY,Shop_Name TEXT ,Sales_Data TEXT)"
    mycursor.execute(s)
    mydb.commit()

    sql = "ALTER table etsy add COLUMN shop_name TEXT,add COLUMN sales_data TEXT"
    mycursor.execute(sql)
    mydb.commit()


def etsy():
    sql = "ALTER table etsy DROP COLUMN shop_name,drop column sales_data"
    mycursor.execute(sql)
    mydb.commit()

    create_table()

    sql = "INSERT INTO ETSY (Shop_Name,Sales_Data)  VALUES( %s, %s) "
    for x in range(1, 1250):
        r = requests.get(f"https://www.etsy.com/in-en/search/shops?order=most_relevant&page={x}")
        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        shop_names = soup.find_all(class_="wt-text-title-01 wt-text-truncate")
        for idx, i in enumerate(shop_names):
            shop_name = i.text
            try:
                r1 = requests.get(f"https://www.etsy.com/in-en/shop/{shop_name}")
                data1 = r1.text
                soup1 = BeautifulSoup(data1, 'html.parser')
                sales_num = soup1.find(class_="wt-text-caption wt-no-wrap")
                sales = sales_num.text.split()[0].replace(",", "")
            except:
                p_data = (shop_name, "No sales data")
            else:
                p_data = (shop_name,
                          sales)
                mycursor.execute(sql, p_data)
                mydb.commit()
                print(mycursor.rowcount, "lines were inserted.")
    sql = "DELETE FROM etsy where shop_name IS NULL"
    mycursor.execute(sql)
    mydb.commit()


def track():
    try:
        global date, d
        date_last = date.today() - timedelta(days=7)
        d = f"{date_last.day}_{date_last.month}_{date_last.year}"
        sql_date = f"alter table ETSY drop column {d} "
        mycursor.execute(sql_date)
        mydb.commit()
        print("Column Date Column Deleted")
    except:
        pass
    try:
        date = date.today()
        d = f"{date.day}_{date.month}_{date.year}"
        sql = f"alter table ETSY add column {d} TEXT"
        mycursor.execute(sql)
        mydb.commit()
        print("New Date Column Added")
    except:
        pass

    sql = f"select shop_name from etsy "
    mycursor.execute(sql)
    l1 = []
    a = mycursor.fetchall()
    for i in a:
        l1.append(i[0])
    for shop_name in l1:
        try:
            r1 = requests.get(f"https://www.etsy.com/in-en/shop/{shop_name}")
            data1 = r1.text
            soup1 = BeautifulSoup(data1, 'html.parser')
            sales_num = soup1.find(class_="wt-text-caption wt-no-wrap")
            sales = sales_num.text.split()[0].replace(",", "")
        except:
            pass
        else:
            sql = f"select sales_data from etsy where shop_name = '{shop_name}'"
            mycursor.execute(sql)
            a = mycursor.fetchall()
            for i in a:
                try:
                    sales1 = int(i[0].replace(",", ""))
                    print(sales1)
                    sql1 = f"update etsy set {d} = {sales} - {sales1} where shop_name = '{shop_name}';"
                except:
                    pass
                else:
                    mycursor.execute(sql1)
                    mydb.commit()
                    print(mycursor.rowcount, "datelines were inserted.")


create_table()
etsy()
track()
