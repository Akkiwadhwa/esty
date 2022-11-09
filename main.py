import concurrent.futures
from bs4 import BeautifulSoup
import requests
import pandas as pd
from tqdm import tqdm


def etsy(x):
    global s, dict1, shop_names, shop_name, soup1
    print(x)
    try:
        r = requests.get(f"https://www.etsy.com/search/shops?ref=empty_redirect&order=alphabetical&page={x}")
    except:
        pass
    else:
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
                s = sales_num.text.split()[0]
            except:
                dict1 = {
                    "Shop name": shop_name,
                    "Sales": "No sales data"
                }
                l1.append(dict1)
            else:
                dict1 = {
                    "Shop name": shop_name,
                    "Sales": s
                }
                l1.append(dict1)
    return l1


l1 = []
total_pages = 1250
with concurrent.futures.ThreadPoolExecutor(32) as executor:
    results = list(tqdm(executor.map(etsy, range(1, total_pages + 1)), total=total_pages))
    for i in results:
        df = pd.DataFrame(i)
        df.to_csv("data.csv")
