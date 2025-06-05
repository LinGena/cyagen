
from db.core import IsDbCreated, IsDbTable
from parser.insert_genes import insert_genes
from proxy.proxy_manager import get_proxies
from parser.search import SearchLinks
from parser.get_page import GetPageContent
from db.core import Db
from concurrent.futures import ThreadPoolExecutor
import string
import shutil
import os
from dotenv import load_dotenv
import undetected_chromedriver as uc


load_dotenv(override=True)


def check_db():
    IsDbCreated().check()
    IsDbTable().check()


def fetch_and_parse(gen: str, proxies: dict):
    print('Scrape gene:',gen)
    SearchLinks(proxies).get(gen)

def get_search():
    model = Db()
    sql = f"SELECT symbol FROM {model.table_genes} WHERE status=1"
    rows = model.select(sql)
    model.close_connection()
    # rows = [(str(i),) for i in range(10)]
    # rows = [(letter,) for letter in string.ascii_letters]
    proxies = get_proxies()
    with ThreadPoolExecutor(max_workers=os.getenv('THREADS_COUNT')) as executor:
        futures = [
            executor.submit(fetch_and_parse, row[0], proxies)
            for row in rows if row
        ]
        for future in futures:
            future.result()

def page_fetch_and_parse(id: int, url: str, datas: dict, proxies: dict):
    print('Scrape url:',url)
    GetPageContent(proxies).get(id, url, datas)

def get_content():
    os.makedirs('pages', exist_ok=True)
    if os.path.exists('chrome_profiles'):
        shutil.rmtree('chrome_profiles')
    model = Db()
    sql = f"SELECT id, product_page_url, datas FROM {model.table_data} WHERE status=1"
    rows = model.select(sql)
    model.close_connection()
    proxies = get_proxies()
    try:
        driver = uc.Chrome(version_main=int(os.getenv('DRIVER_VERSION')))
        driver.quit()
    except:
        pass
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(page_fetch_and_parse, row[0], row[1], row[2], proxies)
            for row in rows if row
        ]
        for future in futures:
            future.result()

if __name__ == '__main__':
    # check_db()
    # insert_genes()
    # get_search()
    get_content()

