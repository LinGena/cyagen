import requests
import random
import json
from math import ceil
from db.core import Db
from bs4 import BeautifulSoup


class SearchLinks():
    def __init__(self, proxies):
        self.proxies = proxies
        self.product_category = 'Animal Models'

    def get(self, gen: str):
        try:
            self.model = Db()
            pros_count = None
            page = 1
            total_pages = 2
            self.csrftoken = None
            self.current_proxy = None
            count_try = 0
            while page <= total_pages:
                datas = self.get_response(gen, page)
                if datas and datas.get('products'):
                    self.insert_datas(datas.get('products'))
                    if not pros_count:
                        pros_count = datas.get('pros_count')
                        if not pros_count:
                            break
                        total_pages = ceil(pros_count/10)
                    page += 1
                else:
                    print('No datas or products: ',gen)
                    if page > 1:
                        self.csrftoken = None
                        self.current_proxy = None
                        count_try += 1
                        if count_try > 1:
                            page += 1
                            count_try = 0
                    else:
                        total_pages = 0
            self.update_gene_status(gen)
        except Exception as ex:
            print(ex)
        finally:
            self.model.close_connection()

    def update_gene_status(self, gen: str):
        sql = f"UPDATE {self.model.table_genes} SET status=2 WHERE symbol=%s"
        self.model.insert(sql, (gen,))
        
    def insert_datas(self, datas: dict) -> None:
        for data in datas:
            try:
                product_page_url = 'https://www.cyagen.com/us/en/catalog-model-bank-us/' + data['product_num']
                sql = f"INSERT INTO {self.model.table_data} (product_page_url, product_category, datas) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE product_page_url = product_page_url"
                self.model.insert(sql,(product_page_url, self.product_category, json.dumps(data)))
                print('insert', product_page_url)
            except Exception as ex:
                print('ERROR insert_datas =',ex)

    def get_response(self, gen: str, page: int = 1, count_try: int = 0) -> dict:
        if count_try>3:
            return None
        try:
            session = requests.session()
            if not self.current_proxy:
                proxy = random.choice(self.proxies)
                self.current_proxy = {
                    'http': proxy,
                    'https': proxy
                }
            session.proxies.update(self.current_proxy)
            if not self.csrftoken:
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'ru-RU,ru;q=0.9',
                    'dnt': '1',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                }
                html = session.get('https://www.cyagen.com/us/en/catalog-model-bank.html', headers=headers)
                soup = BeautifulSoup(html.text, "html.parser")          
                input_block = soup.find('input',{"name":"csrfmiddlewaretoken"})
                self.csrftoken = input_block.get('value')
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uk;q=0.6',
                'content-type': 'application/json;charset=UTF-8',
                'dnt': '1',
                'origin': 'https://www.cyagen.com',
                'priority': 'u=1, i',
                'referer': 'https://www.cyagen.com/us/en/catalog-model-bank.html',
                'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                'x-csrftoken': self.csrftoken,
                'x-requested-with': 'XMLHttpRequest',
            }
            json_data = {
                'kw': gen,
                'ti': '10090',
                'limit': 10,
                'page': page,
                'web_source': 'cn',
            }
            response = session.post('https://www.cyagen.com/api/bio/sperm-bank/cn-search/', 
                                     headers=headers, 
                                     json=json_data,
                                     timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            # print(ex)
            pass
        finally:
            session.close()
        return self.get_response(gen, page, count_try+1)