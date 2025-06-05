from seleniumwire import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup, Tag
import os
import time
import re
import uuid
import random
import json
import shutil
from datetime import datetime
from db.core import Db
from utils.func import write_to_file


class GetPageContent():
    def __init__(self, proxies: dict):
        self.proxies = proxies

    def get(self, id: int, url: str, datas: dict):
        try:
            src = self.get_response(url)
            write_to_file(f'pages/{id}.html', src)
            if not src:
                print('NO CONTENT:',url)
                return
            result = self.get_page_data(src, json.loads(datas))
            self.update_data(id, result)
        except Exception as ex:
            if 'No - in TITLE' in str(ex):
                model = Db()
                sql = f"UPDATE {model.table_data} SET status=400 WHERE id=%s"
                model.insert(sql, (id,))
                model.close_connection()
            print(f'URL: {url}, ERROR: {ex}')

    def update_data(self, id: int, result: dict) -> None:
        model = Db()
        data = json.dumps(result)
        sql = f"UPDATE {model.table_data} SET result=%s, status=2, cache_time=%s WHERE id=%s"
        model.insert(sql, (data, datetime.utcnow().isoformat(), id))
        print('UPDATE', id)

    def get_page_data(self, src: str, datas: dict) -> dict:
        soup = BeautifulSoup(src, 'html.parser')
        strain_name = datas.get('strain_name','')
        result = {}
        result['product_number'] = datas.get('product_num','')
        result['product_title'] = self.get_product_title(strain_name, soup)
        result['product_name'] = datas.get('gene_symbol','')
        result['strain_name'] = strain_name
        result['strain_name_superscript'] = datas.get('sub_name','')
        result['strain_number'] = datas.get('product_id','')
        result['strain_description'] = self.get_value(soup, "Strain Description")
        result['ncbi_gene_id'] = datas.get('gene_id','')
        result['synonyms'] = '; '.join(datas.get('pro_gene_synonyms',[]))
        result['phenotype_tip'] = datas.get('mgi_url','').split('/')[-1]
        result['strain_background'] = datas.get('cell_line','')
        result['strain_modification'] = datas.get('pro_type','')
        result['environmental_standards'] = self.get_strtitle_value(soup, 'Environmental Standards')
        result['related_diseases'] = self.get_related_diseases(soup)
        result['source'] = self.get_strtitle_value(soup, 'Source')
        result['sperm_test'] = self.get_strtitle_value(soup, 'Sperm Test')
        result['product_status'] = self.get_strtitle_value(soup, 'Product Status') 
        result['available_region'] = self.get_strtitle_value(soup, 'Available Region')
        return result

    def get_strtitle_value(self, soup: BeautifulSoup, name: str) -> str:
        label = soup.find('strong',
                        string=lambda s: s and name in s)
        if not label:
            return ''                      
        td: Tag = label.find_parent('td')    
        if not td:
            return ''
        raw = td.get_text(' ', strip=True)
        pattern = rf'^{re.escape(name)}\s*[：:]?\s*'
        clean = re.sub(pattern, '', raw, flags=re.I)
        return ' '.join(clean.split())
        
    def get_related_diseases(self, soup: BeautifulSoup) -> list[str]:
        label = soup.find('strong',
                      string=lambda s: s and 'Related Diseases' in s)
        if not label:
            return []
        container = label.find_parent()   
        spans = container.find_all('span', recursive=False) 
        diseases = [' '.join(span.stripped_strings) for span in spans]
        return [d for d in diseases if d] 

    def get_product_title(self, strain_name: str, soup: BeautifulSoup) -> str:
        title = soup.find('title').get_text(strip=True)
        if '-' not in title:
            raise Exception('No - in TITLE:', title)
        block = title.partition(' ')[2].partition('-')[0].strip()
        if not block:
            raise Exception(f'Bad TITLE format: {title!r}')
        return f'{strain_name} {block}'.strip()
    
    def get_value(self, soup: BeautifulSoup, block_name: str) -> str:
        for marker in soup.find_all("td", string=lambda s: s and block_name in s):
            title_tr: Tag = marker.find_parent("tr")
            if not title_tr:
                continue
            next_tr = title_tr.find_next_sibling(
                lambda t: isinstance(t, Tag) and t.name == "tr"
            )
            if not next_tr:
                continue
            raw = next_tr.get_text(" ", strip=True)
            clean = " ".join(raw.split())
            return clean  
        return "" 

    def get_response(self, url: str, count_try: int =0) -> str:
        if count_try > 3:
            return None
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument("--disable-extensions")
            options.add_argument('--disable-application-cache')
            options.add_argument('--ignore-ssl-errors=yes')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--start-maximized')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--headless=new')
            self.profile_dir = os.path.join(os.getcwd(), "chrome_profiles", str(uuid.uuid4()))
            os.makedirs(self.profile_dir, exist_ok=True)
            options.add_argument(f"--user-data-dir={self.profile_dir}")
            proxy = random.choice(self.proxies)
            proxies = {
                'http': proxy,
                'https': proxy
            }
            seleniumwire_options = {  
                'proxy': proxies,
                'suppress_connection_errors': True 
            }
            self.driver = uc.Chrome(options=options, 
                                    seleniumwire_options = seleniumwire_options, 
                                    user_multi_procs=True, 
                                    version_main=int(os.getenv('DRIVER_VERSION')))
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    'source': '''
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Object;c
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_JSON;
                        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Proxy;
                '''
                })
            self.wait = lambda time_w, criteria: WebDriverWait(self.driver, time_w).until(
                EC.presence_of_element_located(criteria))
            self.driver.get(url)
            self.wait(30,(By.XPATH,'//*[@id="content_body"]/div/div/div[3]/div/div/div[1]/div/div[2]'))
            try:
                self.wait(30, By.XPATH,'//*[@id="content_body"]/div/div/div[3]/div/div/div[1]/div/div[2]/div/div[2]/table/tbody/tr[9]/td/div/span[1]')
            except:
                pass
            return self.driver.page_source
        except Exception as ex:
            # print(f'{url} ---- {ex}')
            pass
        finally:
            try:
                self.driver.close()
            except:
                pass
            try:
                self.driver.quit()  
            except:
                pass   
            if os.path.exists(self.profile_dir):
                try:
                    time.sleep(1)
                    shutil.rmtree(self.profile_dir)
                except Exception as ex:
                    print(ex)
        return self.get_response(url, count_try + 1)