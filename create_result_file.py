from utils.func import *
import pandas as pd
import pytz 
import json
from datetime import datetime
from db.core import Db
import csv


def create_tsv():
    model = Db()
    sql = f"SELECT product_category, result, product_page_url, cache_time FROM {model.table_data} WHERE status=2"
    result = model.select(sql, with_column_names=True)
    df = pd.DataFrame(result)
    # Парсим поле result (JSON) в отдельные колонки
    parsed_results = df['result'].apply(lambda r: json.loads(r) if isinstance(r, str) else {})
    parsed_df = pd.json_normalize(parsed_results)
    df = pd.concat([df.drop(columns=['result']), parsed_df], axis=1)
    df.insert(0, 'vendor_name', 'Cyagen Biosciences')

    if 'cache_time' in df.columns:
        eastern = pytz.timezone('US/Eastern')
        df['cache_time'] = (
            pd.to_datetime(df['cache_time'], errors='coerce', utc=True)
              .dt.tz_convert(eastern)   
              .dt.strftime('%-m/%-d/%Y %-I:%M:%S %p')
        )

    final_columns = [
        'vendor_name',
        'product_category',
        'product_number',
        'product_title',
        'product_name',
        'strain_name',
        'strain_name_superscript',
        'strain_number',
        'strain_description',
        'ncbi_gene_id',
        'synonyms',
        'phenotype_tip',
        'strain_background',
        'strain_modification',
        'environmental_standards',
        'related_diseases',
        'source',
        'sperm_test',
        'product_status',
        'available_region',
        'product_page_url',
        'cache_time'
    ]
    df = df[final_columns]

    now = datetime.now().strftime("%Y%m%d")
    df.to_csv(f"Cyagen_Biosciences_Animal_Model_Results_{now}.tsv", sep='\t', index=False, quoting=csv.QUOTE_NONE)


if __name__ == "__main__":
    create_tsv()
