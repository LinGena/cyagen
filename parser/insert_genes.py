import mysql.connector
import csv
from tqdm import tqdm  
from db.core import Db

def insert_genes():
    FILE_PATH = "Inputs_Gene_IDs.tsv" 

    model = Db()
    total_lines = sum(1 for _ in open(FILE_PATH, "r"))
    print(f"Всего строк в файле: {total_lines}")

    with open(FILE_PATH, "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter="\t") 

        for row in tqdm(reader, total=total_lines, desc="Загрузка в MySQL"):
            if not row:  
                continue
            symbol = row[0]  
            print(symbol)
            sql = f"INSERT IGNORE INTO {model.table_genes} (symbol, status) VALUES (%s, 1)" 
            model.insert(sql, (symbol,))
    model.connection.close()

    print("✅ Все данные загружены в MySQL!")