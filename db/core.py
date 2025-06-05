from mysql.connector import connect, Error
import time
import os
from dotenv import load_dotenv
from utils.logger import Logger

load_dotenv(override=True)


class Db():
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        self.connecting()
        self.table_data = 'results'
        self.table_genes = 'genes'
        

    def connecting(self, max_retries=10, delay=5) -> None:    
        for attempt in range(max_retries):
            try:
                self.connection = connect(
                    host=os.getenv("DB_HOST"),
                    port=os.getenv("DB_PORT"),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    database=os.getenv("DB_DATABASE")
                )
                self.cursor = self.connection.cursor()
                return 
            except Error as e:
                self.logger.error(f"Connection failed: {e}")
                time.sleep(delay)
        raise Exception("Could not connect to the database after multiple attempts")

    def __del__(self):
        self.close_connection()

    def insert(self, sql: str, params: tuple = None) -> None:
        if not params:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)
        self.connection.commit()

    def select(self, sql: str) -> list:
        self.cursor.execute(sql)
        rows = self.cursor.fetchall() 
        return rows
        
    def close_connection(self) -> None:
        if hasattr(self, 'connection'):
            self.connection.close()


class IsDbCreated():
    def check(self) -> None:
        for attempt in range(5):
            try:
                connection = connect(host=os.getenv("DB_HOST"), 
                                     port=os.getenv("DB_PORT"), 
                                     user=os.getenv("DB_USER"), 
                                     password=os.getenv("DB_PASSWORD"))
                cursor = connection.cursor()
                cursor.execute("SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
                cursor.execute(f'CREATE DATABASE IF NOT EXISTS {os.getenv("DB_DATABASE")}')
                connection.close()
                IsDbTable().check()
                return
            except Error as e:
                print(f"Connection failed: {e}")
                time.sleep(5)
        raise Exception("Could not connect to MySQL for database creation after multiple attempts.")


class IsDbTable(Db):
    def __init__(self):
        super().__init__()

    def check(self) -> None:
        if self.check_tables(self.table_data):
            self.create_datas()
        if self.check_tables(self.table_genes):
            self.create_genes()
       
    def create_datas(self) -> None:
        self.insert(f"""
            CREATE TABLE `{self.table_data}` (
                `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `product_page_url` VARCHAR(255) NOT NULL UNIQUE,
                `product_category` VARCHAR(255) DEFAULT NULL,
                `datas` JSON DEFAULT NULL,
                `result` JSON DEFAULT NULL,
                `cache_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `status` INT DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """)

    def create_genes(self) -> None:
        self.insert(f"""
            CREATE TABLE `{self.table_genes}` (
                `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `symbol` VARCHAR(255) NOT NULL UNIQUE,
                `status` INT DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """)
    
    def check_tables(self, table_name: str) -> bool:
        sql = f"SHOW TABLES FROM {os.getenv('DB_DATABASE')} LIKE '{table_name}'"
        rows = self.select(sql)
        if len(rows) == 0:
            return True
        return False