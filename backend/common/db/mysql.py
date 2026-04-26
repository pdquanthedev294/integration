import time
import mysql.connector

def get_mysql_conn():
  while True:
    try:
      return mysql.connector.connect(
        host="mysql",
        user="root",
        password="123456",
        database="noah_webstore_db",
        port=3306
      )
    
    except Exception:
      print("[WARN] MySQL not ready. Retry in 5s...")
      time.sleep(5)