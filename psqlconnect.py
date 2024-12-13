import os
from dotenv import load_dotenv
import subprocess
import time
import psycopg2
from psycopg2 import OperationalError
from sqlqueries import sql_query_creating_tables, query_1, query_2, query_3, query_4, query_5, query_6, query_7

# Load environment variables from the .env file
load_dotenv()

# expanded project directory path
file_path = os.path.expanduser('~/dwproject/')

# Read connection information from environment variables
hostname = 'localhost'
username = os.getenv('POSTGRES_USER')
pw = os.getenv('POSTGRES_PASSWORD')
port_used = '5433'
db = os.getenv('POSTGRES_DB')
os.environ['PGPASSWORD'] = pw

# Base command for using psql cli
psql_command = ['psql', '-h', hostname, '-U', username, '-d', db,'-p', port_used, '-c']

def psql_conn():
    
    # Timeout counter
    timeout = 0
    
    while True:
        
        timeout = timeout + 1
        
        try:
            # Establish the connection to the PostgreSQL server
            conn = psycopg2.connect(
                host=hostname,
                user=username,
                password=pw,
                port=port_used,
                dbname=db
            )
            print("Connected to PostgreSQL database successfully.")
            # Return the connection object
            return conn
        
        except:
            print('Connecting to the PSQL instance via psycopg2')
            time.sleep(3)
        
        finally:
            #exit the loop when timeout counter reached 200, approximately 10 mins
            if timeout == 200:
                print('Connection to the PSQL instance timed out')
                return None
    
def psql_close(conn):
    
    # Close the psql connection
    try:
        conn.close()
        print('Connection to the PostgreSQL database closed')
    
    except OperationalError as e:
        print(f"Error: {e}")
        return None
    
def create_tables(conn):
    
    try:
        # Create a cursor
        cur = conn.cursor()
        
        # SQL command for setting up the dimension tables and fact table for the data warehouse
        sql_command = sql_query_creating_tables()
        
        cur.execute(sql_command)
        conn.commit()
        print('Dimension tables and fact table created successfully')
    
    except OperationalError as e:
        print(f"Error: {e}")
        return None
    
    finally:
    # Ensure the cursor is closed
        if cur:
            print('Closing the cursor')
            cur.close()
            print('Cursor closed')
            
def insert_datedimtable():
    
    psql_command.append(f"\\copy \"dateDimTable\"(saledate, saledate_year, saledate_month, saledate_monthname, saledate_day, saledate_weekday, saledate_weekdayname, quarter, quartername, date_id) FROM '{file_path}dateDimTable.csv' DELIMITER ',' CSV HEADER;")
    
    try:
        subprocess.run(psql_command, check=True)
        print('Successfully inserted data to dateDimTable in the PSQL instance')
    except subprocess.CalledProcessError as e:
        print(f"Error inserting data to dateDimTable: {e}")
        
    psql_command.pop()
    
def insert_statedimtable():
    
    psql_command.append(f"\\copy \"stateDimTable\"(state,state_id) FROM '{file_path}stateDimTable.csv' DELIMITER ',' CSV HEADER;")
   
    try:
        subprocess.run(psql_command, check=True)
        print('Successfully inserted data to stateDimTable in the PSQL instance')
    except subprocess.CalledProcessError as e:
        print(f"Error inserting data to stateDimTable: {e}")
    
    psql_command.pop()

def insert_sellerdimtable():
    
    psql_command.append(f"\\copy \"sellerDimTable\"(seller,seller_id) FROM '{file_path}sellerDimTable.csv' DELIMITER ',' CSV HEADER;")

    try:
        subprocess.run(psql_command, check=True)
        print('Successfully inserted data to sellerDimTable in the PSQL instance')
    except subprocess.CalledProcessError as e:
        print(f"Error inserting data to sellerDimTable: {e}")
        
    psql_command.pop()
        
def insert_vehicledimtable():
    
    psql_command.append(f"\\copy \"vehicleDimTable\"(year,make,model,trim,body,transmission,color,interior,vehicle_id) FROM '{file_path}vehicleDimTable.csv' DELIMITER ',' CSV HEADER;")
    
    try:
        subprocess.run(psql_command, check=True)
        print('Successfully inserted data to vehicleDimTable in the PSQL instance')
    except subprocess.CalledProcessError as e:
        print(f"Error inserting data to vehicleDimTable: {e}")
        
    psql_command.pop()
        
def insert_salesfacttable():
    
    psql_command.append(f"\\copy \"salesFactTable\"(vin,condition,odometer,mmr,sellingprice,date_id,seller_id,state_id,vehicle_id,sale_id) FROM '{file_path}salesFactTable.csv' DELIMITER ',' CSV HEADER;")
    
    try:
        subprocess.run(psql_command, check=True)
        print('Successfully inserted data to salesFactTable in the PSQL instance')
    except subprocess.CalledProcessError as e:
        print(f"Error inserting data to salesFactTable: {e}")
    
    psql_command.pop()

def sql_verify_queries():
    
    psql_command.append(query_1())
    
    try:
        print('First 5 rows of dateDimTable')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_2())
    
    try:
        print('First 5 rows of sellerDimTable')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_3())
    
    try:
        print('First 5 rows of stateDimTable')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_4())
    
    try:
        print('First 5 rows of vehicleDimTable')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_5())
    
    try:
        print('First 5 rows of SalesFactTable')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_6())
    
    try:
        print('Top 10 car make sold')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()
    
    psql_command.append(query_7())
    
    try:
        print('Total car sales for each quarter of each year')
        subprocess.run(psql_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error querying database table: {e}")  
    
    psql_command.pop()

        