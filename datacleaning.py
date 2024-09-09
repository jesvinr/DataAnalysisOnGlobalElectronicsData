import pandas as pd
from datetime import datetime
import mysql.connector as sconn
from mysql.connector import Error

# connection
def configuration():
    # configure the sql connector 
    try:
        config = {
            "user":"root",
            "password":"root",
            "host":"localhost",
            "database":"global_electronics"
        }
        print("Configuration completed")
        return config
    
    except Error as e:
        print("Error occurred during configuration:",e)

def connection():
    # configure the connection between python and mysql
    try:
        config=configuration()
        if config is None:
            return None
        conn=sconn.connect(**config)

        if conn.is_connected():
            print("connection completed")
        return conn
    
    except Error as e:
        print("Error occurred when open connection:",e)

def drop_table(conn,query_drop):
    # this function will drop the table
    c=conn.cursor()
    try:
        c.execute(query_drop)
        print("Table dropped successfully")
    
    except Error as e:
        print("Error occurred when dropping data",e)

def create_table(conn,query_create):
    #this function will create the table
    c=conn.cursor()
    try:
        c.execute(query_create)
        print("Table Created")
        pass

    except Error as e:
        print("Error occurred when creating table",e)

def insert_data_to_table(conn,value, query):
    cursor = conn.cursor()
    
    for index, row in value.iterrows():
        values = tuple(row)
        
        try:
            # Execute the query
            cursor.execute(query, values)
            conn.commit()
            print(f"Row {index + 1} inserted successfully")
        except Error as err:
            print(f"Error: {err}")
            conn.rollback()

def close_connection(conn):
    #this function will close the connection
    try:
        conn.close()
        print("connection closed successfully")
    except Error as e:
        print('Error occurred when closing the connection: ',e)  

def calculate_age(born):
    today = datetime.now()
    age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    return int(age)  # Explicitly return as integer

# customer data
customer = pd.read_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\Datasets\\Customers.csv", encoding='latin1')
customer["City"] = customer["City"].str.lower() 
customer["Birthday"] = pd.to_datetime(customer["Birthday"], format="%m/%d/%Y")
customer['age'] = customer['Birthday'].apply(calculate_age)
customer['age'] = customer['age'].astype(int)


#exchange rate column
ex_rate = pd.read_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\Datasets\\Exchange_Rates.csv", encoding='latin1')
ex_rate["Date"] = pd.to_datetime(ex_rate["Date"], format="%m/%d/%Y")


#product
product = pd.read_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\Datasets\\Products.csv", encoding='latin1')

# Clean the price columns by removing dollar signs and converting to float
product['Unit Cost USD'] = product['Unit Cost USD'].replace({r'[^\d.]': ''}, regex=True).astype('float')
product['Unit Price USD'] = product['Unit Price USD'].replace({r'[^\d.]': ''}, regex=True).astype('float')


# sales
sales = pd.read_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\Datasets\\Sales.csv", encoding='latin1')
sales["Order Date"] = pd.to_datetime(sales["Order Date"], format="%m/%d/%Y")
sales["Delivery Date"] = pd.to_datetime(sales["Delivery Date"], format="%m/%d/%Y")

# Calculate the difference between known delivery dates and order dates
sales['date_diff'] = (sales["Delivery Date"] - sales["Order Date"]).dt.days

# Calculate the average difference (timedelta)
average_diff = int(round(sales['date_diff'].mean()))

# Fill missing delivery dates with orderdate + average_diff
sales["Delivery Date"] = sales["Delivery Date"].fillna(sales["Order Date"] + pd.to_timedelta(average_diff, unit='D'))

# Drop the temporary 'date_diff' column
sales.drop(columns=['date_diff'], inplace=True)


# stores
stores = pd.read_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\Datasets\\Stores.csv", encoding='latin1')
stores["Open Date"] = pd.to_datetime(stores["Open Date"], format="%m/%d/%Y")


# merging data
# Merging customer, product, sales data and store data

merge_data =  pd.merge(product, sales, on='ProductKey')
merge_data = pd.merge(merge_data, customer, on='CustomerKey')



# --------------------------------------- Database part ------------------------------------------ #

conn=connection()

# table names:
# product_sales_table
# exchange_rate_table
# store_table
# drop_table(conn,"drop table if exists sales_table")
# drop_table(conn,"drop table if exists exchange_rate_table")
# drop_table(conn,"drop table if exists store_table")



# creating table

# Ordernumber is unique after combining 3 tables [customer, product, sales]
createProductSalesTable = """
create table if not exists sales_table (
product_key int,
product_name varchar(100),
product_brand varchar(100),
product_color varchar(20),
product_cost_price decimal(10, 2),
product_selling_price decimal(10, 2),
product_subcategorykey int,
product_subcategory varchar(100),
product_categoryKey int,
product_category varchar(100),
sales_ordernumber int,
sales_lineitem int,
sales_orderdate date,
sales_deliverydate date,
sales_customer_key int,
sales_storekey int,
sales_quantity int,
sales_currencycode char(3),
customer_gender varchar(10),
customer_name varchar(50),
customer_city varchar(100),
customer_state_code varchar(100),
customer_state varchar(100),
customer_zipcode varchar(10),
customer_country varchar(50),
customer_continent varchar(50),
customer_birthday date,
primary key(sales_ordernumber, sales_lineitem)
)
"""

createExchangeRateTable = """
create table if not exists exchange_rate_table (
id int primary key auto_increment,
exchange_date date,
exchange_currency char(3),
exchange_rate float
)
"""

createStoreTable = """
create table if not exists store_table (
storekey int primary key,
store_country varchar(100),
store_state varchar(100),
store_area int,
store_open_date date
)
"""

# creating table
# create_table(conn, createProductSalesTable)
# create_table(conn, createExchangeRateTable)
# create_table(conn, createStoreTable)


# inserting query data into table

insertSalesQuery = """
        INSERT INTO sales_table (
            product_key, product_name, product_brand, product_color, 
            product_cost_price, product_selling_price, product_subcategorykey, product_subcategory, 
            product_categoryKey, product_category, sales_ordernumber, sales_lineitem, 
            sales_orderdate, sales_deliverydate, sales_customer_key, sales_storekey, sales_quantity, 
            sales_currencycode, customer_gender, customer_name, 
            customer_city, customer_state_code, customer_state, customer_zipcode, 
            customer_country, customer_continent, customer_birthday
        )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

insertExchangeQuery = """
        INSERT INTO exchange_rate_table (
            exchange_date, exchange_currency, exchange_rate
        ) VALUES (%s, %s, %s);
"""

insertStoreQuery = """
        INSERT INTO store_table(
            storekey, store_country, store_state, store_area, store_open_date
        ) VALUES (%s, %s, %s, %s, %s);
"""

# insert_data_to_table(conn,merge_data, insertSalesQuery)
# insert_data_to_table(conn,ex_rate, insertExchangeQuery)
# insert_data_to_table(conn,stores, insertStoreQuery)



# ---------------------------------------- cleaned dataset ------------------------------------------------ #
merge_data.to_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\resultentDataset\\product_sales.csv", index=True, mode = 'w')
ex_rate.to_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\resultentDataset\\exchange_rate.csv",
               index=True, mode = 'w' )
stores.to_csv("D:\\python\\VsCodePythonWorkplace\\project_global_electronics\\resultentDataset\\stores_data.csv",
               index=True, mode = 'w')