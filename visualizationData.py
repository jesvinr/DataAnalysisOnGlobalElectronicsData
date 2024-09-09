import pandas as pd
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

def close_connection(conn):
    #this function will close the connection
    try:
        conn.close()
        print("connection closed successfully")
    except Error as e:
        print('Error occurred when closing the connection: ',e)  

# creating connection
conn=connection()


# getting data for different data analysis
popularityQueryHigh10 ="(SELECT product_name, COUNT(product_name) AS popularity FROM sales_table\
                    GROUP BY product_name ORDER BY popularity DESC LIMIT 10)"
popularityQueryLow10 ="(SELECT product_name, COUNT(product_name) AS popularity FROM sales_table\
                    GROUP BY product_name ORDER BY popularity ASC LIMIT 10)"


# product_category sales
salesPerformanceOnProductCategory = "SELECT product_category, COUNT(product_category) as sales\
    FROM sales_table\
    GROUP BY product_category ORDER BY COUNT(product_category)  DESC limit 10"

# product_subcategory sales
salesPerformanceOnProductSubCategory = "SELECT product_subcategory, COUNT(product_subcategory) as sales\
    FROM sales_table GROUP BY product_subcategory ORDER BY COUNT(product_subcategory) DESC"

# gender count
salesByGender ="select customer_gender,count(customer_gender) as gender_count from sales_table group by customer_gender;"

# top product in each country
topProductInEachCountry = """
select 
    st.store_country,
    product_name,
    sum(s.sales_quantity) as total_quantity_sold
from sales_table s
join store_table st on s.sales_storekey = st.storekey
group by st.store_country, product_name
having sum(s.sales_quantity) = (
    select max(total_quantity_sold)
    from (
        select 
            st.store_country,
            product_name,
            sum(s.sales_quantity) as total_quantity_sold
        from sales_table s
        join store_table st on s.sales_storekey = st.storekey
        group by st.store_country, product_name
    ) as Subquery
    where Subquery.store_country = st.store_country)
order by total_quantity_sold desc;
"""

# revenue country country
revenueByCountry = "SELECT customer_country, SUM(product_selling_price) as sales,\
     SUM(product_selling_price - product_cost_price) AS profit from sales_table\
    group by customer_country order by sales desc"

# number of customer by country
customerByCountry = "SELECT customer_country, COUNT(customer_country) as number_of_customer from sales_table\
    group by customer_country"

# store preformance based on sales
storePerformanceOnSales = "SELECT st.storekey, SUM(st.product_selling_price) as sales_in_store FROM \
    (SELECT store.storekey, store.store_open_date, sale.product_selling_price \
    FROM store_table as store JOIN sales_table AS sale ON store.storekey=sale.sales_storekey) as st\
    GROUP BY st.storekey;"


# grouping store by country
storeByCountry ="""select store_country, COUNT(store_country) 
                    as store_range from store_table group by store_country order by store_country asc"""

storeByYear = "select year(store_open_date) as year_opened, count(storekey) as total_stores_opened\
                from store_table group by year(store_open_date) order by year_opened;"

salesByCurrency = "SELECT sales_currencycode as currency, SUM(product_selling_price) as total_sale\
                    from sales_table GROUP BY currency"

top10ProductSaleByVolume ="select product_name, sum(sales_quantity) as total_sales_volume from sales_table\
                            group by product_name order by total_sales_volume desc limit 10;"
least10ProductSaleByVolume = "select product_name, sum(sales_quantity) as total_sales_volume from sales_table\
                            group by product_name order by total_sales_volume asc limit 10;"


# getting data
storePerformanceOnSalesRes = pd.read_sql(storePerformanceOnSales, conn)
storeByCountryRes = pd.read_sql(storeByCountry, conn)
storeByYearRes = pd.read_sql(storeByYear, conn)
salesByCurrencyRes = pd.read_sql(salesByCurrency, conn)
top10ProductSaleByVolumeRes = pd.read_sql(top10ProductSaleByVolume, conn)
least10ProductSaleByVolumeRes = pd.read_sql(least10ProductSaleByVolume, conn)
salesOnCategoryRes = pd.read_sql(salesPerformanceOnProductCategory, conn)
salesOnSubCategoryRes = pd.read_sql(salesPerformanceOnProductSubCategory, conn)
salesByGenderRes = pd.read_sql(salesByGender, conn)
topProductInEachCountryRes = pd.read_sql(topProductInEachCountry, conn)
popularityQueryHigh10Res = pd.read_sql(popularityQueryHigh10, conn)
top10LeastPopularItem = pd.read_sql(popularityQueryLow10,conn)
revenueByCountryRes = pd.read_sql(revenueByCountry, conn)
customerByCountryRes = pd.read_sql(customerByCountry, conn)


top10ProductSaleByVolumeRes.to_csv("powerBI_csv/top10ProductSaleByVolume.csv",index=False,mode='w')
salesByCurrencyRes.to_csv("powerBI_csv/salesByCurrency.csv", index=False,mode='w')
storeByYearRes.to_csv("powerBI_csv/storeIncreaseByYear.csv", index=False,mode='w')
storePerformanceOnSalesRes.to_csv("powerBI_csv/storeSalesPerformance.csv", index=False,mode='w')
storeByCountryRes.to_csv("powerBI_csv/numberOfStoreByCountry.csv", index=False,mode='w')
least10ProductSaleByVolumeRes.to_csv("powerBI_csv/least10ProductSaleByVolume.csv", index=False,mode='w')
topProductInEachCountryRes.to_csv("powerBI_csv/topProductInEachCountry.csv")
salesByGenderRes.to_csv("powerBI_csv/salesNumberByGender.csv")
salesOnSubCategoryRes.to_csv("powerBI_csv/salesOnSubCategory.csv", index=False,mode='w')
salesOnSubCategoryRes.to_csv("powerBI_csv/top10SalesOnSubCategory.csv", index=False,mode='w')
salesOnCategoryRes.to_csv("powerBI_csv/top10SalesOnCategory.csv", index=False,mode='w')
popularityQueryHigh10Res.to_csv("powerBI_csv/top10MostPopularItem.csv", index=False,mode='w')
top10LeastPopularItem.to_csv("powerBI_csv/top10leastPopularItem.csv", index=False,mode='w')
revenueByCountryRes.to_csv("powerBI_csv/revenueByCountry.csv", index=False,mode='w')
customerByCountryRes.to_csv("powerBI_csv/customerByCountry.csv", index=False,mode='w')


