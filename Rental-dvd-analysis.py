#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector
import sys
from mysql.connector import errorcode


# In[2]:


# create a function to make the connection with mysql database
def create_connection(host, user, password, database):

    try :
        print('connecting to mysql database')
        conn = mysql.connector.connect(host=host,user=user, password=password,database=database)

    except:
        if mysql.connector.Error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            return "Something is wrong with your user name or password\n"
        elif mysql.connector.Error.errno == errorcode.ER_BAD_DB_ERROR:
            return "Database does not exist"
        else:
            return 'Connection interrupted with this error: '+ mysql.connector.Error

        return 'closing the connection'
        sys.exit(1)

    finally:
        try:
            if (conn.is_connected()):
                print('connection was successful.')
                return conn
        except AttributeError:
            return 'error'


# In[3]:


# create a function to execute the query in myssql database and fetch the records
def fetch_records(con, query):

        try:      
           cursor = con.cursor()
           cursor.execute(query)           
           df = pd.DataFrame(cursor.fetchall())
           df.columns = cursor.column_names
           
           return df
        except mysql.connector.Error as err:
               print("Error occured while reading data from MySQL table", err)
        # finally:
        #     if (con.is_connected()):
        #         cursor.close()
        #         con.close()
        #         print("\nMySQL connection is closed...")


# In[4]:


## SQL queries 

### customer_id , full name , email ,membership_age ,  revenue , city ###

Cust_details= ''' select customer.customer_id , concat(first_name , ' ', last_name) name , 
          CASE
                  WHEN email NOT REGEXP '^[^@]+@[^@]+\.[^@]{2,}$' THEN 'NA'
                  ELSE email
          END as valid_email ,
              TIMESTAMPDIFF(MONTH, create_date, now()) membership_age ,
          revenue ,city.city from customer 
          left join ( select customer_id , sum(amount) revenue from payment
                group by customer_id) a
          on customer.customer_id=a.customer_id 
          inner join address on customer.address_id=address.address_id
          inner join  city on city.city_id= address.city_id
          order by customer.customer_id; '''

### --- top 5 preferred film language ####

Query_preferred_lang= ''' select  customer_id , group_concat(film_language) preferred_film_language 
                          from (SELECT  customer.customer_id , language.name film_language, count(*) ,
                                row_number() over(partition by customer.customer_id order by customer_id,count(*) desc) rwn   
                                 FROM customer left join rental ON rental.customer_id = customer.customer_id
                                 Inner join inventory ON rental.inventory_id = inventory.inventory_id
                                 Inner join film ON inventory.film_id = film.film_id
                                 Inner join language on film.language_id= language.language_id
                                 group by customer.customer_id,language.name
                                 order by customer.customer_id , language.name) a
                             where a.rwn <=5 
                             group by customer_id; '''

### -- query to find the top 5 category ####

Query_top5_category= ''' select  customer_id , group_concat(name) as Preferred_film_category  
                         from (  select customer.customer_id , category.name , count(*) ,
                                 row_number() over(partition by customer.customer_id order by customer_id,count(*) desc) rwn from 
                                 customer left join rental on customer.customer_id= rental.customer_id
                                 inner join inventory on inventory.inventory_id=rental.inventory_id
                                 Inner join film ON inventory.film_id = film.film_id
                                 Inner join film_category on film.film_id= film_category.film_id
                                 inner join category on category.category_id=film_category.category_id
                                 where category.category_id < 16
                                 group by customer.customer_id, category.name) as a
                         where a.rwn <=5 
                         group by customer_id; '''
   
   
### ----  query to find top 2 preferred film years ####

Query_top2_film_years= ''' select customer_id , group_concat(movie_type) preferred_film_year from 
                        (select customer_id , movie_type , cnt , 
                        row_number() over(partition by customer_id order by customer_id , cnt desc) rnk from 
                           (select customer_id, movie_type , count(*) as cnt  from
                            (select customer.customer_id ,film.film_id, film.title, film.release_year , 
                                 CASE
                                  WHEN film.release_year > 2010 THEN "New"
                                  WHEN film.release_year between 2001 and 2010 THEN "00s"
                                  WHEN film.release_year between 1991 and 2000 THEN "90s"
                                  ELSE "old"
                                END as movie_type
                                 from 
                                 customer left join rental on customer.customer_id= rental.customer_id
                                 inner join inventory on inventory.inventory_id=rental.inventory_id
                                 Inner join film ON inventory.film_id = film.film_id
                                 order by customer.customer_id)  a
                           group by a.customer_id , movie_type
                           order by customer_id) b ) c
                               where rnk <=2
                               group by customer_id; '''


# In[5]:


# connect to mysql sakila schema
conn= create_connection('localhost','root','HarryPatel','sakila')


# In[6]:


# create dataframe from the query result
df_Cust_details= fetch_records(conn,Cust_details)
df_preferred_lang= fetch_records(conn,Query_preferred_lang)
df_top5_category= fetch_records(conn,Query_top5_category)
df_top2_film_years= fetch_records(conn,Query_top2_film_years)


# In[7]:


df_Cust_details.head(2)


# In[8]:


df_preferred_lang.head(2)


# In[9]:


df_top5_category.head(2)


# In[10]:


df_top2_film_years.head(2)


# In[11]:


#check the shape of each dataframe
print(df_Cust_details.shape)
print(df_preferred_lang.shape)
print(df_top5_category.shape)
print(df_top2_film_years.shape)


# In[12]:


# merge all 4 dataframes into 1
df=df_Cust_details.merge(df_preferred_lang,on='customer_id').merge(df_top5_category,on='customer_id').merge(df_top2_film_years, on='customer_id')

print(df.shape)


# In[13]:


df.head(5)


# ## EDA (Exploratory Data Analysis)
# 
# #### Find the Top 5 revenue generator customers

# In[23]:


df_analysis= df.copy()
df_analysis.sort_values(by=['revenue'], inplace=True, ascending=False)


# In[26]:


top_5= df_analysis.iloc[:5,]


# In[27]:


top_5.head()


# In[28]:


# save original df dataframe to csv file 

df.to_csv('summit-media-rental-dev.csv')


# In[ ]:




