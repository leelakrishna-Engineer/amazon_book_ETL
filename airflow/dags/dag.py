#dag - directed acyclic graph
# tasks: 1) fetch amazon data 2) clean data (transform) 3) create and store data in table on postgres (load)
# operators: Python Operator and PostgresOperator
#hooks - allows connection to postgress
# dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
#1) fetching amazon data (extract) 2) clean data (transform)
headers={
    "referer":"https://www.amazon.com/",
    "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/",
    "Sec-Ch-Ua":"Not_A Brand",
    "Sec-Ch-Ua-Mobile":"?0",
    "Sec-Ch-Ua-Platform":"Windows",
}
def get_amazon_data_books(num_books,ti):
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"
    books=[]
    seen_titles=set()
    page=1
    while len(books)<num_books:
        url=f"{base_url}&page={page}"
        #SEND A REQUEST TO THE URL AND GET THE RESPONSE
        response=requests.get(url,headers=headers) 
        if response.status_code ==200:
            #Parse the context of the request with BeautifulSoup
            soup=BeautifulSoup(response.content,"html.parser")
            # find book containers(you many need to adjust the class names based on the actual HTML structure)
            book_containers=soup.find_all("div",{"class":"s-result-item"})
            #Loop throug the book cotainers and extract data
            for book in book_containers:
                title=book.find("span",{"class":"a-text-normal"})
                author = book.find("a",{"class": "a-size_base"})
                price = book.find("span",{"class": "a-price-whole"})
                rating = book.find("span",{"class": "a-icon-alt"})
                if title and author and price and rating:
                    book_title=title.text.strip()
                    # check if title has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "title": book_title,
                            "author": author.text.strip(),
                            "price": price.text.strip(),
                            "rating": rating.text.strip()
                        })
                # Increament the page number for the next iteration
            page +=1
        else:
            print("Failed to retrive the page")
            break

        #limit to the requested number of books
        books=books[:num_books]
        #convert the list of dictionaries into a dataframe
        df=pd.DataFrame(books)
        #remove duplicates based on 'Title' column
        df.drop_duplicates(subset="Title", inplace=True)
        #push the DataFrame to XCom
        ti.xcom_push(key='book_data',value=df.to_dict("records"))

#3) create and store data in table on postgres (load)
def insert_book_data_into_postgres(ti):
    #pull the data from XCom
    book_data=ti.xcom_pull(key='book_data',task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")
    Postgres_hook=PostgresHook(postgres_conn_id='books_connection')
    insert_query="""
    INSERT INTO books (title, author, price, rating)
    VALUES (%s, %s, %s, %s)"""
    for book in book_data:
        Postgres_hook.run(insert_query,parameters=(book['title'],book['author'],book['price'],book['rating']))
       
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag=DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple dag to fetch book data from amazon and store data in Postgres',
    schedule=timedelta(days=1),
)

fetch_book_data_task=PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],
    dag=dag,
)

create_table_task=PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)
insert_book_data_task=PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

# DEPENDENCIES
fetch_book_data_task >> create_table_task >> insert_book_data_task