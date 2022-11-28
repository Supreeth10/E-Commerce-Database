#!/usr/bin/env python
# coding: utf-8

# Insatlling libraries
# For Mac you may use the following to install psycopg2-binary:
# pip3 install psycopg2-binary
# And in your code use:
# import psycopg2
# To use pandas install it like:
# pip3 install pandas

import psycopg2
import pandas as pd

def initialize():
    connection = psycopg2.connect(
        user = "supreeth_mudduchetty", #username that you use
        password = "postgres", #password that you use, you don't need to include your password when submiting your code
        host = "localhost", 
        port = "5432", 
        database = "supreeth_mudduchetty"
    )
    connection.autocommit = True
    return connection
    
# If you need to add new tables to your database you can use the following function to create the target table 
# assuming that conn is a valid, open connection to a Postgres database
# def createTable(conn):
#     with conn.cursor() as cursor:
#         cursor.execute(f"""
#             DROP TABLE IF EXISTS editions;
#             CREATE TABLE editions (
#                 bookid         NUMERIC,
#                 date           DATE,
#                 edition        TEXT,
#                 lang           TEXT,
#                 pubid          INTEGER
#             );
#             ALTER TABLE editions ADD PRIMARY KEY (bookid, date);
#         """)
#     print(f"Created editions table")
   
# If you need created and added a new table to your database you can use the following function to insert data into your target table
# def insertTable(conn):
#     #You can import your data from a CSV file by pg_insert_v1
#     pg_insert_v1 = '''COPY editions(bookid,date,edition,
#             lang,pubid)
#             FROM 'editions.csv'
#             DELIMITER ','
#             CSV HEADER;'''
    #Or You can get the get the column name of a table inside the database and enter some values by using the following commented lines
    # pg_insert_v2 = """ INSERT INTO editions
    #             VALUES (%s,%s,%s,%s,%s)"""
    # inserted_values = (1, '05/16/2022', '4th edition', 'English', '101')


##Execute the the insert SQL string
    # with conn.cursor() as cursor:
    #     cursor.execute(pg_insert_v1)
    #     count = cursor.rowcount
    #     print (count, "Successfully inserted")

def runQuery_test(conn):
    select_Query = "select * from customers"
    editions_df = pd.DataFrame(columns = ['Customer ID', 'name', 'state', 'city'])
    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'Customer ID': row[0], 'name': row[1], 'state': row[2], 'city': row[3]}
            # print("Book Id = ", row[0], )
            # print("Date = ", row[1])
            # print("Edition  = ", row[2])
            # print("Language = ", row[3])
            editions_df = editions_df.append(output_df, ignore_index=True)
    
        print(editions_df)

def runQuery_1(conn):
    select_Query = """select * from (
    select  od.order_id , c.cust_id,sum(od.quantity) No_of_Items
    from order_details od join orders o 
    on  o.order_id = od.order_id 
    join customers c on 
    c.cust_id=o.cust_id
    group by od.order_id,c.cust_id
) as tempTbl
where tempTbl.No_of_Items = (
    select max(temp2.No_of_Items) from (
        select sum(od.quantity) as No_of_Items,od.order_id
    from order_details od join orders o 
    on  o.order_id = od.order_id 
    group by od.order_id
    ) as temp2
);"""
    Query_df = pd.DataFrame(columns = ['Order ID','Customer_ID','Order_item_count'])
    with conn.cursor() as cursor:
            cursor.execute(select_Query)
            records = cursor.fetchall()
            for row in records:
                output_df ={'Order ID': row[0], 'Customer_ID': row[1], 'Order_item_count': row[2]}
                Query_df = Query_df.append(output_df, ignore_index=True)
            print(Query_df)

def runQuery_2(conn):
    select_Query = """select distinct od.order_id 
    from order_details od 
    where od.order_id not in (
    select distinct od.order_id 
    from order_details od join products p 
    on od.prod_id=p.prod_id
    where p.availability = 'NO'
    );"""
    Query_df = pd.DataFrame(columns = ['Order ID'])
    with conn.cursor() as cursor:
            cursor.execute(select_Query)
            records = cursor.fetchall()
            for row in records:
                output_df ={'Order ID': row[0]}
                Query_df = Query_df.append(output_df, ignore_index=True)
            print(Query_df)

def main():
    conn = initialize()
    # createTable(conn)
    # insertTable(conn)
    # runQuery_test(conn)
    # runQuery_1(conn)
    runQuery_2(conn)


if __name__ == "__main__":
    main()
