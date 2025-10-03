from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt


PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def Fetch_Order_Data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    query = """
        select 
            o.orderdate::date as sale_date ,
            od.productid ,
            p.productname ,
            od.quantity ,
            p.price 
        from orders o 
        join order_details od on o.orderid = od.orderid 
        join products p on od.productid = p.productid 
    """
    df = pd.read_sql(query, conn)
    df.to_csv("/home/kiwilytics/Francis_out_airflow/fetch_sales_data.csv", index=False)

def proc_daily_revenue():
    df = pd.read_csv("/home/kiwilytics/Francis_out_airflow/fetch_sales_data.csv")
    df['total_revenue'] = df['price'] * df['quantity']
    revenue_per_day = df.groupby('sale_date')['total_revenue'].sum().reset_index()
    revenue_per_day.to_csv("/home/kiwilytics/Francis_out_airflow/daily_revenue.csv", index=False)

def Visualisation_total_revenue():
    df = pd.read_csv("/home/kiwilytics/Francis_out_airflow/daily_revenue.csv")
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o', linestyle='-')
    plt.title("Daily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    out_path = '/home/kiwilytics/Francis_out_airflow/Total_Revenue_Visualization.png'
    plt.savefig(out_path)
    plt.close()
    print(f"Revenue Visualization Saved: {out_path}")

def calculate_top_products():
    df = pd.read_csv("/home/kiwilytics/Francis_out_airflow/fetch_sales_data.csv")
    df['total_revenue'] = df['price'] * df['quantity']
    product_revenue = df.groupby('productname')['total_revenue'].sum().reset_index()
    top5 = product_revenue.sort_values('total_revenue', ascending=False).head(5)
    top5.to_csv("/home/kiwilytics/Francis_out_airflow/top5_products.csv", index=False)

def visualize_top_products():
    df = pd.read_csv("/home/kiwilytics/Francis_out_airflow/top5_products.csv")
    plt.figure(figsize=(12, 6))
    plt.bar(df['productname'], df['total_revenue'], color='skyblue')
    plt.title('Top 5 Products by Total Revenue')
    plt.xlabel('Product')
    plt.yl
