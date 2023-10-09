from datetime import datetime, timedelta, date
import pandas as pd
import pandahouse as ph
import numpy as np
import io
from io import StringIO
import requests
import telegram
import matplotlib.pyplot as plt
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator

def report_to_bot_petrova(chat=None):
    my_token = '6068430500:AAHsAZdIN1PVKWiXkbhEivIYGvcjvZlqXfo' # тут нужно заменить на токен вашего бота
    bot = telegram.Bot(token=my_token) # получаем доступ
    
    chat_id = chat or -928988566
    msg = 'Отчет за последние 7 дней'
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    connection = {'host': 'http://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230820',
                          'user':'student', 
                          'password':'dpo_python_2020'
                         }
     
    query = '''
       SELECT toDate(time) as date, count(distinct user_id) as dau,
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            likes/views as ctr
        FROM simulator_20230820.feed_actions
        WHERE toDate(time) between today() - 7 and today()
        GROUP BY  toDate(time)
            '''
    df = ph.read_clickhouse(query, connection=connection)
    df
    
    sns.lineplot(x=df['date'], y=df['dau'])
    plt.title('dau trend for last 7 days')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'dau_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    sns.lineplot(x=df['date'], y=df['likes'])
    plt.title('likes trend for last 7 days')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'likes_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    sns.lineplot(x=df['date'], y=df['views'])
    plt.title('views trend for last 7 days')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'views_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    sns.lineplot(x=df['date'], y=df['ctr'])
    plt.title('ctr trend for last 7 days') 
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'ctr_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    data = df
    file_object = io.StringIO()
    data.to_csv(file_object)
    file_object.name = '7days_report_file.csv'
    file_object.seek(0)
    bot.sendDocument(chat_id=chat_id, document=file_object) 
    
# Дефолтные параметры, которые прокидываются в таски
default_args = {
'owner': 'eka_petrova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 9, 15),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_petrova_7days_report():
    
    @task
    def make_report():
        report_to_bot_petrova()
    make_report()
   
dag_petrova_7days_report = dag_petrova_7days_report()