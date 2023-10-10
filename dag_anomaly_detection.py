# импорт библиотек
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

# функция для расчета аномального значения, за которое примем значение, выходящее за значение 25 и 75 перцентиля
def anomaly_detection(metric_1, metric_2, name, chat=None):
    my_token = '********'
    bot = telegram.Bot(token=my_token)
    if metric_1.mean() <=  metric_2.quantile(0.25) or metric_1.mean() >  metric_2.quantile(0.75):
        chat_id = chat or -968261538
        msg = f'Внимание! Среднее значение метрики  {name} за последние 15 минут - {round(metric_1.mean(),2)}, что отличается на {round(((metric_1.mean() - metric_2.mean())/metric_2.mean())*100, 2)} % от метрики предыдущего периода. Ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/4275/'
        bot.sendMessage(chat_id=chat_id, text=msg)

# функция для расчета аномального значения для количества пользователей
def anomaly_detection_2(metric_1, metric_2, chat=None):
    my_token = '********' 
    bot = telegram.Bot(token=my_token)
    if metric_1 > (metric_2 * 1.5):
        chat_id = chat or -968261538
        msg = f'Количество пользователей за последние 15 минут {metric_1}, что больше на {round(metric_1 * 100/metric_2, 2)} %, чем за предыдущие 15 минут. Ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/4275/'
        bot.sendMessage(chat_id=chat_id, text=msg)        
    elif metric_1 < (metric_2 * 0.5):
        chat_id = chat or -968261538
        msg = f'Количество пользователей за последние 15 минут {metric_1}, что меньше на {round(metric_1 * 100/metric_2, 2)} %, чем за предыдущие 15 минут. Ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/4275/'
        bot.sendMessage(chat_id=chat_id, text=msg)     

# функция для загрузки данных         
def dag_alert_to_bot_petrova(chat=None):
    my_token = '*********' 
    bot = telegram.Bot(token=my_token) 
    chat_id = chat or -968261538
          
    connection = {'host': 'http://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230820',
                          'user':'student', 
                          'password':'dpo_python_2020'
                         }
     # загрузка датасета feed_action с учетом агрегации по 15 минут
    query = """
            select *
            from
            (select *,
            DENSE_RANK() over(order by period desc) as groups
            from 
            (select toStartOfFifteenMinutes(time) as period, 
                user_id,
                countIf(post_id, action='like') as likes, 
                countIf(post_id, action='view') as views, 
                round(likes/views, 3) as ctr
            from simulator_20230820.feed_actions
            group by toStartOfFifteenMinutes(time), user_id
            ) t1
            ) t2
            where groups = 1 or groups = 2

        """
# загрузка датасета messages с учетом агрегации по 15 минут
    query2 = '''

            select *,
            DENSE_RANK() over(order by period desc) as groups
                    from 
            (select toStartOfFifteenMinutes(time) as period, 
                    count(distinct user_id) messenger_users
            from simulator_20230820.feed_actions
            where time >= now() - 30*60
            group by toStartOfFifteenMinutes(time)) t1
            join 
            (select toStartOfFifteenMinutes(time) as period, count(distinct user_id) feed_users, 
                        count(user_id) as messages,  
                        round(messages/feed_users, 2) as messages_per_user
                            from simulator_20230820.message_actions
                        where time >= now() - 30*60
                        group by toStartOfFifteenMinutes(time)
                        order by period desc) t3
            using period
    '''
    
    df_metr = ph.read_clickhouse(query, connection=connection)
    df_sum = ph.read_clickhouse(query2, connection=connection)
    df_1 = df_metr[df_metr['groups'] == 1] # Разделяем датасет по группам в зависимости от времени
    df_2 = df_metr[df_metr['groups'] == 2] # Разделяем датасет по группам в зависимости от времени
    
    anomaly_detection(metric_1=df_1.views, metric_2=df_2.views, name='view') # применение функции нахождения аномалий
    anomaly_detection(metric_1=df_1['likes'], metric_2=df_2['likes'], name='like') # применение функции нахождения аномалий
    anomaly_detection(metric_1=df_1['ctr'], metric_2=df_2['ctr'], name='ctr') # применение функции нахождения аномалий
    
   #  расчет количества пользователей, использующих только мессенджер и ленту новостей
    f_u1 = df_sum.feed_users[0]
    f_u2 = df_sum.feed_users[0]
    f_m1 = df_sum.messenger_users[0]
    f_m2 = df_sum.messenger_users[0]
     
    anomaly_detection_2(metric_1=f_u1, metric_2=f_u2) 
    anomaly_detection_2(metric_1=f_m1, metric_2=f_m2)
    
# Дефолтные параметры, которые прокидываются в таски
default_args = {
'owner': 'eka_petrova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 9, 15),
}

# Интервал запуска DAG каждые 15 минут
schedule_interval = '0/15 * * * *'

# запускаем даг
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alerts_every_fiftheen_min():
    
    @task
    def make_report():
        dag_alert_to_bot_petrova()
    make_report()

dag_alerts_every_fiftheen_min = dag_alerts_every_fiftheen_min()
