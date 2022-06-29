#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#1) Поменять имена dag на уникальные.
#Поставить новую дату начала DAG и новый интервал (все еще должен быть ежедневным)

#2) Удалить таски get_stat и get_stat_com. Вместо них сделать свои собственные, которыесчитают следующие:
#Найти топ-10 доменных зон по численности доменов
#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
#На каком месте находится домен airflow.com?

#3) Финальный таск должен писать в лог результат ответы на вопросы выше


# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
b
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[3]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def get_top_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df.domain.map(len)
    top_data_df['start_zone'] = top_data_df.domain.str.find('.')
    my_set = {}
    for row in top_data_df.itertuples():
        domain_zone = str(row.domain[-row.len_domain + 1 + row.start_zone:])
        my_set[domain_zone] = my_set.get(domain_zone, 0) + 1
    my_list = sorted(my_set.items(), key=lambda x: x[1], reverse=True)[0:10]
    a, b, c = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), np.array([]), np.array([])
    for i in range(10):
        b, c = np.append(b, my_list[i][0]), np.append(c, my_list[i][1])
    top_data_top_domain_10 = pd.DataFrame()
    top_data_top_domain_10['rank'] = pd.Series(a)
    top_data_top_domain_10['domain_zone'] = pd.Series(b)
    top_data_top_domain_10['count'] = pd.Series(c)
    top_data_top_domain_10['count'] = top_data_top_domain_10['count'].astype(int)
    with open('top_data_top_domain_10.csv', 'w') as f:
        f.write(top_data_top_domain_10.to_csv(index=False, header=False))


# In[5]:


def get_longest_name():
    top_data_max_len = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_max_len['len_domain'] = top_data_max_len.domain.map(len)
    len_max = top_data_max_len['len_domain'].max()
    top_data_max_len = top_data_max_len[top_data_max_len['len_domain'] == len_max].sort_values(by='domain').head(1)
    del top_data_max_len['len_domain']
    with open('top_data_max_len.csv', 'w') as f:
        f.write(top_data_max_len.to_csv(index=False, header=False))


# In[6]:


def get_domain_airflow():
    top_data_aifrlow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_aifrlow = top_data_aifrlow[top_data_aifrlow.domain == 'airflow.com']
    if top_data_aifrlow.shape[0] == 0:
        top_data_aifrlow = pd.DataFrame()
        top_data_aifrlow['info'] = pd.Series(['No airflow.com'])
        with open('top_data_aifrlow.csv', 'w') as f:
            f.write(top_data_aifrlow.to_csv(index=False, header=False))
    else:
        with open('top_data_aifrlow.csv', 'w') as f:
            f.write(top_data_aifrlow.to_csv(index=False, header=False))


# In[7]:


def print_data(ds):
    with open('top_data_top_domain_10.csv', 'r') as f:
        all_data_domain = f.read()
    with open('top_data_max_len.csv', 'r') as f:
        all_data_len = f.read()
    with open('top_data_aifrlow.csv', 'r') as f:
        all_data_airlow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(all_data_domain)

    print(f'Max length domain for date {date}')
    print(all_data_len)

    print(f'Airlow.com for date {date}')
    print(all_data_airlow)


# In[8]:


default_args = {
    'owner': 'n-chujkin-dag',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 29),
    'schedule_interval': '* 20 * * *'
}


# In[9]:


dag = DAG('n-chujkin-dag', default_args=default_args)


# In[10]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)


# In[11]:


t2_domains = PythonOperator(task_id='get_top_domains',
                            python_callable=get_top_domains,
                            dag=dag)


# In[12]:


t2_len = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)


# In[13]:


t2_airlow = PythonOperator(task_id='get_domain_airflow',
                           python_callable=get_domain_airflow,
                           dag=dag)


# In[14]:


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[15]:


t1 >> [t2_domains, t2_len, t2_airlow] >> t3


# In[ ]:




