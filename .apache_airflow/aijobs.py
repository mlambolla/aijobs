import datetime
import logging
import pandas as pd
import time

# scraping
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

#airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import variable

#variables
AIJOBS_URL = variable.Variable.get("aijobs_url")
OUTPUT_FILE_PATH = variable.Variable.get("output_file_path")
BUCKET = variable.Variable.get('s3_bucket')
BULK_TABLE = variable.Variable.get('bulk_table')
TODAY = '{0:%Y%m%d}'.format(datetime.datetime.now())
FILE = f'ai_jobs_{TODAY}.csv'

COPY_SQL = """
COPY {}
FROM 's3://{}/{}'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER '$'
"""

INSERT_NEW_JOBS_SQL = """
    insert into public.data_jobs
    select bdj.keyword, bdj.company, bdj.title, bdj."location", bdj.work_type, bdj.link, bdj.skills , date_trunc('day',CURRENT_DATE )::date as creation_date  , null as deletion_date
    from public.bulk_data_jobs bdj 
        left join public.data_jobs dj 
            on bdj.link = dj.link 
    where dj.link is null
"""

DELETE_JOBS_SQL = """
    update public.data_jobs
    set delete_date = date_trunc('day',CURRENT_DATE )::date
    where link not in (select  link from public.bulk_data_jobs)
    and delete_date is null
"""

def scrape_aijobs():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = True
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

    url = AIJOBS_URL
    driver.get(url)

    job_list = []

    #create dict with job title + link
    links_dict = {}
    a_lis = driver.find_elements_by_xpath('//*[@id="content"]/div/div[4]')[0].find_elements_by_tag_name('a')
    for a in a_lis:
        links_dict[a.text.replace('Open ','').replace(' Jobs','')] = a.get_attribute("href")  
        
    all_job_links = []
    for key in links_dict:
        driver.get(links_dict[key])
        time.sleep(1)
        #iterate in each page, all jobs descriptions
        elements = driver.find_elements_by_tag_name('a')
        for elem in elements:
            if ('list-group-item' in elem.get_attribute("class")):
                link = elem.get_attribute("href")
                
                if (link not in all_job_links):
                    h5_tags = elem.find_elements_by_tag_name('h5')
                    for htag in h5_tags:
                        if ('mb-1' in htag.get_attribute("class")):
                            title = htag.text
                            
                    p_tags = elem.find_elements_by_tag_name('p')
                    for p in p_tags:
                        if ('job-list-item-company' in p.get_attribute("class")):
                            company = p.text
                        if ('text-primary mb-1' in p.get_attribute("class")):
                            title = p.text
                            
                    s_tags = elem.find_elements_by_tag_name('span')
                    skills = []
                    for s in s_tags:
                        if ('badge badge-light badge-pill' in s.get_attribute("class")):
                            skills.append(s.text)
                        if ('job-list-item-location' in s.get_attribute("class")):
                            location = s.text
                        if ('badge-secondary badge-pill' in s.get_attribute("class")):
                            work_type = s.text
                    row = [key, company, title, location, work_type, link, skills]
                    job_list.append(row)
                    all_job_links.append(link)
    df = pd.DataFrame(data=job_list,columns=['keyword', 'company', 'title', 'location', 'work_type', 'link', 'skills'])
    
    df.to_csv( OUTPUT_FILE_PATH + '/' + FILE,index=False,sep='$')  

def upload_file_to_S3():
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    logging.info(OUTPUT_FILE_PATH + '/' + FILE)
    s3_hook.load_file(OUTPUT_FILE_PATH + '/' + FILE,FILE,BUCKET)

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials",client_type='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(COPY_SQL.format(BULK_TABLE,BUCKET,FILE,credentials.access_key, credentials.secret_key))

dag = DAG(
    'aijobs',
    start_date=datetime.datetime.now(),
    schedule_interval= '@once'  
)

scrape = PythonOperator(
    task_id="scrape",
    python_callable=scrape_aijobs,
    dag=dag
)

file_to_s3 = PythonOperator(
    task_id= "file_to_s3",
    python_callable=upload_file_to_S3,
    dag = dag
)

truncate_table= PostgresOperator(
    task_id="truncate_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=f"TRUNCATE TABLE {BULK_TABLE}"
)

bulk_file = PythonOperator(
    task_id='bulk_file',
    dag= dag,
    python_callable=load_data_to_redshift
)

insert_new_jobs = PostgresOperator(
    task_id="insert_new_jobs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=INSERT_NEW_JOBS_SQL
)

delete_jobs = PostgresOperator(
    task_id="delete_jobs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=DELETE_JOBS_SQL
)

scrape >> file_to_s3 >> truncate_table >> bulk_file >> insert_new_jobs >> delete_jobs