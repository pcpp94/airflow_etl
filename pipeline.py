import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

#extract tasks
@task()
def retrieve_origin_tables():
    hook = MsSqlHook(mssql_conn_id="sqlserver")
    sql = """ select  t.name as table_name  
     from sys.tables t where t.name in ('prod','prodSubLine','prodLine') """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict
#
@task()
def import_origin_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_t = []
    start_time = time.time()
    #access the table_name element in dictionaries
    for k, v in tbl_dict['table_name'].items():
        #print(v)
        all_t.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MsSqlHook(mssql_conn_id="sqlserver")
        df = hook.get_pandas_df(sql)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
        df.to_sql(f'origin_{v}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print("Data imported Succesfully")
    return all_t

#convertation tasks
@task()
def convert_originServ():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM peru."origin_prod" ', engine)
    #drop columns
    df_2 = pdf[['prodK', 'prodAltK', 'prodsK','wMeCode', 'sizeMeCode', 'prodN', 'cost','finish_flag', 'dolor', 'safetyL', 'rPoint','listPr', 'size', 'sizeR', 'weight', 'days','prodL', 'price', 'class', 'style', 'model', 'description', 'sDate','eDate', 'status']]
    #replace nulls
    df_2['wMeCode'].fillna('0', inplace=True)
    df_2['prodsK'].fillna('0', inplace=True)
    df_2['sizeMeCode'].fillna('0', inplace=True)
    df_2['cost'].fillna('0', inplace=True)
    df_2['listPr'].fillna('0', inplace=True)
    df_2['prodL'].fillna('NA', inplace=True)
    df_2['class'].fillna('NA', inplace=True)
    df_2['style'].fillna('NA', inplace=True)
    df_2['size'].fillna('NA', inplace=True)
    df_2['model'].fillna('NA', inplace=True)
    df_2['description'].fillna('NA', inplace=True)
    df_2['price'].fillna('0', inplace=True)
    df_2['weight'].fillna('0', inplace=True)
    # Rename columns with rename function
    df_2 = df_2.rename(columns={"description": "Description", "prodN":"ServName"})
    df_2.to_sql(f'stg_prod', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported Succesfully"}

#
@task()
def convert_originServSubLine():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM peru."origin_prodSubLine" ', engine)
    #drop columns
    df_2 = pdf[['prodsK','subcatN', 'subcatK','subcatN', 'prodCatK']]
    # Rename columns with rename function
    df_2 = df_2.rename(columns={"subcatN": "ServSubLineName"})
    df_2.to_sql(f'stg_prodSubLine', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported Succesfully"}

@task()
def convert_originServLine():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM peru."origin_prodLine" ', engine)
    #drop columns
    df_2 = pdf[['prodCatK', 'prodCatAltK','englishProdN']]
    # Rename columns with rename function
    df_2 = df_2.rename(columns={"englishProdN": "prodCatName"})
    df_2.to_sql(f'stg_prodLine', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported Succesfully"}

#load
@task()
def prdServ_model():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pc = pd.read_sql_query('SELECT * FROM peru."stg_prodLine" ', engine)
    p = pd.read_sql_query('SELECT * FROM peru."stg_prod" ', engine)
    p['prodsK'] = p.prodsK.astype(float)
    p['prodsK'] = p.prodsK.astype(int)
    ps = pd.read_sql_query('SELECT * FROM peru."stg_prodSubLine" ', engine)
    #join all three
    df_3 = p.merge(ps, on='prodsK').merge(pc, on='prodCatK')
    df_3.to_sql(f'prd_prodLine', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported Succesfully"}


# [START how_to_task_group]
with DAG(dag_id="Serv_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["Serv_model"]) as dag:

    with TaskGroup("extract_dimProudcts_load", tooltip="Extract and load source data") as extract_load_origin:
        origin_Serv_tbls = retrieve_origin_tables()
        load_prods = import_origin_data(origin_Serv_tbls)
        #define order
        origin_Serv_tbls >> load_prods

    # [START howto_task_group_section_2]
    with TaskGroup("convert_origin_Serv", tooltip="convert and stage data") as convert_origin_Serv:
        convert_originServ = convert_originServ()
        convert_originServSubLine = convert_originServSubLine()
        convert_originServLine = convert_originServLine()
        #define task order
        [convert_originServ, convert_originServSubLine, convert_originServLine]

    with TaskGroup("load_Serv_model", tooltip="Final Serv model") as load_Serv_model:
        prd_Serv_model = prdServ_model()
        #define order
        prd_Serv_model

    extract_load_origin >> convert_origin_Serv >> load_Serv_model
