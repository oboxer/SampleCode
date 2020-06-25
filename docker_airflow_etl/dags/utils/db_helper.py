import logging

import psycopg2
from airflow.models import Variable

S3_SETTINGS = Variable.get('prod_s3_sec', deserialize_json=True)
REDSHIFT_CONN = Variable.get('prod_redshift', deserialize_json=True)
CON = psycopg2.connect(dbname=REDSHIFT_CONN['dbname'],
                       host=REDSHIFT_CONN['host'],
                       port=REDSHIFT_CONN['port'],
                       user=REDSHIFT_CONN['user'],
                       password=REDSHIFT_CONN['password'],
                       )
CON.autocommit = True

INIT_TABLES = {
    'companies': '''
    create table companies
            (
              company_id   bigint        not null unique primary key,
              company_name varchar(2048) not null encode zstd
            ) distkey (company_id)
              sortkey (company_id);
    ''',
    'customers': '''
    create table customers
    (
      customer_id    bigint        not null unique primary key,
      full_name      varchar(2048) not null encode zstd,
      dob            date          not null encode zstd,
      user_name      varchar(2048) not null encode zstd,
      email          varchar(2048) not null encode zstd,
      most_freq_city varchar(2048) not null encode zstd,
      most_freq_cnty char(3)       not null encode zstd
    ) distkey (customer_id)
      sortkey (customer_id);
      ''',
    'products': '''
    create table products
    (
      prod_id            bigint         not null unique primary key,
      prod_name          varchar(4096) encode zstd,
      prod_unit_price    decimal(15, 4) not null,
      prod_currency_code char(3)        not null encode zstd,
      supplier_id        bigint         not null,
      foreign key (supplier_id)
        references companies (company_id)
    ) distkey (prod_id)
      compound sortkey (prod_id);
      ''',
    'orders': '''
    create table orders
    (
      order_id         bigint         not null unique primary key,
      prod_id          bigint         not null,
      customer_id      bigint         not null,
      prod_quote_price decimal(15, 4) not null,
      is_lead_sale     bool           not null default true,
      crm_lead_Id      bigint         not null default -1,
      company_id       bigint         not null,
      created_date     timestamp      not null,
      foreign key (prod_id)
        references products (prod_id),
      foreign key (customer_id)
        references customers (customer_id),
      foreign key (company_id)
        references companies (company_id)
    ) distkey (order_id)
      sortkey (order_id, prod_id);
      ''',
}

METRIC_TABLES = {
    'monthly_sales': '''
    create table monthly_sales
    (
      sale_month     int            not null,
      sale_year      int            not null,
      monthly_sales  decimal(18, 2) not null,
      monthly_profit decimal(18, 2) not null
    );    
    ''',
    'total_leads': '''
    create table total_leads
    (
      customer_id int not null,
      total_leads int not null
    );
    ''',
    'most_used_devices': '''
    create table most_used_devices
    (
      device        varchar(256) not null,
      total_devcies int          not null
    );
    ''',
    'most_popular_products': '''
    create table most_popular_products
    (
      prod_id int     not null,
      country char(2) not null
    );
    ''',
}

log = logging.getLogger()


def copy_csv_to_tbl(tbl_name: str, s3_path: str):
    sql_stmt = f"copy {tbl_name} from '{s3_path}' credentials 'aws_access_key_id={S3_SETTINGS['aws_access_key_id']};aws_secret_access_key={S3_SETTINGS['aws_secret_access_key']}' delimiter ','"
    if tbl_name == 'customers':
        sql_stmt += " dateformat 'YYYY-MM-DD';"
    elif tbl_name == 'orders':
        sql_stmt += " timeformat 'YYYY-MM-DDTHH:MI:SS';"
    else:
        sql_stmt += ";"

    CON.cursor().execute(sql_stmt)


def copy_metrics_csv_to_tbl(tbl_name: str, s3_path: str):
    sql_stmt = f"copy {tbl_name} from '{s3_path}' credentials 'aws_access_key_id={S3_SETTINGS['aws_access_key_id']};aws_secret_access_key={S3_SETTINGS['aws_secret_access_key']}' ignoreheader 1 delimiter ',';"
    CON.cursor().execute(sql_stmt)


def drop_table(tbl_name: str):
    CON.cursor().execute(f'drop table if exists {tbl_name} cascade;')


def drop_and_create_table(tbl_name: str):
    sql_stmt = ''
    if tbl_name in INIT_TABLES:
        sql_stmt = INIT_TABLES[tbl_name]
    elif tbl_name in METRIC_TABLES:
        sql_stmt = METRIC_TABLES[tbl_name]

    drop_table(tbl_name)
    CON.cursor().execute(sql_stmt)


def get_redshift_customer_user_names() -> [str]:
    cur = CON.cursor()
    cur.execute('select user_name from customers;')
    return [name[0] for name in cur.fetchall()]
