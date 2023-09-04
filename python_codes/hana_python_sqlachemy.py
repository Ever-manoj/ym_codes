from sqlalchemy import create_engine
import pandas as pd
import hdbcli
import sqlalchemy_hana
from sqlalchemy import text

try :
    # engine = create_engine('hana+hdbcli://user:password@hana-cockpit-001.cfapps.us10.hana.ondemand.com/?encrypt=true&sslCryptoProvider=openssl&sslTrustStore=DigiCertGlobalRootCA.pem&webSocketURL=/service/<hana-db-instance-guid>&proxy_host=proxy&proxy_port=8080')
    # engine = create_engine("saphana:///?User=system&Password=mypassword&Server=localhost&Database=systemdb")
    engine = create_engine(
        'hana+hdbcli://dbadmin:January2023@d12d2145-d8a4-4c88-9dad-33bbc132504c.hana.trial-us10.hanacloud.ondemand.com:443')
    # db_connection = "hana+hdbcli://Username:Password@Host:port/tennat_db_name"
    print(engine)
    # query = 'select * from tables'
    '''query = "create table dbadmin.sales_demo_table_3(Product_Id Integer, Transaction_Id varchar(20),Return_date date," \
            "Region varchar(10));"
    # query = "select * from SYS.M_TABLES where SCHEMA_NAME ='DBADMIN'"
    query = "select * from dbadmin.ml_predicted_sales"
    t = text(query)
    conn = engine.connect()
    result_set = conn.execute(t)
    print(result_set)
    result_list = []
    for result in result_set:
        print(result)
        result_list.append(result)
    print("\nlist of result set is\n")
    print(result_list)
    '''
    conn = engine.connect()
    predicted_file_df = pd.read_csv(
        "G:\\My Drive\\Dell\\DataForce2.0\\backend\\Data_force_2\\dataforce_project\\df_data\\udata\\1_new_hana_conn_2_ml-predicted-data.csv",
        sep=',', header=1)
    print("predicted_file_df:%s" % predicted_file_df)
    db_admin = 'dbadmin'
    ml_sales_table = 'ml_sales_table'
    # engine = src_conn.get_bind()
    print("dest_schema:%s, dest_table:%s, engine:%s" % (db_admin, ml_sales_table, engine))
    predicted_file_df.to_sql(schema=db_admin, name=ml_sales_table, con=conn, index=False, if_exists='replace')
    print("finished")
except Exception as err :
    print(err)

'''
pip install sqlalchemy
pip install sqlalchemy-hana
pip install hdbcli
'''
