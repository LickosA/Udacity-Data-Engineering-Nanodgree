import config
from rib import RIB
from rov import ROV
from roa import ROA
from bgp import BGP
from datetime import date, datetime, timedelta
import pandas as pd
#import cassandra
import numpy as np
import os, sys

import warnings
warnings.filterwarnings("ignore")

#from cassandra.cluster import Cluster


def download_rib(url):
    rib = RIB()
    file = rib.retrieveFile(url)
    df = rib.parseMRT(config.TMP_DIR + file)
    return df

def process_rov(df,d):
    rpki_dir = config.DEFAULT_RPKI_DIR 
    ##Compute RPKI archive URLs
    
    rpki_url = []
    rpki_dir += "/" + "/".join([str(d.year), str(d.month), str(d.day)]) + '/' 
    for url in config.RPKI_ARCHIVE_URLS:
        rpki_url.append( url.format(year=int(d.year), month=int(d.month), day=int(d.day)) )
        
    rov = ROV(rpki_url, rpki_dir=rpki_dir)
    rov.download_databases(False)
    rov.load_rpki()
    
    df['status'] = df.apply(rov.check, axis=1)
    df = df.loc[df.status.str.contains('Invalid')]

    return df

def process_roa(df):
    roa = ROA()
    
    #define a data model for the ROA dataframe
    df_roas = pd.DataFrame(columns={'prefix','tal','roa_creation_time'})
    
    for index, row in df.iterrows():
        res = roa.getROACreationDates(row['prefix'])
        
        if not res:
            continue
            
        timestamps = res['dates']
        tal = res['tal']
        prefix = row['prefix']
        #print(prefix, tal, timestamps)
        
        for t in timestamps:
            #df_roas.loc[df_roas.shape[0]] = [prefix, tal, t]
            df_roas = df_roas.append({'prefix':prefix,'tal':tal,'roa_creation_time':t},ignore_index=True)
    #filter out roas with creation time < 2018
    df_roas = df_roas.loc[df_roas.roa_creation_time > '2018-01-01']
    
    # take a sample (n=20) from each TAL, if it fails just take everything
    df_arin = df_roas.loc[df_roas.tal == 'arin']
    df_apnic = df_roas.loc[df_roas.tal == 'apnic']
    df_ripe = df_roas.loc[df_roas.tal == 'ripencc']
    df_lacnic = df_roas.loc[df_roas.tal == 'lacnic']
    df_afrinic = df_roas.loc[df_roas.tal == 'afrinic']

    try:
        df_roas = pd.concat([df_arin.sample(n=20), df_apnic.sample(n=20), df_ripe.sample(n=20), df_lacnic.sample(n=20), df_afrinic.sample(n=20)], ignore_index=True)
    except Exception as e:
        pass      
    return  df_roas

def create_cluster():
    # This should make a connection to a Cassandra instance your local machine 
    # (127.0.0.1)


    cluster = Cluster()

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    # TO-DO: Create a Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")


    # TO-DO: Set KEYSPACE to the keyspace specified above
    session.set_keyspace('udacity')

    query = "CREATE TABLE IF NOT EXISTS roa_timing "
    query = query + "(prefix text, tal text, peer_ip inet, roa_create_time timestamp, withdrawal_time timestamp, delta float, PRIMARY KEY (prefix, peer_ip))"
    session.execute(query)
    return session

def process_bgp(df_roas, session, date):
    #### /!\/!\/!\/!\/!\this takes a lot of time
    bgp = BGP()
    
    if (session is None):
        f = open(config.OUTPUT_FILE.format(date.isoformat()),'w')
        f.write("prefix,tal,peer_ip,roa_create_time,withdrawal_time,delta\n")
    
    for index, row in df_roas.iterrows():
        prefix = row['prefix']
        t = row['roa_creation_time']
        tal = row['tal']

        elements = {}

        elements = bgp.extractWithdrawalTimePyBGPStream(prefix, t, 1, False)

        for peer_ip, wtime in elements.items():
            roa_create_time = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
            withdrawal_time = datetime.fromtimestamp(int(wtime))
            delta = (withdrawal_time - roa_create_time).seconds
            
            if (session is None):
                f.write("{},{},{},{},{},{}\n".format(prefix,tal,peer_ip,roa_create_time,withdrawal_time,delta))
            else:
                query = "INSERT INTO roa_timing (prefix, tal, peer_ip, roa_create_time, withdrawal_time, delta)"
                query = query + "VALUES (%s, %s, %s, %s, %s, %s)"
                session.execute(query, (prefix, tal, peer_ip, roa_create_time, withdrawal_time, delta))
            
        if (session is None):
            f.close()

def main():
    """
    This is the main function to start the ETL process
    :return: None
    """
    
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == '-date':
        y = datetime.strptime(args[1], '%Y-%m-%d')
    else:
        d = date.today()
        y = d - timedelta(days=1)
        # Read in the data here

    #get yesterday date
    url = config.RIB_SOURCE_URL.format(y.year,str(y.month).zfill(2),str(y.day).zfill(2))
    
    print('Downloading: ' + url)
    df = download_rib(url)
    
    print('Processing ROV')
    df = process_rov(df,y)
    
    print('Retrieving ROAs')
    df_roas = process_roa(df)
    
    #print('Create cluster for storage')
    #session = create_cluster()
    
    print('Retrieving BGP messages')
    process_bgp(df_roas, session=None, date=y)
    
    ##display the results
    #query = "SELECT prefix, tal, peer_ip, roa_create_time, withdrawal_time, delta from roa_timing"
    #rows = session.execute(query)
    #for row in rows:
    #    print(row.prefix, row.tal, row.peer_ip, row.roa_create_time, row.withdrawal_time, row.delta)
    
    #session.shutdown()
    #cluster.shutdown()
    
if __name__ == "__main__":
    main
    