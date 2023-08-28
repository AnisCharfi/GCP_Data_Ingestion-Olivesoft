import os
import pandas as pd
import numpy as np
import io
from flask import Flask, jsonify, request
from google.cloud import storage, bigquery
from google.oauth2 import service_account

app = Flask(__name__)

json_file_path = os.path.join(os.path.dirname(__file__), 'os-gc-dpf-prj-ing-02-dev-b206b8390fa5.json')
storage_client = storage.Client.from_service_account_json(json_file_path)
bigquery_client = bigquery.Client.from_service_account_json(json_file_path)
credentials = service_account.Credentials.from_service_account_file(json_file_path)
project_id = credentials.project_id
ingested_count = 0
non_ingested_count = 0

def load_csv_to_dataframe(file_content):
    return pd.read_csv(io.StringIO(file_content), sep=';')

def create_or_update_table(blob, dataset_ref, table_name, df, keys):
    
    table_ref = dataset_ref.table(table_name)
    try:
        bigquery_client.get_table(table_ref, retry=bigquery.DEFAULT_RETRY)
        table_exists = True
    except:
        table_exists = False

    if table_exists:
        table = bigquery.Table(table_ref)
        existing_rows = bigquery_client.list_rows(table).to_dataframe().astype(str) #j'ai ajouté .astype(str)
        
        ex_rows = existing_rows.iloc[:, :-2]

        for column in df.columns:

                # Convertissez uniquement les valeurs non-NaN en type entier

                if(df[column].dtype==np.float64):

                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(pd.Int32Dtype())
                    
                #print(df['parentId'].isna())


        df = df.astype(str) # à ne pas modifier
        df.replace("<NA>", "None", inplace=True)
        print(df)
        '''
        for column in df.columns:                      
            if type(df[column]) == float :
                print(f'column  {column.name} = {type(df[column])}')
        '''
        '''
        df['parentId'] = df['parentId'].astype(int) #à verifier
        print(type(df['parentId']))
        df['parentId'] = df['parentId'].astype(str) #à verifier
        print(type(df['parentId']))
        '''

        #df = str(df) # boucle for : changer les valeurs de chaque colonne
        
        #for column in df.columns:                      
            #df[column]=df[column].astype(str)
        #df['parentId'] = int(df['parentId'])
        #df = df.astype({'parentId':'int'})
        #print(f'df : {df}')
        #df = df.astype({'parentId':'str'})
        #print(f'df : {df}')
        #df['parentId'] = str(df['parentId'])
        '''
        df = df.replace("nan", "None")
        print(f'df :\n{df}')
        df['parentId']=df['parentId'].astype(int)
        df['parentId']=df['parentId'].astype(float).astype(int)
        
        print(df['parentId'])
        '''
        print(f'existing_rows :\n {ex_rows}')
        #print(type(df['parentId']))
        new_rows = df.merge(ex_rows, indicator=True, how='left').loc[lambda x: x['_merge'] == 'left_only'].drop(columns='_merge')        
        print(f'new_rows : \n {new_rows}')
        new_rows.replace("None", None, inplace=True)
        print(f'new_rows : \n {new_rows}')
        if not new_rows.empty :
            for index1, row1 in new_rows.iterrows():
              
                key1 = ""
                statement = ""
                for i in range(len(keys)):
                    key1 += str(row1[keys[i]])
                    if isinstance(row1[keys[i]], str):
                        statement += f"{keys[i]} = '{row1[keys[i]]}' and "
                    else:
                        statement += f"{keys[i]} = {row1[keys[i]]} and "
                    #print(f'key1 = {key1}')
                        
                statement = statement[:-4]

                verif = False
                for index2, row2 in existing_rows.iterrows():
                    key2 = ""
                    for i in range (len(keys)) : 
                        key2+=str(row2[keys[i]])
                    #print(f'key2 = {key2}')
                    if key1==key2:
                        verif=True
                        
                        delete_query = ( f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` " f"WHERE {statement}" )
                        query_job = bigquery_client.query(delete_query)
                        query_job.result()
                        
                        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
                        row1['last_modification'] = str(pd.to_datetime('now'))
                        row1['insertion_timestamp'] = str(row2['insertion_timestamp'])

                        job = bigquery_client.load_table_from_dataframe(pd.DataFrame([row1]), table_ref, job_config=job_config)
                        job.result()
                        
                        print(f"Rows updated in Table {table_name}.")
                        break

                if(not verif):
                        row1['last_modification'] = str(pd.to_datetime('now'))
                        row1['insertion_timestamp'] = str(pd.to_datetime('now'))
                    
                        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
                        job = bigquery_client.load_table_from_dataframe(pd.DataFrame([row1]), table_ref, job_config=job_config)
                        job.result()
                        
                        print(f"File {blob.name} added new data in {table_name}.") 
        else :
            print(f'no changes in file {blob.name} ')               
    


    else:
        schema = []
        for col_name in df.columns:
            schema.append(bigquery.SchemaField(col_name, "STRING"))
        df['last_modification'] = str(pd.to_datetime('now'))
        df['insertion_timestamp'] = str(pd.to_datetime('now'))
        job_config = bigquery.LoadJobConfig(
            schema = schema,
            source_format=bigquery.SourceFormat.CSV,
        )
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"File {blob.name} successfully ingested into BigQuery!")
        #added section
        '''table = bigquery.Table(table_ref)
        existing_rows = bigquery_client.list_rows(table).to_dataframe()
        print(existing_rows)
        print(existing_rows.isna())'''
        
    
def process_blob(blob, dataset_ref, tables):
    if blob.name.endswith('.csv'):
        file_content = blob.download_as_text()
        df = load_csv_to_dataframe(file_content)

        base_name = os.path.splitext(os.path.basename(blob.name))[0]  # Remove extension
        parts = base_name.split('_')  # Split based on underscores
        table_name = "raw_" + parts[0]
        #table_name = os.path.splitext(os.path.basename(blob.name))[0].lower().replace(' ', '_')
        for table in tables:
            tname = table["table_name"]
            keys = table ["keys"] 
            if tname == table_name:
                create_or_update_table(blob, dataset_ref, table_name, df, keys)
                break   
        
    
               

def create_archive_and_error_folders(bucket, folder_name):
    archive_folder = bucket.blob(f"Archive/{folder_name}/")
    if not archive_folder.exists():
        archive_folder.upload_from_string('')

    error_folder = bucket.blob(f"Error/{folder_name}/")
    if not error_folder.exists():
        error_folder.upload_from_string('')

def move_blob_to_folder(blob, bucket, folder_name, subfolder_name):
  
    blob_name = blob.name.split("/")[-1]
    source_blob = bucket.blob(blob.name)
    destination_blob = bucket.blob(f"{subfolder_name}/{folder_name}/{blob_name}")

    # Télécharger le contenu du blob source et l'uploader vers le blob de destination
    destination_blob.upload_from_string(source_blob.download_as_string())

    # Supprimer le blob source
    source_blob.delete()

def process_blob_with_error_handling(bucket, blob, dataset_ref, tables, folder_name):
    global ingested_count, non_ingested_count
    try:
        process_blob(blob, dataset_ref, tables)
        move_blob_to_folder(blob, bucket, folder_name, "Archive")
        ingested_count += 1
    except Exception as e:
        move_blob_to_folder(blob, bucket, folder_name, "Error")
        non_ingested_count += 1
def process_blobs_in_folder(bucket, folder_name, dataset_ref, tables):
    blobs = list(bucket.list_blobs(prefix=folder_name))
    
    for blob in blobs:
        process_blob_with_error_handling(bucket, blob, dataset_ref, tables, folder_name)

@app.route('/primary_keys', methods=['GET','POST'])
def convert_csv_to_bigquery():
    global ingested_count, non_ingested_count
    print("start")
    try:

        data_config = request.json

        dataset_id = data_config.get('dataset')
        bucket_name = data_config.get('bucket_name')
        folder_name = data_config.get('folder_name')
        tables = data_config.get('tables')
       
        bucket = storage_client.bucket(bucket_name)
       
        blobs = list(bucket.list_blobs(prefix=folder_name))
        blobs = [blob for blob in blobs if not blob.name.endswith('/')]

        dataset_ref = bigquery_client.dataset(dataset_id)

        create_archive_and_error_folders(bucket, folder_name)
        process_blobs_in_folder(bucket, folder_name, dataset_ref, tables) 
        print(f"Total files ingested: {ingested_count}")
        print(f"Total files not ingested (moved to Error folder): {non_ingested_count}")
        ingested_count = 0
        non_ingested_count = 0
        return jsonify({'message': 'CSV files converted to BigQuery tables successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
    #print(f"Total files ingested: {ingested_count}")
    #print(f"Total files not ingested (moved to errors folder): {non_ingested_count}")

'''
'''