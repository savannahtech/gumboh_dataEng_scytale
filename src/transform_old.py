import os
import json
import pyarrow.parquet as pq

def transform_data():

    # Load latest extract file
    files = os.listdir('../data/raw_data') 
    latest = max(files, key=os.path.getctime)
    
    with open(latest) as f:
        data = json.load(f)
        
    print(f'Loaded {latest}')
    
    # Transform steps here    
    transformed_data = transform(data)

    # Define output directory and file
    out_dir = '../data/results'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    outfile = f'{out_dir}/transformed_data.parquet'
    
    # Write transformed data to file 
    write_to_parquet(transformed_data, outfile)
    
    print(f'Saved transformed data to {outfile}')

def transform(data):
    # Transform raw data 
    return transformed_data

def write_to_parquet(data, outfile):
    # Code to write data to parquet forma
    pq.write_table(table, outfile)
    pass
    ...
