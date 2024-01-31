import requests
import json
import datetime
import os

def extract_github_data():
    
    url = 'https://api.github.com/users/pytorch/repos'

    # API call to extract data
    # data = extract_data()
    data = requests.get(url).json()
    
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    # Construct output file path
    out_dir = 'data/raw_data'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    out_file = f'{out_dir}/github_data_{timestamp}.json'
    
    # Save extract 
    with open(out_file, 'w') as f:
        json.dump(data, f)
        
    print(f'Wrote extract to {out_file}')

    try:
            with open(out_file, 'w') as f:
                        json.dump(data, f)
    except IOError: 
            print("Error writing file")
