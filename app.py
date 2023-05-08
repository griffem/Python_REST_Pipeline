import requests, json, sys, yaml, time
import pandas as pd

config = yaml.safe_load(open('config.yml', mode='r'))

def main(prod_run=False):
    url = config['url']
    
    # Determine to use test or production collection for Data Lake
    if prod_run:
        collection = config['prod_collection']
    else:
        collection = config['test_collection']
        
    print("Reading and transforming data")
    # Get data as a Python obj to be converted to json
    data_items = get_data_items(config['data_source'])
    
    print("Authenticating to Data Lake")
    # Get access token from Data Lake
    token = auth(url, config['user'], config['pass'])
    
    print("Populating data into Data Lake")
    # Interatively populate data into Data Lake
    populate_data(url, collection, token, data_items['data'])
    
    print("Validating population results")
    # Validate collection metadata and first and last rows are correct
    validate_population(url, token, collection, data_items)
    
    print("Data Population complete")
    
def auth(url, user, pwd):
    data = { 'Grant': 'password', 'Username': user, 'Password': pwd }

    # Send a post request to get an access token
    auth_response = requests.post(f"{url}/authenticate", data=data)

    return auth_response.json()['accesstoken']

# Created due to getting occasional "Access Denied" even when there is a valid key
def attempt_request(url, headers, data):
    resp = requests.post(url, headers=headers, data=data).json()
    
    # Every 1 second, send request till it passes
    while request_denied_check(resp):
        time.sleep(1)
        resp = requests.post(url, headers=headers, data=data).json()
    
    return resp
    
# Check if REST response denied accesss
def request_denied_check(response):
    if 'Message' in response.keys():
        if response['Message'] == 'Access Denied':
            return True
        
    return False
            
# Returns Python obj for data header
def get_data_header(collection, items):
    data = {
        'CollectionName': collection,
        'Items': items
    }
    
    return json.dumps(data)

# Iterate through data items to post 100 at a time
def populate_data(url, collection, token, data_list):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    for i in range(0, len(data_list)+1, 100):
        # Get current chunk from data list
        to_submit = data_list[i:i+100]
        
        # Get data header in JSON
        data_header = get_data_header(collection, to_submit)
        
        # Post data
        attempt_request(f"{url}/additems", headers, data_header)
        
def validate_population(url, token, collection, data_items):
    headers = { 'Authorization': f'Bearer {token}' }
    
    # Validation collection metadata matchs data items
    result = validate_collection_metadata(url, headers, collection, data_items)
    if result != True:
        raise Exception(result)
    
    # Validate first and last rows of data items match in Data Lake
    result = validate_first_last_rows(url, headers, collection, data_items)
    if result != True:
        raise Exception(result)
    
def validate_collection_metadata(url, headers, collection, data_items):
    collection_details = attempt_request(f"{url}/getcollectiondetails", headers, {'CollectionName': collection})

    # Ensure row counts match
    if collection_details['Count'] != len(data_items['data']):
        return f"Collection Item Count ({collection_details['Count']}) does not match Data Row Count ({len(data_items['data'])})"
    
    # Ensure all fields are present
    for item_field in data_items['fields']:
        match = False
        
        if item_field['name'] == 'index':
            continue
        
        if item_field['type'] == 'datetime':
            item_field['type'] = 'date'
            
        # Iterate through collection fields and check for Name and Type match
        for coll_field in collection_details['Fields']:
            if item_field['name'] == coll_field['Name']:
                if item_field['type'] == coll_field['Type']:
                    match = True
                    break
        
        if match != True:
            return f"Not all Item Fields ({item_field}) populated into Collection Fields"
    
    return True

def validate_first_last_rows(url, headers, collection, data_items):
    field_name_list = []
    
    for field in data_items['fields']:
        field_name_list.append(field['name'])
        
    # Get first and last row of data for data comparison
    first_row = data_items['data'][0]
    last_row = data_items['data'][len(data_items['data'])-1]
    
    first_row_req = { 'CollectionName': collection, 'Attributes': field_name_list, 'Key': first_row['Key'] }
    last_row_req = { 'CollectionName': collection, 'Attributes': field_name_list, 'Key': last_row['Key'] }
    
    # Get first row of data from Data Lake, convert to Python object
    first_row_resp = attempt_request(f"{url}/getitem", headers, first_row_req)
    
    # Compare attributes between first row data item and response from data lake
    if compare_rows(first_row, first_row_resp) == False:
        return "First row validation did not pass"
    
    # Get last row of data from Data Lake, convert to Python object
    last_row_resp = attempt_request(f"{url}/getitem", headers, last_row_req)
    
    # Compare attributes between last row data item and response from data lake
    if compare_rows(last_row, last_row_resp) == False:
        return "Last row validation did not pass"
    
    return True

# Compare every attribute within a row in dataset and row returned from Data Lake
def compare_rows(item_row, response):
    # If item is not found, return False
    if response['ItemFound'] != True:
        return False 
    
    # Iterate through each attribute in Data Lake and in original data to check for match
    for item_attr in item_row['Attributes']:
        match = False
        
        for coll_attr in response['Item']['Attributes']:
            # Check for Name, Type and Value Match
            if item_attr['Name'] == coll_attr['Name']:
                if item_attr['Type'] == coll_attr['Type']:
                    if str(item_attr['Value']) == str(coll_attr['Value']):
                        match = True
                    # Special case for how Data Lake changes the time component of date
                    elif item_attr['Type'] == 'date':
                        if item_attr['Value'].split('T')[0] == coll_attr['Value'].split('T')[0]:
                            match = True
                    # Remove .0 from item if it is preventing match check
                    elif '.0' in str(item_attr['Value']) and item_attr['Type'] == 'number':
                        if int(item_attr['Value']) == int(coll_attr['Value']):
                            match = True 
                            
        if match == False:
            return False
    
    return True

# Reads excel file, adds unique key, then converts to Python object that can be converted to Data Lake JSON
def get_data_items(file_path):
    df = pd.read_excel(file_path, sheet_name=0)
    
    # Add unique row number key
    df.index = [i for i in range(1, len(df.values)+1)]
    
    # Convert datatypes
    df['Discounts'] = df['Discounts'].replace(' $-   ', '0.0', regex=False).astype(float)
    df['Profit'] = df['Profit'].replace(' $-   ', '0.0', regex=False).astype(float)
    
    df = df.astype({
        'MonthNumber': str,
        'Year': str,
        'SalePrice': float,
        'ManufacturingPrice': float
    })
    
    df = df.fillna("")
    
    # Convert table to JSON then to Python object
    df_data = json.loads(df.to_json(orient='table'))
    
    # Get primary key from dataframe
    primary_key = df_data['schema']['primaryKey'][0]

    items_list = []
    
    # Iterate through each row of data
    for row in df_data['data']:
        item = { 'Key': '', 'Attributes': [] }
        
        # Iterates through each of the column fields
        for field in df_data['schema']['fields']:
            cell_value = row[field['name']]
            
            # If primary key is the field, populate it in the key portion instead of attributes
            if field['name'] == primary_key:
                item['Key'] = str(cell_value)
                
            else:
                # Convert datetime value to date
                if field['type'] == 'datetime':
                    field['type'] = 'date'
                    
                row_data = {
                    'Name': field['name'], 
                    'Type': field['type'],
                    'Value': cell_value
                }
                
                item['Attributes'].append(row_data)
                
        items_list.append(item)
        
    return { 'data': items_list, 'fields': df_data['schema']['fields'] }
    
if __name__ == "__main__":
    prod_run = False 
    
    # Checks if "prod" is in console run arguments, if yes sets prod_run to True
    if len(sys.argv) > 1:
        if sys.argv[1] == 'prod':
            prod_run = True

    main(prod_run)