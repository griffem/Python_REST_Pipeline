import requests, json, sys
import pandas as pd

test_collection = "EmeryTest"
prod_collection = "EmeryExample"
data_source = "DDAssessmentSampleData.xlsx"

def main(prod_run=False):
    if prod_run:
        collection = prod_collection
    else:
        collection = test_collection
        
    data_items = get_json_items(data_source)

# Reads excel file, adds unique key, then converts to JSON format
def get_json_items(file_path):
    df = pd.read_excel(file_path, sheet_name=0)
    
    # Add unique row number key
    df.index = [i for i in range(1, len(df.values)+1)]
    
    # Convert Discounts to float
    df['Discounts'] = df['Discounts'].replace(' $-   ', '0.0', regex=False).astype(float)

    # Convert table to iterable json format
    json_raw = json.loads(df.to_json(orient='table'))
    
    # Get primary key from DF
    primary_key = json_raw['schema']['primaryKey'][0]
    
    items_list = []
    
    # Iterate through each row of data
    for row in json_raw['data']:
        item = { 'Key': '', 'Attributes': [] }
        
        # Iterates through each of the column fields
        for field in json_raw['schema']['fields']:
            cell_value = row[field['name']]
            
            # If primary key is the field, populate it in the key portion instead of attributes
            if field['name'] == primary_key:
                item['Key'] = str(cell_value)
                
            else:
                row_data = {
                    'Name': field['name'], 
                    'Type': field['type'],
                    'Value': cell_value
                }
                
                item['Attributes'].append(row_data)
                
        items_list.append(item)
        
    # Converts Python object to JSON
    return json.dumps(items_list)
    
if __name__ == "__main__":
    prod_run = False 
    
    # Checks if "prod" is in console run arguments, if yes sets prod_run to True
    if len(sys.argv) > 1:
        if sys.argv[1] == 'prod':
            prod_run = True
    print(prod_run)
    main(prod_run)