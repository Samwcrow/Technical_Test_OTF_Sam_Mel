import pandas as pd
import requests
import networkx as nx
import json

#from functools import lru_cache

def get_contacts(api_key, limit, properties):

    """
    Function that collects contacts from the HubSpot API based on specific filters and returns them as a DataFrame.

    Arguments:
        api_key (str): The API key used for authentication with the HubSpot API.
        limit (int): The maximum number of contacts to collect per request.
        properties (list): A list of contact properties that need to be collected.     

    Returns:
        DataFrame: A DataFrame containing the filtered contact data, removing some extra columns.
    
    Description:
        This function retrieves contact information from the HubSpot CRM API using a POST request with specified filters.
        , collecting data across multiple pages until all contacts are retrieved or a maximum of
        iterations is reached. If the API request fails, the function stops and prints the error message.
    """

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    
    headers = {
        "Authorization": f'Bearer {api_key}',  
        "Content-Type": "application/json"
    }

    data = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "allowed_to_collect", 
                "operator": "EQ",                     
                "value": "true"                       
            }]
        }],
        "properties": properties,
        "limit": limit  
    }

    all_contacts = []
    after = None  
    IndexRequest = 0 

    while True:
        print(f'Loop #{IndexRequest}')
        if IndexRequest == 31:
            break
        IndexRequest += 1
        
        if after:
            data['after'] = after 
        
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            response_data = response.json()

            properties_list = [entry['properties'] for entry in response_data.get('results', [])]

            all_contacts.extend(properties_list) 
            
            if 'paging' in response_data and 'next' in response_data['paging']:
                after = response_data['paging']['next']['after'] 

            else:
                break 
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    DataFrame = pd.DataFrame(all_contacts)
    DataFrame = DataFrame.drop(columns=['createdate', 'lastmodifieddate'])
    return DataFrame
   