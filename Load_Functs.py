import pandas as pd
import requests
import networkx as nx
import json
import time

from datetime import datetime

def  Load_Function(DataFrame, api_key, batch_size=100):
    
    """
    Sends the information contact data to the HubSpot API in batches.

    Args:
        DataFrame : A DataFrame containing contact information. The required columns are: 
                           'Email', 'Phone_Number', 'City/Country', 'firstname', 'lastname', 'address', 
                           'technical_test___create_date', 'industry', and 'hs_object_id'.
        api_key (str): HubSpot API key for authentication.
        batch_size: Number of contacts to send in each batch. Default is 100.

    Returns:
        list: A list of responses from the HubSpot API for each batch.

    Description:
        This function sends contact information in batches to the HubSpot API endpoint for creating contacts.
        It extracts and formats the contact data from the provided DataFrame, and sends it via POST requests.
        The function collects and returns the API responses for further inspection or logging.
    """

    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"

    headers = {
        "Authorization": f'Bearer {api_key}',  
        "Content-Type": "application/json"
    }

    responses = []

    for index in range(0, len(DataFrame), batch_size):
        contact_batch = DataFrame.iloc[index:index + batch_size]

        inputs = []
        for _, row in contact_batch.iterrows():

            City_Country = row["City/Country"]
            City = City_Country[0]
            Country = City_Country[1]
            Contacts = {
                "properties": {
                    "email": row["Email"],
                    "phone": row["Phone_Number"],
                    "country": Country,
                    "city": City,
                    "firstname": row["firstname"],
                    "lastname": row["lastname"],
                    "address": row["address"],
                    "original_create_date": row["technical_test___create_date"].strftime("%Y-%m-%d"),
                    "original_industry": row["industry"],
                    "temporary_id": row["hs_object_id"]
                }
            }
            inputs.append(Contacts)
        
        payload = {"inputs": inputs}
        
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        responses.append(response.json())

        # if response.status_code == 429:
        #     print("Rate limit reached, waiting...")
        #     time.sleep(10)

    return responses

#------------------------------------------------------------------------------------------------
def Properties_Multiple_CheckB(api_key,Options_Indust):

    """
    Function to select which options should be registered in the Multiple checkboxes property.

    Args:
        api_key (str): HubSpot API key for authentication.
        Options_Indust : A list of industry options to be added as checkboxes under the 'original_industry' property.

    Returns:
        dict: The JSON response from the HubSpot API, providing details about the operation's success or failure.

    Description:
        This function sends a PUT request to the HubSpot API to update the 'original_industry' contact property. 
        It converts the provided list of industry options into the format required for a multi-checkbox field and 
        updates the property with these new options. The API response is printed to provide feedback on the 
        operation's success.
    """

    url = "https://api.hubapi.com/properties/v1/contacts/properties/named/original_industry"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    new_options = [{"label": Option, "value": Option} for Option in Options_Indust]

    payload = {
        "name": "original_industry",
        "label": "Original Industry",
        "groupName": "contactinformation",
        "type": "enumeration",  
        "fieldType": "checkbox",  
        "options": new_options,
        "mutability": "READ_WRITE",
        "formField": True
    }

    response = requests.put(url, headers=headers, json=payload)
    print(response.json())