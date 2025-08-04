
"""Service to interact with SAP BusinessObjects Universes"""
import time
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict, Union, Optional
from requests import RequestException
from Helpers.Constants import BASE_URL_SAP as BASE_URL

def get_headers(token: str) -> Dict[str, str]:
    """Generate headers with SAP token."""
    return {
        'X-SAP-LogonToken': token,
        'Accept-Language': 'fr',
        'Accept': 'application/xml',
        'Content-Type': 'application/xml'
    }

def get_token(username: str, password: str, auth_type: str = "secWinAD") -> Union[str, None]:
    """Retrieve SAP connection token."""
    url = f"{BASE_URL}/logon/long"
    headers = {'Content-Type': 'application/xml'}
    payload = f"""
    <attrs xmlns="http://www.sap.com/rws/bip">
        <attr name="userName" type="string">{username}</attr>
        <attr name="password" type="string">{password}</attr>
        <attr name="auth" type="string" possibilities="secEnterprise,secLDAP,secWinAD,secSAPR3">{auth_type}</attr>
    </attrs>
    """
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        token = response.headers.get('X-SAP-LogonToken', None)
        return token[1:-1] if token else None
    except RequestException as e:
        print(f"Error retrieving token: {e.response.text}")
        return None

def logoff(token: str) -> Union[str, None]:
    """Log off from SAP BI Platform."""
    url = f"{BASE_URL}/logoff"
    headers = get_headers(token)
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.text
    except RequestException as e:
        print(f"Error logging off: {e.response.text}")
        return None

def get_universe_list(token: str) -> Union[pd.DataFrame, None]:
    """Retrieve list of Universes."""
    url = f"{BASE_URL}/raylight/v1/universes"
    headers = get_headers(token)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        universes = []
        for universe in root.findall('universe'):
            universe_dict = {
                'id': universe.find('id').text,
                'name': universe.find('name').text,
                'cuid': universe.find('cuid').text
            }
            universes.append(universe_dict)
        return pd.DataFrame(universes)
    except RequestException as e:
        print(f"Error retrieving universe list: {e.response.text}")
        return None

def get_universe_details(token: str, universe_id: int) -> Union[Dict, None]:
    """Retrieve Universe folder structure and objects."""
    url = f"{BASE_URL}/raylight/v1/universes/{universe_id}/details"
    headers = get_headers(token)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        folders = []
        for folder in root.findall('.//folder'):
            folder_dict = {
                'id': folder.find('id').text,
                'name': folder.find('name').text,
                'objects': []
            }
            for obj in folder.findall('.//object'):
                obj_dict = {
                    'id': obj.find('id').text,
                    'name': obj.find('name').text,
                    'type': obj.find('type').text,  # dimension, measure, attribute
                    'cuid': obj.find('cuid').text
                }
                folder_dict['objects'].append(obj_dict)
            folders.append(folder_dict)
        return {'folders': folders}
    except RequestException as e:
        print(f"Error retrieving universe details: {e.response.text}")
        return None

def build_query_xml(universe_id: int, selected_objects: List[Dict[str, str]], filters: List[Dict[str, str]]) -> Union[str, None]:
    """Build XML for querying a Universe with selected objects and filters."""
    query = ET.Element("query")
    query.set("universeId", str(universe_id))
    result_objects = ET.SubElement(query, "resultObjects")
    for obj in selected_objects:
        obj_elem = ET.SubElement(result_objects, "object")
        obj_elem.set("id", obj["id"])
        obj_elem.set("cuid", obj["cuid"])
    query_filters = ET.SubElement(query, "filters")
    for filt in filters:
        if {'object_id', 'cuid', 'operator', 'values'}.issubset(filt):
            filt_elem = ET.SubElement(query_filters, "filter")
            filt_elem.set("objectId", filt["object_id"])
            filt_elem.set("cuid", filt["cuid"])
            filt_elem.set("operator", filt["operator"])  # e.g., "EqualTo", "InList"
            values = ET.SubElement(filt_elem, "values")
            value_list = filt["values"] if isinstance(filt["values"], list) else [filt["values"]]
            for value in value_list:
                ET.SubElement(values, "value").text = value
        else:
            print("Invalid filter format. Required keys: 'object_id', 'cuid', 'operator', 'values'")
            return None
    return ET.tostring(query, encoding='utf-8', method='xml').decode('utf-8')

def execute_query(token: str, universe_id: int, query_xml: str) -> Union[str, None]:
    """Execute a query against a Universe."""
    url = f"{BASE_URL}/raylight/v1/universes/{universe_id}/query"
    headers = get_headers(token)
    try:
        response = requests.post(url, headers=headers, data=query_xml.encode('utf-8'))
        response.raise_for_status()
        return response.text  # XML or JSON result set
    except RequestException as e:
        print(f"Error executing query: {e.response.text}")
        return None

def save_query_result(token: str, query_result: str, file_path: str) -> None:
    """Save query result as Excel."""
    try:
        root = ET.fromstring(query_result)
        rows = []
        for row in root.findall('.//row'):
            row_dict = {col.tag: col.text for col in row}
            rows.append(row_dict)
        df = pd.DataFrame(rows)
        df.to_excel(file_path, index=False)
        print(f"Query result saved at {file_path}")
    except Exception as e:
        print(f"Error saving query result: {e}")

def interact_with_universe(
    username: str, password: str,
    universe_id: int, selected_objects: List[Dict[str, str]],
    filters: List[Dict[str, str]], file_path: str,
    auth_type: str = 'secWinAD'
) -> bool:
    """Interact with a Universe: select objects, apply filters, and save results."""
    token = get_token(username, password, auth_type)
    if not token:
        return False

    query_xml = build_query_xml(universe_id, selected_objects, filters)
    if not query_xml:
        logoff(token)
        return False

    query_result = execute_query(token, universe_id, query_xml)
    if query_result:
        save_query_result(token, query_result, file_path)
        logoff(token)
        return True

    logoff(token)
    return False
