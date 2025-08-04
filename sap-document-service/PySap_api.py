"""Service qui planifie et télécharge le document"""
import time
import requests
from fastapi import FastAPI
import xml.etree.ElementTree as ET
from typing import List, Dict, Union
from requests import RequestException
from Helpers.Constants import BASE_URL_SAP as BASE_URL

app = FastAPI()

@app.get("/get_headers")
def get_headers(token: str) -> Dict[str, str]:
    """Generate headers with SAP token.
    :param token: SAP token"""

    return {
        'X-SAP-LogonToken': token,
        'Accept-Language': 'fr'
    }



def get_document_list(token: str) -> any:
    """Retrieve SAP document list.
    :param token: SAP token"""

    url = f"{BASE_URL}/v1/documents/"
    headers = get_headers(token)

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving document list: {e.response.text}")
        return None



def get_document_details(token: str, document_id: int) -> Union[str, None]:
    """Retrieve SAP document details.
    :param token: SAP token
    :param document_id: SAP document id"""

    url = f"{BASE_URL}/raylight/v1/documents/{document_id}/parameters"
    headers = get_headers(token)

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving document details for {document_id}: {e.response.text}")
        return None



def get_token(username: str, password: str, auth_type: str = "secEnterprise") -> Union[str, None]:
    """Retrieve SAP connection token.
    :param username: SAP username
    :param password: SAP password
    :param auth_type: SAP auth type. Defaults to secEnterprise'
    :return: SAP connection token"""

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
        response.raise_for_status()  # Raise an error for bad status codes
        token = response.headers.get('X-SAP-LogonToken', None)
        if token:
            return token[1:-1]  # Returned value with quotes removed
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving token: {e.response.text}")
        return None



def build_schedule_xml(doc_name: str, field_list: List[Dict[str, str]]) -> Union[str, None]:
    """Format the SAP document update value

    :param doc_name: The name of the document
    :param field_list: A list of dictionaries containing the fields to format. Example: build_schedule_xml("MyDoc.xlsx", [{"id": "1", "dpId": "123", "technicalName": "TechName", "values": ["val1", "val2"]}])
    :return: The updated values in XML format"""

    # Créer la racine de l'élément XML
    schedule = ET.Element("schedule")

    # Ajouter l'élément 'name'
    name = ET.SubElement(schedule, "name")
    name.text = doc_name

    # Ajouter le format du document
    ET.SubElement(schedule, "format", {"type": "xls"})

    # Ajouter les paramètres
    parameters = ET.SubElement(schedule, "parameters")

    # Boucler sur chaque champ dans field_list
    for field in field_list:
        if {'id', 'dpId', 'technicalName', 'values'}.issubset(field):
            param = ET.SubElement(parameters, "parameter", {"dpId": field["dpId"]})
            ET.SubElement(param, "id").text = field["id"]
            ET.SubElement(param, "technicalName").text = field["technicalName"]

            # Ajouter les réponses (valeurs)
            answer = ET.SubElement(param, "answer")
            values = ET.SubElement(answer, "values")
            value_list = field["values"] if isinstance(field["values"], list) else [field["values"]]
            for value in value_list:
                ET.SubElement(values, 'value').text = value
        else:
            print("One or more fields were not found. Required fields: 'id', 'dpId', 'technicalName', 'values'")
            return None

        # Sérialiser l'élément XML en chaîne de caractères
    xml_string = ET.tostring(schedule, encoding='utf-8', method='xml').decode('utf-8')

    # Retourner le XML au format string
    return xml_string



def schedule_document(token: str, document_id: int, schedule_xml: str) -> Union[str, None]:
    """Schedule a report from SAP document
    :param token: The SAP token
    :param document_id: The SAP document ID
    :param schedule_xml: The SAP document update value"""

    url = f"{BASE_URL}/raylight/v1/documents/{document_id}/schedules"
    headers = {
        **get_headers(token),
        "Content-Type": "application/xml",
        "Accept": "application/xml",
    }

    try:
        response = requests.post(url, headers=headers, data=schedule_xml.encode('utf-8'))
        response.raise_for_status()
        root = ET.fromstring(response.text)
        instance_id = root.find('.//id').text
        print(f"Schedule successful, Instance ID: {instance_id}")
        return instance_id
    except requests.exceptions.RequestException as e:
        print(f"Error scheduling document: {e.response.text}")
        return None



def delete_file(token: str, document_id: str, instance_id: str) -> None:
    """Download the scheduling file from SAP.
    :param: token: The SAP token
    :param: document_id: The SAP document ID
    :param: instance_id: The id of the SAP schedule instance
    :param: file_path: The path to the file"""

    url = f"{BASE_URL}/raylight/v1/documents/{document_id}/schedules/{instance_id}"
    headers = {**get_headers(token), "Content-Type": "application/xml"}

    try:
        response = requests.delete(url, headers=headers, data="")
        response.raise_for_status()
        if response.status_code == 200:
            print(f"The document instance has been deleted from SAP")
        else:
            print(f"Failed to delete document. Status code: {response.status_code}")
    except RequestException as e:
        print(f"Error deleting document: {e.response.text}")



def download_file(token: str, instance_id: str, file_path: str) -> None:
    """Download the scheduling file from SAP.
    :param: token: The SAP token
    :param: instance_id: The id of the SAP schedule instance
    :param: file_path: The path to the file"""

    url = f"{BASE_URL}/infostore/folder/{instance_id}/file"

    headers = {**get_headers(token), "Content-Type": "application/xml"}

    try:
        response = requests.get(url, headers=headers, data="")
        response.raise_for_status()
        if response.status_code == 200:
            with open(f"{file_path}", 'wb') as file:
                file.write(response.content)
            print(f"Document saved at {file_path}")
        else:
            print(f"Failed to retrieve document. Status code: {response.status_code}")
    except RequestException as e:
        print(f"Error retrieving document: {e.response.text}")



def logoff(token: str) -> Union[str, None]:
    """Retrieve SAP connection token.
    :param token: The SAP token"""

    url = f"{BASE_URL}/logoff"
    headers = get_headers(token)

    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error logoff : {e.response.text}")
        return None



def check_schedule_state(token: str, instance_id: str, document_id: int) -> bool:
    """Check the state of the SAP instance.
    :param token: The SAP token
    :param instance_id: The id of the SAP schedule instance
    :param document_id: The SAP document ID
    :return: State of the SAP instance"""

    url = f"{BASE_URL}/raylight/v1/documents/{document_id}/schedules/{instance_id}"
    headers = {
        **get_headers(token),
        "Content-Type": "application/xml",
        "Accept": "application/json",
    }
    # Failed, Running, Pending, Completed
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        status = response.json()["schedule"]["status"]["@id"]
        print(response.json()["schedule"]["status"]["$"])
        while status in [0, 9]:  # status in ["Running", "Pending"]
            time.sleep(5)
            status = check_schedule_state(token, instance_id, document_id)
        if status == 1:  # status == "Completed"
            return status
        print(f"Error while checking schedule: {response.json()['schedule']['error']['message']}")
        return status
    except requests.exceptions.RequestException as e:
        print(f"Error checking schedule : {e.response.text}")
        return False


@app.get("/get_file")
def schedule_and_get_file(
        username: str, password: str,
        file_path: str, document_id: int,
        field_list: List[Dict[str, str]],
        auth_type: str = 'secWinAD'
) -> bool:
    """Schedule a report from SAP document and download the file.

    :param username: SAP username
    :param password: SAP password
    :param file_path: Path to save the document
    :param document_id: ID of the document to retrieve
    :param field_list: A list of dictionaries containing the fields to format
    :param auth_type: Type of authentication (default: secWinAD)
    :return: True if file was downloaded, False otherwise"""

    return_value = False

    # 1. Authentification
    token = get_token(username, password, auth_type)

    if token:
        # 2. Planifie le rapport
        doc_name = file_path.split("/")[-1].split(".")[0]
        schedule_xml = build_schedule_xml(doc_name, field_list)

        if schedule_xml:
            instance_id = schedule_document(token, document_id, schedule_xml)

            if instance_id:
                allOk = check_schedule_state(token, instance_id, document_id)

                if allOk:
                    # 3. Télécharge l'instance
                    download_file(token, instance_id, file_path)
                    # time.sleep(30)
                    return_value = True

                # 4. Supprime l'instance
                delete_file(token, instance_id)

        # 5. Se déconnecte
        logoff(token)

    return return_value
