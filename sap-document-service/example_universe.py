
"""Example script to interact with SAP BusinessObjects Universes"""
import os
from PySap_univers import get_token, get_universe_list, get_universe_details, interact_with_universe, logoff

# Configuration
USERNAME = os.getenv("SAP_USERNAME", "AFRILAND\\jdoe")  # Replace with your AD username
PASSWORD = os.getenv("SAP_PASSWORD", "your_password")    # Replace with your AD password
FILE_PATH = "universe_query_result.xlsx"
UNIVERSE_ID = 12345  # Replace with actual Universe ID
    
# Step 1: List Universes to find a Universe ID
token = get_token(USERNAME, PASSWORD, auth_type="secWinAD")
if token:
    df_universes = get_universe_list(token)
    if df_universes is not None:
        print("Available Universes:")
        print(df_universes)
    logoff(token)
else:
    print("Authentication failed")

# Step 2: Browse Universe folders and objects to find object IDs and CUIDs
token = get_token(USERNAME, PASSWORD, auth_type="secWinAD")
if token:
    details = get_universe_details(token, UNIVERSE_ID)
    if details:
        print(f"Universe {UNIVERSE_ID} Structure:")
        for folder in details['folders']:
            print(f"Folder: {folder['name']} (ID: {folder['id']})")
            for obj in folder['objects']:
                print(f"  Object: {obj['name']} (Type: {obj['type']}, ID: {obj['id']}, CUID: {obj['cuid']})")
    logoff(token)
else:
    print("Authentication failed")

# Step 3: Query the Universe with selected objects and filters
selected_objects = [
    {"id": "1", "cuid": "ABC123"},  # Replace with actual object ID and CUID
    {"id": "2", "cuid": "DEF456"}   # Replace with actual object ID and CUID
]
filters = [
    {
        "object_id": "1",
        "cuid": "ABC123",           # Replace with actual CUID
        "operator": "EqualTo",
        "values": "38110090601"     # Filter for account number
    }
]
result = interact_with_universe(
    USERNAME, PASSWORD,
    UNIVERSE_ID, selected_objects, filters,
    FILE_PATH, auth_type="secWinAD"
)
print(f"Query execution {'successful' if result else 'failed'}")
