"""
Simple SAP BI Test Script

Direct implementation of SAP BI API calls to test connection and document download.
Based on the reference code in sap-document-service folder.
"""

import requests
import time
import json
import xml.etree.ElementTree as ET

# SAP BI Configuration
BASE_URL = "http://afbbilv.afrilandfirstbank.cm:8085/biprws"  # Production URL from reference

# Test Configuration - REPLACE WITH YOUR ACTUAL VALUES
USERNAME = ""  # Your SAP BI username
PASSWORD = ""  # Your SAP BI password
DOCUMENT_ID = 44777  # Document ID to test with
AUTH_TYPE = "secWinAD"  # Authentication type

def get_sap_token(username, password, auth_type="secWinAD"):
    """Get authentication token from SAP BI."""
    url = f"{BASE_URL}/logon/long"
    
    headers = {
        "Content-Type": "application/xml",
        "Accept": "application/xml"
    }
    
    # Authentication XML payload
    auth_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <attrs xmlns="http://www.sap.com/rws/bip">
        <attr name="userName" type="string">{username}</attr>
        <attr name="password" type="string">{password}</attr>
        <attr name="auth" type="string" possibilities="{auth_type}">{auth_type}</attr>
    </attrs>"""
    
    try:
        print(f"🔐 Requesting authentication token...")
        response = requests.post(url, headers=headers, data=auth_xml)
        response.raise_for_status()
        
        if response.status_code == 200:
            print("✅ Authentication successful!")
            # Extract token from response headers
            token = response.headers.get('X-SAP-LogonToken')
            if token:
                print(f"🎫 Token received: {token[:20]}...")
                return token
            else:
                print("❌ No token found in response headers")
                return None
        else:
            print(f"❌ Authentication failed: {response.status_code}")
            return None
            
    except requests.RequestException as e:
        print(f"❌ Authentication error: {str(e)}")
        return None

def schedule_document(token, document_id, field_list):
    """Schedule a document for generation with parameters."""
    url = f"{BASE_URL}/infostore/{document_id}/schedule"
    
    headers = {
        "X-SAP-LogonToken": token,
        "Content-Type": "application/xml",
        "Accept": "application/xml"
    }
    
    # Build schedule XML with parameters
    schedule_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <attrs xmlns="http://www.sap.com/rws/bip">
        <attr name="name" type="string">SARA_Test_Report</attr>
        <attr name="si_kind" type="string">CrystalReport</attr>
        <attr name="si_instance" type="boolean">true</attr>
        <attr name="si_format" type="string">xlsx</attr>
    """
    
    # Add date parameters
    for field in field_list:
        tech_name = field["technicalName"]
        value = field["values"]
        schedule_xml += f'<attr name="{tech_name}" type="string">{value}</attr>\n'
    
    schedule_xml += "</attrs>"
    
    try:
        print(f"📋 Scheduling document {document_id}...")
        response = requests.post(url, headers=headers, data=schedule_xml)
        response.raise_for_status()
        
        if response.status_code == 201:
            print("✅ Document scheduled successfully!")
            # Parse response to get instance ID
            root = ET.fromstring(response.text)
            for attr in root.findall('.//{http://www.sap.com/rws/bip}attr[@name="si_id"]'):
                instance_id = attr.text
                print(f"📄 Instance ID: {instance_id}")
                return instance_id
            
            print("❌ Could not find instance ID in response")
            return None
        else:
            print(f"❌ Document scheduling failed: {response.status_code}")
            return None
            
    except requests.RequestException as e:
        print(f"❌ Scheduling error: {str(e)}")
        return None

def check_schedule_status(token, instance_id, max_wait=60):
    """Check if document generation is complete."""
    url = f"{BASE_URL}/infostore/{instance_id}"
    
    headers = {
        "X-SAP-LogonToken": token,
        "Accept": "application/xml"
    }
    
    print(f"⏳ Waiting for document generation (max {max_wait}s)...")
    
    for attempt in range(max_wait):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            if response.status_code == 200:
                # Parse response to check status
                root = ET.fromstring(response.text)
                for attr in root.findall('.//{http://www.sap.com/rws/bip}attr[@name="si_instance_status"]'):
                    status = attr.text
                    if status == "success":
                        print("✅ Document generation completed!")
                        return True
                    elif status == "running":
                        print(f"⏳ Still generating... ({attempt + 1}s)")
                        time.sleep(1)
                        continue
                    else:
                        print(f"❌ Document generation failed with status: {status}")
                        return False
            
        except requests.RequestException as e:
            print(f"❌ Status check error: {str(e)}")
            return False
    
    print(f"❌ Timeout waiting for document generation")
    return False

def download_document(token, instance_id, output_file):
    """Download the generated document."""
    url = f"{BASE_URL}/infostore/folder/{instance_id}/file"
    
    headers = {
        "X-SAP-LogonToken": token
    }
    
    try:
        print(f"📥 Downloading document to {output_file}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        if response.status_code == 200:
            with open(output_file, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✅ Document downloaded successfully! ({file_size} bytes)")
            return True
        else:
            print(f"❌ Download failed: {response.status_code}")
            return False
            
    except requests.RequestException as e:
        print(f"❌ Download error: {str(e)}")
        return False

def cleanup_instance(token, instance_id):
    """Clean up the generated instance."""
    url = f"{BASE_URL}/infostore/{instance_id}"
    
    headers = {
        "X-SAP-LogonToken": token
    }
    
    try:
        response = requests.delete(url, headers=headers)
        if response.status_code == 200:
            print("🧹 Instance cleaned up")
        else:
            print(f"⚠️  Cleanup warning: {response.status_code}")
    except:
        print("⚠️  Could not clean up instance")

def logout(token):
    """Logout and invalidate token."""
    url = f"{BASE_URL}/logoff"
    
    headers = {
        "X-SAP-LogonToken": token
    }
    
    try:
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            print("👋 Logged out successfully")
        else:
            print(f"⚠️  Logout warning: {response.status_code}")
    except:
        print("⚠️  Could not logout properly")

def test_sap_connection():
    """Complete test of SAP BI connection and document download."""
    
    if not USERNAME or not PASSWORD:
        print("❌ Please set USERNAME and PASSWORD in the script")
        print("Also set the correct DOCUMENT_ID for your SAP BI setup")
        return False
    
    # Test parameters
    field_list = [
        {"technicalName": "psDate de Début", "values": "30/07/2025"},
        {"technicalName": "psDate de Fin", "values": "30/07/2025"}
    ]
    
    output_file = "test_sap_download.xlsx"
    
    print("🔧 Testing SAP BI connection...")
    print(f"Server: {BASE_URL}")
    print(f"Username: {USERNAME}")
    print(f"Document ID: {DOCUMENT_ID}")
    print(f"Output file: {output_file}")
    print()
    
    # Step 1: Get authentication token
    token = get_sap_token(USERNAME, PASSWORD, AUTH_TYPE)
    if not token:
        return False
    
    try:
        # Step 2: Schedule document
        instance_id = schedule_document(token, DOCUMENT_ID, field_list)
        if not instance_id:
            return False
        
        # Step 3: Wait for completion
        if not check_schedule_status(token, instance_id):
            return False
        
        # Step 4: Download document
        if not download_document(token, instance_id, output_file):
            return False
        
        # Step 5: Cleanup
        cleanup_instance(token, instance_id)
        
        print("\n🎉 SAP BI test completed successfully!")
        return True
        
    finally:
        # Step 6: Logout
        logout(token)

if __name__ == "__main__":
    print("🚀 SAP BI Simple Connection Test")
    print("=" * 50)
    print(f"📡 SAP BI Server: {BASE_URL}")
    print()
    
    success = test_sap_connection()
    
    if success:
        print("\n✅ SUCCESS! SAP BI connection test passed")
        print("You can now proceed with Agent and Customer document IDs")
    else:
        print("\n❌ FAILED! SAP BI connection test failed")
        print("\nTroubleshooting:")
        print("1. Check username and password")
        print("2. Verify document ID exists and you have access")
        print("3. Ensure network connectivity to SAP BI server")
        print("4. Check if the server URL is correct")
        print("5. Verify date format matches SAP BI expectations") 