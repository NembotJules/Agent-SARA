import requests
import unittest
from unittest.mock import patch, MagicMock
from PySap import (
    get_headers, get_document_list, get_document_details, get_token,
    build_schedule_xml, schedule_document, download_file, logoff,
    check_schedule_state, schedule_and_get_file
)


class TestPySap(unittest.TestCase):

    token = "dummy_token"
    document_id = 12345
    username = "test_user"
    password = "test_password"
    auth_type = "secEnterprise"
    file_path = "/path/to/file.xlsx"
    field_list = [
        {"id": "1", "dpId": "123", "technicalName": "TestParam", "values": ["val1", "val2"]}
    ]



    # Test for get_headers
    def test_get_headers(self):
        expected_headers = {'X-SAP-LogonToken': self.token}
        self.assertEqual(get_headers(self.token), expected_headers)



    # Test for get_document_list
    @patch('requests.get')
    def test_get_document_list_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        response = get_document_list(self.token)
        self.assertEqual(response.status_code, 200)



    @patch('requests.get')
    def test_get_document_list_failure(self, mock_get):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_get.side_effect = mock_exception
        response = get_document_list(self.token)
        self.assertIsNone(response)



    # Test for get_document_details
    @patch('requests.get')
    def test_get_document_details_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<details>Test</details>"
        mock_get.return_value = mock_response
        response = get_document_details(self.token, self.document_id)
        self.assertEqual(response, "<details>Test</details>")



    @patch('requests.get')
    def test_get_document_details_failure(self, mock_get):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_get.side_effect = mock_exception
        response = get_document_details(self.token, self.document_id)
        self.assertIsNone(response)



    # Test for get_token
    @patch('requests.post')
    def test_get_token_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {'X-SAP-LogonToken': '"sample_token"'}
        mock_post.return_value = mock_response
        token = get_token(self.username, self.password, self.auth_type)
        self.assertEqual(token, 'sample_token')



    @patch('requests.post')
    def test_get_token_failure(self, mock_post):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_post.side_effect = mock_exception
        token = get_token(self.username, self.password)
        self.assertIsNone(token)



    # Test for build_schedule_xml
    def test_build_schedule_xml_success(self):
        expected_xml = '''<schedule><name>TestDoc</name><format type="xls" /><parameters><parameter dpId="123"><id>1</id><technicalName>TestParam</technicalName><answer><values><value>val1</value><value>val2</value></values></answer></parameter></parameters></schedule>'''
        result_xml = build_schedule_xml("TestDoc", self.field_list)
        self.assertEqual(result_xml, expected_xml)



    def test_build_schedule_xml_failure(self):
        # Invalid field_list missing necessary keys
        invalid_field_list = [{"id": "1", "dpId": "123", "values": ["val1"]}]
        result_xml = build_schedule_xml("TestDoc", invalid_field_list)
        self.assertIsNone(result_xml)



    # Test for schedule_document
    @patch('requests.post')
    def test_schedule_document_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<schedule><id>12345</id></schedule>'
        mock_post.return_value = mock_response
        instance_id = schedule_document(self.token, self.document_id, "<xml></xml>")
        self.assertEqual(instance_id, "12345")



    @patch('requests.post')
    def test_schedule_document_failure(self, mock_post):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_post.side_effect = mock_exception
        instance_id = schedule_document(self.token, self.document_id, "<xml></xml>")
        self.assertIsNone(instance_id)



    # Test for download_file
    @patch('requests.get')
    def test_download_file_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"test content"
        mock_get.return_value = mock_response
        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            download_file(self.token, "12345", self.file_path)
            mock_file.assert_called_once_with(self.file_path, 'wb')



    @patch('requests.get')
    def test_download_file_failure(self, mock_get):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_get.side_effect = mock_exception
        result = download_file(self.token, "12345", self.file_path)
        self.assertIsNone(result)



    # Test for logoff
    @patch('requests.post')
    def test_logoff_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        response = logoff(self.token)
        self.assertEqual(response, mock_response.text)



    @patch('requests.post')
    def test_logoff_failure(self, mock_post):
        mock_response = MagicMock()
        mock_exception = requests.exceptions.RequestException()
        mock_exception.response = mock_response
        mock_post.side_effect = mock_exception
        response = logoff(self.token)
        self.assertIsNone(response)



    @patch('PySap.get_token')
    @patch('PySap.schedule_document')
    @patch('PySap.check_schedule_state')
    @patch('PySap.download_file')
    @patch('PySap.logoff')
    def test_schedule_and_get_file_success(self, mock_logoff, mock_download, mock_check, mock_schedule, mock_get_token):
        mock_get_token.return_value = self.token
        mock_schedule.return_value = "instance_id"
        mock_check.return_value = True

        result = schedule_and_get_file(
            self.username, self.password, self.file_path,
            self.document_id, self.field_list, self.auth_type
        )
        self.assertTrue(result)



    @patch('PySap.get_token')
    def test_schedule_and_get_file_failure(self, mock_get_token):
        mock_get_token.return_value = None
        result = schedule_and_get_file(
            self.username, self.password, self.file_path,
            self.document_id, self.field_list, self.auth_type
        )
        self.assertFalse(result)



if __name__ == '__main__':
    unittest.main()
