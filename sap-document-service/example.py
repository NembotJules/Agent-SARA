from PySap import schedule_and_get_file

USERNAME = ""
PASSWORD = "<PASSWORD>"
FILE_PATH = "example_doc.xlsx"

document_id = 44777
# field_list = [
#     {"id": '0', "dpId": "DP2", "technicalName": "psDate de Début", "values": "04/10/2024"},
#     {"id": '1', "dpId": "DP2", "technicalName": "psDate de Fin", "values": "07/10/2024"},
#     {"id": '2', "dpId": "DP2", "technicalName": "pmCode Agence", "values": ["00031", "00088"]},
#     {"id": '10', "dpId": "DP2", "technicalName": "pmMatricule client", "values": ["0939973", "0956031", "0941889", "0955099"]},
# ]
field_list = [
    {"id": '0', "dpId": "DP2", "technicalName": "psDate de Début", "values": "10/10/2024"},
    {"id": '1', "dpId": "DP2", "technicalName": "psDate de Fin", "values": "11/10/2024"},
    {"id": '9', "dpId": "DP2", "technicalName": "pmN° de compte", "values": "38110090601"},
]

schedule_and_get_file(
    USERNAME, PASSWORD,
    FILE_PATH, document_id, field_list,
)
