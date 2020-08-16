import sfdc_clients

def update_channel_view_as():
    soql = "SELECT Id, Channel_View__c " \
           " FROM Contact " \
           " WHERE Status_of_User__c IN ('Active', 'Trial') AND Channel_View__c = False AND " \
           " (OneNote__c = 'On' OR Evernote__c = 'On' OR FileSync__c = True OR " \
           " Sharepoint__c = True OR Upload__c = True OR WebClipper__c = True)"

    contacts = sfdc_clients.simple_client.execute_query(soql)['records']

    contacts_for_update = []
    for c in contacts:
        payload = {
            "Id": c['Id'],
            "Channel_View__c": True
        }

        contacts_for_update.append(payload)

    sfdc_clients.bulk_client.send_bulk_update('Contact', contacts_for_update)

    print('Done')

def fix_channel_view_request():
    from pathlib import Path
    import jsonpickle
    filepath = ('./data/ChannelView_Request.json')

    with open(filepath, 'r') as json_file:
        s = json_file.readline()
        json_data = jsonpickle.decode(s)

    soql = "SELECT Id FROM Contact WHERE AccountId = '0013200001I670o'"
    contacts = sfdc_clients.simple_client.execute_query(soql)['records']

    contact_set = set()
    for c in contacts:
        contact_set.add(c['Id'])

    contacts_for_update = []
    for jc in json_data:
        c_id = jc['Id']

        if c_id in contact_set:
            payload = {
                "Id": c_id,
                "Channel_View__c": False
            }

            contacts_for_update.append(payload)

    sfdc_clients.bulk_client.send_bulk_update('Contact', contacts_for_update)

def update_contacts_pw_last_changed():
    contact_list = sfdc_clients.simple_client.get_contacts()

    contacts_for_update = []

    for c in contact_list:
        c_id = c['Id']
        pw_changed_date = c['CreatedDate']

        if c['Histories']:
            pw_changed_date = c['Histories']['records'][0]['CreatedDate']

        js = {
            'Id': c_id,
            'PW_Last_Changed__c': pw_changed_date
        }

        contacts_for_update.append(js)

    sfdc_clients.bulk_client.send_bulk_update('Contact', contacts_for_update)




def export_pw_last_changed():
    print('Executing Query...')
    soql = "SELECT Id, Username__c, CreatedDate, PW_Last_Changed__c FROM Contact " \
           "WHERE PW_Last_Changed__c != null AND LastModifiedDate = TODAY AND LastModifiedById = '0053g0000016qBw'"

    response = sfdc_clients.simple_client.execute_query(soql)

    import csv
    from datetime import datetime

    print('Saving data to CSV...')
    with open('./contact_export_2020-08-12.csv', 'w') as export_file:
        csv_headers = ['Id', 'Username__c', 'CreatedDate', 'PW_Last_Changed__c']

        writer = csv.DictWriter(export_file, fieldnames=csv_headers, extrasaction='ignore', lineterminator='\n')

        writer.writeheader()

        index = 1
        for row in response['records']:
            pw_date = datetime.fromisoformat(row['PW_Last_Changed__c'].split('.')[0])
            c_date = datetime.fromisoformat(row['CreatedDate'].split('.')[0])
            if pw_date.date() > c_date.date():
                print(f'Writing row {index}')
                writer.writerow(row, )
                index += 1

    print('Done!')