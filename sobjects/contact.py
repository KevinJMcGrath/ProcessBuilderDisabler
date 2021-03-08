import csv

import sfdc_clients

from pathlib import Path

def update_contact_fix_BQ():
    payload = {
          "Id": "0030d00002EN0JXAA1",
          "Apex_Bypass_Toggle__c": False,
          "AS_BQ_Last_Updated__c": "2021-03-08T08:14:15Z",
          "AS_Active_Days_7d__c": 0,
          "AS_Active_Days_28d__c": 0,
          "AS_Active_Days_90d__c": 0,
          "AS_Documents_Read__c": 0,
          "AS_Documents_Read_7d__c": None,
          "AS_Documents_Read_28d__c": None,
          "AS_Watchlists_Created__c": 0,
          "AS_Watchlists_Deleted__c": 0,
          "AS_Watchlists_Modified__c": 0,
          "AS_Watchlist_Searches_7d__c": 0,
          "AS_Watchlist_Searches_28d__c": 0,
          "AS_Net_Watchlists_Created__c": 0,
          "AS_Alerts_Created__c": 0,
          "AS_Alerts_Deleted__c": 0,
          "AS_Active_Alerts_Subscribed__c": 0,
          "AS_Alerts_Subscribed_7d__c": 0,
          "AS_Alerts_Subscribed_28d__c": 0,
          "AS_Table_Export__c": 0,
          "AS_Table_Export_Any_Event__c": 0,
          "FactSet_AMR_Doc_Read_2d__c": 0,
          "FactSet_AMR_Doc_Read_7d__c": 0,
          "FactSet_AMR_Doc_Read_30d__c": 0,
          "FactSet_AMR_Doc_Read_90d__c": 0,
          "FactSet_AMR_Doc_Read_365d__c": 0,
          "Factset_AMR_Doc_Purchase_2d__c": 0,
          "Factset_AMR_Doc_Purchase_7d__c": 0,
          "Factset_AMR_Doc_Purchase_30d__c": 0,
          "Factset_AMR_Doc_Purchase_90d__c": 0,
          "Factset_AMR_Doc_Purchase_365d__c": 0,
          "FactSet_AMR_Pages_Read_2d__c": 0,
          "FactSet_AMR_Pages_Read_7d__c": 0,
          "FactSet_AMR_Pages_Read_30d__c": 0,
          "FactSet_AMR_Pages_Read_90d__c": 0,
          "FactSet_AMR_Pages_Read_365d__c": 0,
          "Factset_AMR_Page_Purchase_2d__c": 0,
          "Factset_AMR_Page_Purchase_7d__c": 0,
          "Factset_AMR_Page_Purchase_30d__c": 0,
          "Factset_AMR_Page_Purchase_90d__c": 0,
          "Factset_AMR_Page_Purchase_365d__c": 0,
          "FactSet_AMR_Pages_Consumed_2d__c": 0,
          "FactSet_AMR_Pages_Consumed_7d__c": 0,
          "FactSet_AMR_Pages_Consumed_30d__c": 0,
          "FactSet_AMR_Pages_Consumed_90d__c": 0,
          "FactSet_AMR_Pages_Consumed_365d__c": 0,
          "Direct_Broker_Pages_Read_2d__c": 0,
          "Direct_Broker_Pages_Read_7d__c": 0,
          "Direct_Broker_Pages_Read_30d__c": 0,
          "Direct_Broker_Pages_Read_90d__c": 0,
          "Direct_Broker_Pages_Read_365d__c": 0,
          "Direct_Broker_Page_Purchase_2d__c": 0,
          "Direct_Broker_Page_Purchase_7d__c": 0,
          "Direct_Broker_Page_Purchase_30d__c": 0,
          "Direct_Broker_Page_Purchase_90d__c": 0,
          "Direct_Broker_Page_Purchase_365d__c": 0,
          "Direct_Broker_Pages_Consumed_2d__c": 0,
          "Direct_Broker_Pages_Consumed_7d__c": 0,
          "Direct_Broker_Pages_Consumed_30d__c": 0,
          "Direct_Broker_Pages_Consumed_90d__c": 0,
          "Direct_Broker_Pages_Consumed_365d__c": 0,
          "AS_Total_iPhone_Sessions__c": 0,
          "AS_iPhone_App_Sessions_7d__c": 0,
          "AS_iPhone_App_Sessions_28d__c": 0,
          "AS_Last_Activity__c": "2021-03-08T08:14:15Z",
          "Last_Login__c": "2018-02-22"
        }

    payload_list = [payload]

    sfdc_clients.bulk_client.send_bulk_update('Contact', payload_list)

def update_contact_roles_e_y():
    csv_path = Path('C:\\Users\\Kevin\\Downloads\\Parthenon - convert active users.csv')

    opp_id = '0063g000005VSHt'
    ocr_role = 'Paying User'

    ocr_for_insert = []
    ocr_for_update = []

    contact_ids = []
    with open(csv_path, 'r') as csv_file:
        reader = csv.reader(csv_file)

        for row in reader:
            contact_ids.append(row[1])

    soql = f"SELECT Id, ContactId, Role FROM OpportunityContactRole WHERE OpportunityId = '{opp_id}'"

    ocr_list = sfdc_clients.simple_client.execute_query(soql)['records']

    ocr_existing_map = {o['ContactId']: (o['Id'], o['Role'])  for o in ocr_list}

    for key, value in ocr_existing_map.items():
        ocr_id = value[0]
        payload = {
            "Id": ocr_id,
            "Role": ocr_role
        }

        ocr_for_update.append(payload)

    for contact_id in contact_ids:
        if contact_id in ocr_existing_map:
            # if the OCRole is already on the Opp, just skip
            continue
        else:
            payload = {
                "ContactId": contact_id,
                "OpportunityId": opp_id,
                "Role": ocr_role
            }

            ocr_for_insert.append(payload)

    # sfdc_clients.bulk_client.send_bulk_update('OpportunityContactRole', ocr_for_update)

    sfdc_clients.bulk_client.send_bulk_insert('OpportunityContactRole', ocr_for_insert)





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


def update_contacts_usage_limits():
    contact_list = sfdc_clients.simple_client.get_contacts()

    contacts_for_update = []

    for c in contact_list:
        payload = {
            "Id": c['Id'],
            "Annual_AMR_Page_Limit__c": 5000
        }

        contacts_for_update.append(payload)

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

def add_links_all_contacts():
    print('Loading all contacts...')
    contact_list = sfdc_clients.simple_client.get_contacts()

    print(f"Loaded {len(contact_list)} Contacts")
    acct_cnt_dict = {}

    print(f"Loading existing Account-Lead-Link records...")
    soql = "SELECT Id, Domain__c, Account__c FROM Account_Lead_Link__c"

    response = sfdc_clients.simple_client.execute_query(soql)
    existing_all_account_ids = { r['Account__c'] for r in response['records'] }

    skipped_contact = 0
    print('Extracting unique domains...')
    for c in contact_list:
        acct_id = c['AccountId']

        if not acct_id:
            continue

        if acct_id in existing_all_account_ids:
            skipped_contact += 1
            continue

        email: str = c['Email']
        domain = email.split('@')[1]

        if acct_id in acct_cnt_dict:
            cnt_domains: set = acct_cnt_dict.get(acct_id)
            cnt_domains.add(domain)
        else:
            acct_cnt_dict[acct_id] = {domain}  # set literal

    print('Creating Account-Lead-Links...')
    all_for_insert = []
    for acct_id, domain_list in acct_cnt_dict.items():
        for domain in domain_list:
            all = {
                "Account__c": acct_id,
                "Domain__c": domain
            }

            all_for_insert.append(all)

    print(f"Skipped {skipped_contact} Contacts belonging to Accounts with existing ALL records")

    print(f'Inserting {len(all_for_insert)} Account_Lead_Link__c records...')

    sfdc_clients.bulk_client.send_bulk_insert('Account_Lead_Link__c', all_for_insert)

    print('Done!')