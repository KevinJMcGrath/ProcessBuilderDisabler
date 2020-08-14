import sfdc_clients

def update_account_transcript_types():
    account_list = sfdc_clients.simple_client.get_accounts()

    acct_for_update = []
    for a in account_list:
        if len(a['ts_types']) > 1:
            ts_type = 'Mixed'
        elif len(a['ts_types']) == 1 and a['ts_types'][0] != 'N/A':
            ts_type = a['ts_types'][0]
        else:
            ts_type = ''

        a_u = {
            "Id": a['id'],
            "Transcript_Provider__c": ts_type
        }

        acct_for_update.append(a_u)

    sfdc_clients.bulk_client.send_bulk_update('Account', acct_for_update)