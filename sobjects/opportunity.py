import time

import sfdc_clients

def update_opp_amounts():
    soql = "SELECT Id, Amount FROM Opportunity " \
           "WHERE Quota_Credit__c = NULL AND Confirm_Win__c = True " \
           "AND StageName IN ('Closed Won', 'Renewal - Won', 'Renewal - Lost') " \
           "AND (CloseDate >= 2020-01-01 OR Effective_Date__c >= 2020-01-01)"

    opps = sfdc_clients.simple_client.execute_query(soql)['records']

    opps_for_update = []

    for o in opps:
        payload = {
            "Id": o['Id'],
            "Amount": o['Amount'] + 0.01
        }

        opps_for_update.append(payload)

    print('Updating all opps (adding $0.01)...')
    job_id = sfdc_clients.bulk_client.send_bulk_update('Opportunity', opps_for_update)

    while not sfdc_clients.bulk_client.check_bulk_job_complete(job_id):
        time.sleep(5)

    print('Updating all opps (restoring original value)')
    opps_for_update.clear()

    for o in opps:
        payload = {
            "Id": o['Id'],
            "Amount": o['Amount']
        }

        opps_for_update.append(o)

    job_id = sfdc_clients.bulk_client.send_bulk_update('Opportunity', opps_for_update)

    while not sfdc_clients.bulk_client.check_bulk_job_complete(job_id):
        time.sleep(5)

    print('Done!')