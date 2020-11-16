import csv

import sfdc_clients

def fix_2020_opp_splits():
    # split type ids
    sc_asv_id = '1490d000000PB5UAAW'
    quota_id = '1493g000000Gma9AAC'
    comm_id = '1493g000000GmaAAAS'

    # soql = "SELECT Id, OpportunityId, SplitPercentage, SplitOwnerId, SplitTypeId, " \
    #        "Opportunity.Quota_Credit__c, Opportunity.Commission_Credit__c " \
    #        "FROM OpportunitySplit " \
    #         " WHERE OpportunityId = '0060d00001uIgZK'"
           # "WHERE Opportunity.CloseDate >= THIS_YEAR AND Opportunity.IsWon = True " \
           #  "AND SplitTypeId IN ('1490d000000PB5UAAW', '1493g000000Gma9AAC', '1493g000000GmaAAAS')"

    soql = "SELECT Id, Quota_Credit__c, Commission_Credit__c, "
    soql += "(SELECT Id, OpportunityId, SplitPercentage, SplitOwnerId, SplitTypeId "
    soql += " FROM OpportunitySplits) FROM Opportunity"
    soql += " WHERE CloseDate >= THIS_YEAR AND IsWon = True"

    # soql += " AND Id ='0060d00001uIgZK'"

    print('Downloading existing splits from Salesforce...')
    opps_with_splits_res = sfdc_clients.simple_client.execute_query(soql)['records']

    splits_for_insert = []
    splits_for_update = []

    for opp in opps_with_splits_res:
        split_list = opp['OpportunitySplits']['records']
        asv_splits = [s for s in split_list if s['SplitTypeId'] == sc_asv_id]
        quota_splits = {s['SplitOwnerId']: s for s in split_list if s['SplitTypeId'] == quota_id}
        comm_splits = {s['SplitOwnerId']: s for s in split_list if s['SplitTypeId'] == comm_id}

        for asv_split in asv_splits:
            asv_owner_id = asv_split['SplitOwnerId']

            if asv_owner_id not in quota_splits:
                quota_split = {
                    "OpportunityId": asv_split['OpportunityId'],
                    "SplitTypeId": quota_id,
                    "SplitOwnerId": asv_split['SplitOwnerId'],
                    "SplitPercentage": asv_split['SplitPercentage']
                }

                splits_for_insert.append(quota_split)
            else:
                quota_split = quota_splits.get(asv_owner_id)
                if asv_split['SplitPercentage'] != quota_split['SplitPercentage']:
                    quota_split_u = {
                        "Id": quota_split['Id'],
                        "SplitPercentage": asv_split['SplitPercentage']
                    }
                    splits_for_update.append(quota_split_u)

            if asv_owner_id not in comm_splits:
                comm_split = {
                    "OpportunityId": asv_split['OpportunityId'],
                    "SplitTypeId": comm_id,
                    "SplitOwnerId": asv_split['SplitOwnerId'],
                    "SplitPercentage": asv_split['SplitPercentage']
                }

                splits_for_insert.append(comm_split)
            else:
                comm_split = comm_splits.get(asv_owner_id)
                if asv_split['SplitPercentage'] != comm_split['SplitPercentage']:
                    comm_split_u = {
                        "Id": comm_split['Id'],
                        "SplitPercentage": asv_split['SplitPercentage']
                    }

                    splits_for_update.append(comm_split_u)

    # sfdc_clients.simple_client.client.bulk.OpportunitySplit.insert(splits_for_insert)

    print(f"Splits for Insert: {len(splits_for_insert)}")
    print(f"Splits for Update: {len(splits_for_update)}")

    print('Saving split_insert data to CSV...')
    with open('./export/opp_split_insert_export_2020-08-30.csv', 'w') as export_file:
        csv_headers = ['OpportunityId', 'SplitTypeId', 'SplitOwnerId', 'SplitPercentage']

        writer = csv.DictWriter(export_file, fieldnames=csv_headers, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        writer.writerows(splits_for_insert)

    print('Saving split_update data to CSV...')
    with open('./export/opp_split_update_export_2020-08-30.csv', 'w') as export_file:
        csv_headers = ['Id', 'SplitPercentage']

        writer = csv.DictWriter(export_file, fieldnames=csv_headers, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        writer.writerows(splits_for_update)


    sfdc_clients.bulk_client.send_bulk_insert('OpportunitySplit', splits_for_insert)
