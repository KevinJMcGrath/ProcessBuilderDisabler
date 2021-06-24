import sfdc_clients
import utility

def update_current_month():
    for_update = []
    unmatched = []

    soql_wd = "SELECT Id, Start_Credits_Current_Month__c FROM WSI_User_Detail__c " \
              "WHERE Active__c = true AND History_Count__c > 0"

    print('Loading WSI User Detail records...', end='')
    user_details = sfdc_clients.simple_client.execute_query(soql_wd)['records']
    print(f'Done! ({len(user_details)})')

    # Note: You can't use QueryMore for Aggregate SOQL queries. So I have to loop through to get them all.

    hist_may = []
    batch_index = 0

    while True:
        offset = batch_index * 1000
        soql_hist_may = "SELECT WSI_User_Detail__c, MIN(Credits_Consumed__c) FROM WSI_User_Detail_History__c " \
                        "WHERE CreatedDate >= 2021-06-01T00:00:01.000-0500 " \
                        f"GROUP BY WSI_User_Detail__c ORDER BY WSI_User_Detail__c LIMIT 1000 OFFSET {offset}"

        print(f'Loading WSI User Detail History records for May (batch {batch_index + 1})...')
        batch_results = sfdc_clients.simple_client.execute_query(soql_hist_may)['records']

        hist_may += batch_results

        batch_index += 1

        if len(batch_results) < 1000:
            break

    print(f'Done! ({len(hist_may)})')

    for wsi_ud in user_details:
        june_start = -1
        wsi_ud_id = wsi_ud['Id']
        wsi_ud_curr_mon_val = wsi_ud['Start_Credits_Current_Month__c']

        for hist in hist_may:
            if hist['WSI_User_Detail__c'] == wsi_ud_id:
                may_start = hist['expr0']
                break

        if june_start >= 0:
            if not wsi_ud_curr_mon_val or (wsi_ud_curr_mon_val > june_start):
                payload = {
                    'Id': wsi_ud_id,
                    'Start_Credits_Prior_Month__c': june_start
                }

                for_update.append(payload)
            else:
                print(
                    f'WSI UD (Id: {wsi_ud_id}) existing prior value is less than min value for May ({wsi_ud_curr_mon_val} vs {june_start})')
        else:
            unmatched.append(wsi_ud_id)

    print(f'WSI UD records for update: {len(for_update)}')
    print(f'WSI UD records without May history match: {len(unmatched)}')

    print('Updating WSI UD records...')
    sfdc_clients.bulk_client.send_bulk_update('WSI_User_Detail__c', for_update)
    print('Done!')

def populate_prior_month():
    for_update = []
    unmatched = []

    soql_wd = "SELECT Id, Start_Credits_Prior_Month__c, Start_Credits_Current_Month__c FROM WSI_User_Detail__c " \
              "WHERE Active__c = true AND History_Count__c > 0"

    print('Loading WSI User Detail records...', end='')
    user_details = sfdc_clients.simple_client.execute_query(soql_wd)['records']
    print(f'Done! ({len(user_details)})')

    # Note: You can't use QueryMore for Aggregate SOQL queries. So I have to loop through to get them all.

    hist_may = []
    batch_index = 0

    while True:
        offset = batch_index * 1000
        soql_hist_may = "SELECT WSI_User_Detail__c, MIN(Credits_Consumed__c) FROM WSI_User_Detail_History__c " \
                        "WHERE CreatedDate >= 2021-05-01T00:00:01.000Z AND " \
                        "CreatedDate < 2021-06-01T00:00:01.000Z " \
                        f"GROUP BY WSI_User_Detail__c ORDER BY WSI_User_Detail__c LIMIT 1000 OFFSET {offset}"

        print(f'Loading WSI User Detail History records for May (batch {batch_index + 1})...')
        batch_results = sfdc_clients.simple_client.execute_query(soql_hist_may)['records']

        hist_may += batch_results

        batch_index += 1

        if len(batch_results) < 1000:
            break


    print(f'Done! ({len(hist_may)})')

    for wsi_ud in user_details:
        may_start = -1
        wsi_ud_id = wsi_ud['Id']
        wsi_ud_prior_mon_val = wsi_ud['Start_Credits_Prior_Month__c']

        for hist in hist_may:
            if hist['WSI_User_Detail__c'] == wsi_ud_id:
                may_start = hist['expr0']
                break


        if may_start >= 0:
            if not wsi_ud_prior_mon_val or (wsi_ud_prior_mon_val > may_start):
                payload = {
                    'Id': wsi_ud_id,
                    'Start_Credits_Prior_Month__c': may_start
                }

                for_update.append(payload)
            else:
                print(f'WSI UD (Id: {wsi_ud_id}) existing prior value is less than min value for May ({wsi_ud_prior_mon_val} vs {may_start})')
        else:
            unmatched.append(wsi_ud_id)

    print(f'WSI UD records for update: {len(for_update)}')
    print(f'WSI UD records without May history match: {len(unmatched)}')

    print('Updating WSI UD records...')
    sfdc_clients.bulk_client.send_bulk_update('WSI_User_Detail__c', for_update)
    print('Done!')

def remove_duplicate_history_records():
    # select WSI_User_Detail__c, MIN(Credits_Consumed__c) from WSI_User_Detail_History__c
    # WHERE CreatedDate >= 2021-05-01T00:00:01.000Z AND CreatedDate < 2021-06-01T00:00:01.000Z
    # GROUP BY WSI_User_Detail__c LIMIT 20
    for_delete = []

    soql_count = "SELECT count(Id) FROM WSI_User_Detail__c WHERE History_Count__c > 0 AND " \
                 "WSI_Content_Pool__r.Type__c != 'Token' " \
                 "AND LastModifiedDate < 2021-06-06T07:00:00.000-0500"

    print('Loading WSI User Detail record count...', end='')
    user_details_count = sfdc_clients.simple_client.execute_query(soql_count)['records']
    record_count = user_details_count[0]['expr0']
    print(f'done! User Detail record count: {record_count}')

    soql_limit = 200
    offset = 0

    index = 0
    while offset + soql_limit < record_count:
        offset = soql_limit * index
        soql_wd = 'SELECT Id, (SELECT Id, WSI_User_Detail__c, Credits_Consumed__c, Pages_Consumed__c, CreatedDate ' \
                  'FROM WSI_User_Detail_Histories__r ORDER BY CreatedDate ASC) ' \
                  "FROM WSI_User_Detail__c WHERE History_Count__c > 0 AND WSI_Content_Pool__r.Type__c != 'Token' " \
                  "AND LastModifiedDate < 2021-06-06T07:00:00.000-0500 " \
                  f'ORDER BY Id LIMIT {soql_limit} OFFSET {offset}'

        print(f'Loading WSI User Detail records (batch {index + 1})...')
        user_details = sfdc_clients.simple_client.execute_query(soql_wd)['records']

        for ud in user_details:
            prior_ud_hist = None

            for_delete_temp = []
            for hist in ud['WSI_User_Detail_Histories__r']['records']:
                if not is_dupe_ud_hist(prior_ud_hist, hist):
                    prior_ud_hist = hist
                else:
                    for_delete_temp.append({ 'Id': hist['Id'] })

            print(f'Adding {len(for_delete_temp)} history records to delete queue for User Detail Id: {ud["Id"]}')
            for_delete += for_delete_temp

        index += 1

        # SOQL OFFSET limit is 2000
        if offset == 1800:
            print('Restaring OFFSET counter...')
            index = 0

            print(f'Deleting {len(for_delete)} duplicate history records...')
            sfdc_clients.simple_client.client.bulk.WSI_User_Detail_History__c.delete(for_delete)
            print('Done!')

            for_delete.clear()

    print(f'Deleting {len(for_delete)} duplicate history records...')
    sfdc_clients.simple_client.client.bulk.WSI_User_Detail_History__c.delete(for_delete)
    print('Done!')


def is_dupe_ud_hist(hist_prior, hist_current):
    if (hist_prior and
            hist_prior['Credits_Consumed__c'] == hist_current['Credits_Consumed__c'] and
            hist_prior['Pages_Consumed__c'] == hist_current['Pages_Consumed__c']):
        return True

    return False