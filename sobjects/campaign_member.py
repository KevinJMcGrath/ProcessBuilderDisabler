import csv

import sfdc_clients


def delete_campaign_members_by_csv(path: str):
    cm_ids = {}

    with open(path, 'r', encoding='Latin1') as csv_file:
        csv_data = csv.reader(csv_file)

        count = 0
        for row in csv_data:
            id_val = row[0]

            if not id_val.startswith('00v'):
                continue

            key = id_val
            val = { 'Id': id_val }

            cm_ids[key] = val
            count +=1

            print(f'CM Rows for Delete: {count}', end='\r')

        print(f'CM Rows for Delete: {len(cm_ids)}')

    print('Deleting Campaign Members...')
    sfdc_clients.simple_client.client.bulk.CampaignMember.delete(list(cm_ids.values()))