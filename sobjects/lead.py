import sfdc_clients

def match_all_leads():


    # soql = "SELECT AccountId, COUNT(Id)cnt_contact FROM Contact GROUP BY AccountId HAVING COUNT(Id) > 0"
    # soql = "SELECT Account.Name, COUNT(Id)contact_count FROM Contact GROUP BY Account.Name LIMIT 20"

    print('Loading Contacts...')
    soql = "SELECT Id, AccountId, Account.Paying_Users_all__c FROM Contact"
    contacts = sfdc_clients.simple_client.execute_query(soql)['records']

    print('Summarizing Contact Counts...')
    account_data = {}
    for c in contacts:
        aid = c['AccountId']

        paying_users = 0
        if aid not in account_data:
            if 'Account' in c and c['Account']:
                if 'Paying_Users_all__c' in c['Account'] and c['Account']['Paying_Users_all__c']:
                    paying_users = c['Account']['Paying_Users_all__c']

            account_data[aid] = {
                "account_id": aid,
                "paying_users": int(paying_users),
                "contact_count": 1
            }
        else:
            account_data[aid]['contact_count'] += 1

    print('Loading ALL...')
    soql = "SELECT Id, Domain__c, Account__c FROM Account_Lead_Link__c ORDER BY Domain__c"
    all = sfdc_clients.simple_client.execute_query(soql)['records']

    domain_map = {}
    for a in all:
        aid  = a['Account__c']
        domain = a['Domain__c']
        acc_data = account_data.get(aid, {"accound_id": aid, "paying_users": 0, "contact_count": 0})

        if domain not in domain_map:
            domain_map[domain] = acc_data
        else:
            domain_map[domain] = compare_acc_data(domain_map[domain], acc_data)


    print('Loading Leads...')
    soql = "SELECT Id, Email, bizible2__Account__c FROM Lead WHERE Account__c = NULL and Email != NULL"
    leads = sfdc_clients.simple_client.execute_query(soql)['records']

    matched_leads_for_update = []
    bizable_leads_for_update = []

    for l in leads:
        if 'bizible2__Account__c' in l and l['bizible2__Account__c']:
            l_u = {
                "Id": l['Id'],
                "Account__c": l['bizible2__Account__c']
            }
            bizable_leads_for_update.append(l_u)
            continue

        if '@' in l['Email']:
            email_domain = l['Email'].split('@')[1]
            acc_data = domain_map.get(email_domain)

            if acc_data:
                l_u = {
                    "Id": l['Id'],
                    "Account__c": acc_data['account_id']
                }

                matched_leads_for_update.append(l_u)

    sfdc_clients.bulk_client.send_bulk_update('Lead', bizable_leads_for_update)
    sfdc_clients.bulk_client.send_bulk_update('Lead', matched_leads_for_update)

def compare_acc_data(ad1, ad2):
    if ad1['paying_users'] > ad2['paying_users']:
        return ad1
    elif ad1['paying_users'] == ad2['paying_users']:
        if ad1['contact_count'] >= ad2['contact_count']:
            return ad1
        else:
            return ad2
    else:
        return ad2