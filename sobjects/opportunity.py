import time

import sfdc_clients
import utility

# RenewalCreditDateUpdate
# IF(CloseDate >= Effective_Date__c, CloseDate, Effective_Date__c)

# CommissionCreditUpdate
# CASE(Type,
# 'Up Sell (deferred)', Amount,
# 'Paid Trial', Amount,
# 'Renewal', IF(Contract_Length_Months__c < 24, 0, ASV__c),
# 'Renewal (full term)', IF(Contract_Length_Months__c < 24, 0, ASV__c),
# 'Renewal (contract/email required)', IF(Contract_Length_Months__c < 24, 0, ASV__c),
# 'New Business', IF(Contract_Length_Months__c < 12, Amount, ASV__c),
# 'Up Sell', IF(Contract_Length_Months__c < 12, Amount, ASV__c),
# 'Sales Credit Only', IF(Contract_Length_Months__c < 12, Amount, ASV__c),
# 0)

# QuotaCreditUpdate
# CASE(Type,
# 'New Business', IF(Pay_As_You_Go__c, Amount, ASV__c),
# 'Up Sell', IF(Pay_As_You_Go__c, Amount, ASV__c),
# 'Sales Credit Only', IF(Pay_As_You_Go__c, Amount, ASV__c),
# 'Increase', IF(Pay_As_You_Go__c, Amount, ASV__c),
# 0)

def dedupe_opp_contact_roles():
    soql = "SELECT Id, ContactId, Contact.Email FROM OpportunityContactRole WHERE OpportunityId = '0063g000005VSHt'"

    ocr_list = sfdc_clients.simple_client.execute_query(soql)['records']

    ocr_to_keep = {}
    ocr_for_delete = []

    print(f'Opportunity currently has {len(ocr_list)} Contact Role records')

    for ocr in ocr_list:
        id = ocr['Id']
        contact_id = ocr['ContactId']
        contact_email = ocr['Contact']['Email']

        if contact_id not in ocr_to_keep:
            ocr_to_keep[contact_id] = ocr
        else:
            payload = {
                "Id": ocr['Id']
            }
            ocr_for_delete.append(payload)
            print(f'Found duplicate OCR for Contact {contact_email}')


    print(f'Preparing to delete {len(ocr_for_delete)} Opportunity Contact Roles')

    #confirm = input('Proceed? y/n')

    #if confirm:
    sfdc_clients.simple_client.client.bulk.OpportunityContactRole.delete(ocr_for_delete)

    print('Done!')



def update_opp_amounts(update_renewal_credit: bool=False, update_comm_credit: bool=False,
                       update_quota_credit: bool=False):
    soql = "SELECT Id, Type, Amount, ASV__c, CloseDate, Pay_As_You_Go__c, Effective_Date__c, Contract_Length_Months__c" \
           " FROM Opportunity " \
           " WHERE Quota_Credit__c = NULL AND Confirm_Win__c = True " \
           " AND StageName IN ('Closed Won', 'Renewal - Won', 'Renewal - Lost') " \
           " AND (CloseDate >= 2020-01-01 OR Effective_Date__c >= 2020-01-01)"

    opps = sfdc_clients.simple_client.execute_query(soql)['records']

    opps_for_update = []

    for o in opps:
        payload = {
            "Id": o['Id']
        }


        opp_type = o['Type']
        close_date = utility.convert_sfdc_datetime_to_datetime(o['CloseDate'])
        eff_date = utility.convert_sfdc_datetime_to_datetime(o['Effective_Date__c'])
        amt = o['Amount']
        asv = o['ASV__c']
        paygo = o['Pay_As_You_Go__c']

        if update_renewal_credit:
            # RenewalCreditDateUpdate
            if close_date >= eff_date:
                payload['Renewal_Credit_Date__c'] = o['CloseDate']
            else:
                payload['Renewal_Credit_Date__c'] = o['Effective_Date__c']

        if update_comm_credit:
            # CommissionCreditUpdate
            if paygo:
                payload['Commission_Credit__c'] = asv
            elif opp_type in ['Up Sell (deferred)', 'Paid Trial']:
                payload['Commission_Credit__c'] = amt
            elif opp_type in ['Renewal', 'Renewal (full term)', 'Renewal (contract/email required)']:
                if o['Contract_Length_Months__c'] < 24:
                    payload['Commission_Credit__c'] = 0
                else:
                    payload['Commission_Credit__c'] = o['ASV__c']
            elif opp_type in ['New Business', 'Up Sell', 'Sales Credit Only', 'Increase']:
                if o['Contract_Length_Months__c'] < 12:
                    payload['Commission_Credit__c'] = amt
                else:
                    payload['Commission_Credit__c'] = asv

        if update_quota_credit:
            # QuotaCreditUpdate
            if opp_type in ['New Business', 'Up Sell', 'Sales Credit Only', 'Increase']:
                if paygo:
                    payload['Quota_Credit__c'] = amt
                else:
                    payload['Quota_Credit__c'] = asv

        opps_for_update.append(payload)

    print(f'Updating all opps({len(opps_for_update)})...')
    sfdc_clients.bulk_client.send_bulk_update('Opportunity', opps_for_update)

    print('Done!')



def revert_opp_amounts():
    soql = "SELECT Id, Amount, ASV__c FROM Opportunity " \
           "WHERE LastModifiedDate >= 2020-08-14T07:00:00-04:00 AND LastModifiedById = '0053g0000016qBw'"

    opps = sfdc_clients.simple_client.execute_query(soql)['records']

    opps_for_update = []

    for o in opps:
        if o['Amount'] == o['ASV__c'] + 0.01:
            payload = {
                "Id": o['Id'],
                "Amount": o['ASV__c']
            }

            opps_for_update.append(payload)

    sfdc_clients.bulk_client.send_bulk_update('Opportunity', opps_for_update)

    print('Done!')