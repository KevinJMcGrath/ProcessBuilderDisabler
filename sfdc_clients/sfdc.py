import jsonpickle
import urllib.parse

from simple_salesforce import Salesforce

import config

class SFClient:
    def __init__(self, username: str, password: str, sec_token: str):
        self.client = Salesforce(username=username, password=password, security_token=sec_token)
        self.batch_size_limit = 2000

    @staticmethod
    def from_config():
        return SFClient(config.Salesforce.username, config.Salesforce.password, config.Salesforce.security_token)

    def execute_query(self, soql: str):
        return self.client.query_all(soql)

    def get_accounts(self, limit: int=0):
        import itertools

        soql = "SELECT AccountId, Transcripts_Type__c, count(id) FROM Contact " \
               "WHERE Status_of_User__c IN ('Active', 'Trial') GROUP BY AccountId, Transcripts_Type__c"

        print('Downloading Contact Agg from Salesforce...', end='')
        response = self.client.query_all(soql)
        print('Done!')
        contact_agg = response['records']

        soql = "SELECT Id, Name, Transcript_Provider__c FROM Account " \
               "WHERE Customer_Status__c IN ('Client', 'Broker Client', 'Paid Trial')"

        if limit > 0:
            soql += f" LIMIT {limit}"

        print('Downloading Accounts from Salesforce...', end='')
        response = self.client.query_all(soql)
        print('Done!')
        accounts = response['records']

        accts = []
        for a in accounts:
            ac = {
                "id": a['Id'],
                "name": a['Name'],
                "ts_prov": a['Transcript_Provider__c'],
                "ts_types": []
            }

            # c_group = itertools.takewhile(lambda c: c['AccountId'] == ac['id'], contact_agg)
            c_group = [c for c in contact_agg if c['AccountId'] == ac['id']]
            for c_agg in c_group:
                if c_agg['Transcripts_Type__c'] not in ac['ts_types']:
                    ac['ts_types'].append(c_agg['Transcripts_Type__c'])

            accts.append(ac)

        return accts

    def get_contacts(self, limit: int=0):
        soql = "SELECT Id, CreatedDate, PW_Last_Changed__c, (SELECT CreatedDate FROM Histories WHERE Field = 'Password__c' " \
               "ORDER BY CreatedDate DESC LIMIT 1) FROM Contact WHERE Password__c != null " \
               "AND PW_Last_Changed__c = null AND Status_of_User__c IN ('Active', 'Trial')"

        if limit > 0:
            soql += f" LIMIT {limit}"

        print('Downloading Contacts from Salesforce...', end='')
        response = self.client.query_all(soql)
        print('done!')

        return [u for u in response['records']]

    def get_all_pb_processes(self):
        query = 'Select Id,ActiveVersion.VersionNumber,LatestVersion.VersionNumber,DeveloperName From FlowDefinition'
        response = self.query_tooling_api(query)

        return {pb['Id']: pb for pb in response['records']}


    def query_tooling_api(self, query):
        cleaned_query = urllib.parse.quote_plus(query)
        data = self.client.restful(path=f'tooling/query/?q={cleaned_query}')
        return data

    def toggle_pb_process(self, process_id, version_num=None):
        pb = {
            'Metadata': {
                'activeVersionNumber': version_num
            }
        }

        pb_str = jsonpickle.encode(pb, unpicklable=False)
        response = None

        try:
            # The response coming from Salesforce is apparently malformed and fails to parse properly
            response = self.client.restful(path=f'tooling/sobjects/FlowDefinition/{process_id}/', method='PATCH', data=pb_str)
        except Exception as ex:
            if 'Expecting value' not in str(ex):
                print(ex)