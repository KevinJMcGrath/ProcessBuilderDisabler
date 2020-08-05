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

    def get_contacts(self):
        soql = 'SELECT Id, xxUser_Timezone_AS__c, User_Timezone_AS__c FROM Contact'

        print('Downloading Contacts from Salesforce...', end='')
        response = self.client.query_all(soql)
        print('done!')

        return [u for u in response['records']]

    def update_contacts(self, contacts_for_update: list):
        group_count, remainder = divmod(len(contacts_for_update), self.batch_size_limit)

        for i in range(0, group_count):
            start = self.batch_size_limit * i
            end = self.batch_size_limit * (i + 1) - 1

            if end >= len(contacts_for_update):
                end = len(contacts_for_update) - 1

            print(f'Sending Contact update rows {start} to {end}')
            self.client.update_contacts(contacts_for_update[start:end])

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
            print(ex)