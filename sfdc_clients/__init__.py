from .sfdc import SFClient
from .sfdc_bulk_custom import SFBulkCustomClient

print('Loading SFDC client')
simple_client = SFClient.from_config()
print('Loading Bulk Update client')
bulk_client = SFBulkCustomClient(simple_client.client.bulk_url, simple_client.client.session_id)