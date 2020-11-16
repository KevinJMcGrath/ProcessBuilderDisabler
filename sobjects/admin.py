import sfdc_clients

# this doesn't work the way I expected. It will apparently only show errors. 
def get_background_task_history():
    kjm_id = '0053g0000016qBw'
    soql = "SELECT CreatedbyId, CreatedDate, Id, Message, MessageType, ParentId FROM BackgroundOperationResult LIMIT 1000" \
           #"WHERE CreatedDate >= THIS_WEEK"

    items = sfdc_clients.simple_client.execute_query(soql)['records']

    for item in items:
        if item['CreatedById'] == kjm_id:
            print(item)