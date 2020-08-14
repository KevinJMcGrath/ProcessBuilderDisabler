import sfdc_clients

contact_pb_dict = {
    'AS_Premium_News_Content': 2,
    'Auto_Populate_Opportunity_Field_in_Contact_Record': 9,
    # 'Contact_Owner_equals_AM_when_Inactive_user_is_Activated': 3,
    'Corp_Investor_Relations_PositionId_Update': 2,
    'MCO_Financial_Inst_Global_AS_TRUE': 2,
    'MCO_Fundamental_Global_AS_TRUE': 2,
    'Stamp_MQL_Final_DateTime_Contacts': 2,
    'Set_up_trial_config': 9,
    'Turn_of_direct_broker_feed': 2,
    'Turn_on_Moody_s_Credit_Research_Capital_Markets': 2,
    'Turn_on_underlying_MCO_Corporate_Global_AS_fields': 2
}

def toggle_processes(activate: bool=False):
    pb_map = sfdc_clients.simple_client.get_all_pb_processes()

    for pb_id, pb in pb_map.items():
        pb_id = pb['Id']
        pb_name = pb['DeveloperName']

        if pb_name in contact_pb_dict.keys():
            active_version = None

            if activate:
                active_version = contact_pb_dict[pb_name]

            print(f'{"Activating" if activate else "Deactivating"} process {pb_name}')
            sfdc_clients.simple_client.toggle_pb_process(pb_id, active_version)