import sfdc_clients

contact_pb_dict = {
    # 'AS_Premium_News_Content': 4,
    # No longer needed KJM 2021-08-06
    # 'Auto_Populate_Opportunity_Field_in_Contact_Record': 9,
    # 'Auto_Populate_Renewal_Opportunity_Field_in_Contact_Record': 4,
    # 'Corp_Investor_Relations_PositionId_Update': 2,
    # 'MCO_Financial_Inst_Global_AS_TRUE': 2,
    # 'MCO_Fundamental_Global_AS_TRUE': 2,
    # 'Set_up_trial_config': 9,
    # 'Transcript_Type_Default': 2,  # Disabled per https://alphasense.jira.com/browse/SI-115
    # 'Turn_on_Moody_s_Credit_Research_Capital_Markets': 3
}

opp_pb_dict = {
    #'Delete_Content_of_Opportunity_field_in_Contact_if_Contact_Role_is_Deleted': None,
    'Assign_primary_contact': 3,
    'Opp_Stage_Date_Stamps': 3,
    'Update_Opportunity_Intacct_Entity': 1,
    'Deal_Source': 6,
    'Opportunity_Account_Manager_for_Won_Upsell_Opps': 4

}

def toggle_processes(activate: bool=False, sobject: str='Contact'):
    pb_map = sfdc_clients.simple_client.get_all_pb_processes()

    for pb_id, pb in pb_map.items():
        pb_id = pb['Id']
        pb_name = pb['DeveloperName']


        working_dict = contact_pb_dict

        if sobject == 'Opportunity':
            working_dict = opp_pb_dict

        if pb_name in working_dict.keys():
            active_version = None

            if activate:
                active_version = working_dict[pb_name]

            print(f'{"Activating" if activate else "Deactivating"} process {pb_name}')
            sfdc_clients.simple_client.toggle_pb_process(pb_id, active_version)