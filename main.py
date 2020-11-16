import pprint

import sfdc_clients
import sobjects.account as account
import sobjects.contact as contact
import sobjects.lead as lead
import sobjects.opportunity as opp
import sobjects.opp_split as opp_split

import sobjects.admin as admin
import sobjects.process_builder as pb

# **************************************
#           System Functions
# **************************************

def show_all_pb():
    pb_dict = sfdc_clients.simple_client.get_all_pb_processes()
    pprint.pprint(pb_dict)

def activate_processes(sobject: str='Contact'):
    pb.toggle_processes(True, sobject)

def deactivate_processes(sobject: str='Contact'):
    pb.toggle_processes(False, sobject)

def get_background_job_data():
    admin.get_background_task_history()

# **************************************
#               Contact
# **************************************

def update_contacts_pw_last_changed():
    contact.update_contacts_pw_last_changed()

def update_channel_view_as():
    contact.update_channel_view_as()

def fix_channel_view_request():
    contact.fix_channel_view_request()

def export_pw_last_changed():
    contact.export_pw_last_changed()

def update_contact_usage_limits():
    contact.update_contacts_usage_limits()

# **************************************
#               Account
# **************************************

def update_account_transcript_types():
    account.update_account_transcript_types()

# **************************************
#               Opportunity
# **************************************

def update_opp_amounts_trigger_custom_fields():
    opp.update_opp_amounts(update_quota_credit=True, update_comm_credit=True, update_renewal_credit=True)

def revert_opp_amounts_trigger_custom_fields():
    opp.revert_opp_amounts()

# **************************************
#           Opportunity Split
# **************************************

def insert_new_splits():
    opp_split.fix_2020_opp_splits()

if __name__ == '__main__':
    # deactivate_processes('Contact')
    # lead.match_all_leads()
    activate_processes('Contact')


