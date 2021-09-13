import pprint

import sfdc_clients
import sobjects.account as account
import sobjects.contact as contact
import sobjects.lead as lead
import sobjects.opportunity as opp
import sobjects.opp_split as opp_split
import sobjects.campaign_member as cm
import sobjects.wsi_user_detail as wsi_ud

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

def update_contact_alert_template():
    contact.update_contact_alert_new_template()

def update_contact_channel_view():
    contact.update_contact_channel_view()

def update_contact_mtnews_stuff():
    contact.update_mtnews_features()

def deactivate_former_users():
    contact.disable_specific_contacts_as()

def inactive_user_check():
    contact.check_inactive_users_as()

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


# **************************************
#           Campaign Members
# **************************************

def del_campaign_members():
    path = "C:\\Users\\Kevin\\Dropbox\\Work\\AlphaSense\\Documents\\2021\\CampaignMembersForPurge-20210524.csv"

    cm.delete_campaign_members_by_csv(path)


# **************************************
#           WSI User Detail
# **************************************

def update_user_detail_prior_month():
    wsi_ud.populate_prior_month()


def update_user_detail_current_month():
    wsi_ud.update_current_month()


def delete_dupe_histories():
    wsi_ud.remove_duplicate_history_records()

if __name__ == '__main__':
    # 2021-09-13 KJM It is no longer necessary to disable PBs for Contacts

    # deactivate_processes('Contact')
    # deactivate_processes('Opportunity')

    deactivate_former_users()

    # activate_processes('Contact')
    # activate_processes('Opportunity')


