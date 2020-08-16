import pprint

import sfdc_clients
import sobjects.account as account
import sobjects.contact as contact
import sobjects.opportunity as opp
import sobjects.process_builder as pb


def activate_processes(sobject: str='Contact'):
    pb.toggle_processes(True, sobject)

def deactivate_processes(sobject: str='Contact'):
    pb.toggle_processes(False, sobject)

def update_contacts_pw_last_changed():
    contact.update_contacts_pw_last_changed()

def update_account_transcript_types():
    account.update_account_transcript_types()

def export_pw_last_changed():
    contact.export_pw_last_changed()

def update_opp_amounts_trigger_custom_fields():
    opp.update_opp_amounts()

def revert_opp_amounts_trigger_custom_fields():
    opp.revert_opp_amounts()

def show_all_pb():
    pb_dict = sfdc_clients.simple_client.get_all_pb_processes()
    pprint.pprint(pb_dict)


def update_channel_view_as():
    contact.update_channel_view_as()

def fix_channel_view_request():
    contact.fix_channel_view_request()

if __name__ == '__main__':
    deactivate_processes('Contact')
    fix_channel_view_request()
    activate_processes('Contact')


