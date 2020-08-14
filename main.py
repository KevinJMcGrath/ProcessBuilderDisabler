import sfdc_clients
import sobjects.account as account
import sobjects.contact as contact
import sobjects.opportunity as opp
import sobjects.process_builder as pb


def activate_processes():
    pb.toggle_processes(True)

def deactivate_processes():
    pb.toggle_processes(False)


def update_contacts_pw_last_changed():
    contact.update_contacts_pw_last_changed()

def update_account_transcript_types():
    account.update_account_transcript_types()

def export_pw_last_changed():
    contact.export_pw_last_changed()

def update_opp_amounts_trigger_custom_fields():
    opp.update_opp_amounts()


if __name__ == '__main__':
    update_opp_amounts_trigger_custom_fields()
    # deactivate_processes()

    # activate_processes()
