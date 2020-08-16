from datetime import datetime

def convert_sfdc_datetime_to_datetime(datetime_str: str):
    if datetime_str:
        return datetime.fromisoformat(datetime_str.split('.')[0])

    return None