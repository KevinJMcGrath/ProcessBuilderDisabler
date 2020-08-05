import jsonpickle

import models


with open('./config.json', 'r') as config_file:
    _config = jsonpickle.decode(config_file.read())

Salesforce = models.SalesforceSettings(_config['salesforce'])