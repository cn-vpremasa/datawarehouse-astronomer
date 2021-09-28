import hvac
import os

def get_vault_token():
    VAULT_TOKEN = os.getenv('VAULT_TOKEN')
    return VAULT_TOKEN


def get_vault_address():
    VAULT_ADDRESS = os.getenv('VAULT_ADDR')
    return VAULT_ADDRESS

class Vault():
    def __init__(self):
        self.secret = None

    def get_vault_data(self, url=get_vault_address(), token=get_vault_token(), force_refresh=False, SECRET_PATH="secret/data-services/evergreen/ci/service-principals/automation-sp-data-warehouse-group-development"):
        if force_refresh is True:
            client = hvac.Client(url=url, token=token)
            self.secret = client.read(SECRET_PATH)['data']

        return self.secret

    def get_secret(self, key):
        return self.secret.get(key)


vault_instance = Vault()
vault_instance.get_vault_data(force_refresh=True)