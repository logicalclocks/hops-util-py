from pyhive import hive
import os
from . import tls

def setup_hive_connection():
    """
        export enviroment variable with Hive connection configuration 
        so it can be used by ipython-sql and PyHive to establish a connection with Hive
    """

    if not ('HIVE_ENDPOINT' in os.environ and 'PROJECT_NAME' in os.environ) : 
        raise EnvironmentError("HIVE_ENDPOINT or PROJECT_NAME is not exported." + 
            "The Hive module is meant to be run only in the context of a Python kernel") 

    connection_conf = {
        'auth' : 'CERTIFICATES',
        'keystore' : tls.get_key_store(),
        'truststore' : tls.get_trust_store(),
        'keystore_password' : tls._get_cert_pw()
    }

    os.environ['DATABASE_URL'] = "hive://" + os.environ['HIVE_ENDPOINT'] + "/" + os.environ['PROJECT_NAME'] + '?' 
    os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'] + '&'.join(['%s=%s' % (key, value) for (key, value) in connection_conf.items()])