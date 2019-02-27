from pyhive import hive
import os
import tls

def setup_hive_connection():
    """
        export enviroment variable with Hive connection configuration 
        so it can be used by ipython-sql and PyHive to establish a connection with Hive
    """

    connection_conf = {
        'auth' : 'CERTIFICATES',
        'keystore' : tls.get_key_store(),
        'truststore' : tls.get_trust_store(),
        'keystore_password' : tls._get_cert_pw()
    }

    os.environ['DATABASE_URL'] = "hive://" + os.environ['HIVE_ENDPOINT'] + "/" + os.environ['PROJECT_NAME'] + '?' 
    os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'] + '&'.join(['%s=%s' % (key, value) for (key, value) in connection_conf.items()])