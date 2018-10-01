"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import jks
import string
import base64
import textwrap
from hops import constants
import os

def get_key_store():
    """
    Get keystore location

    Returns:
        keystore filename
    """
    return constants.SSL_CONFIG.K_CERTIFICATE_CONFIG


def get_trust_store():
    """
    Get truststore location

    Returns:
         truststore filename
    """
    return constants.SSL_CONFIG.T_CERTIFICATE_CONFIG


def _get_cert_pw():
    """
    Get keystore password from local container

    Returns:
        Certificate password
    """
    pwd_path = os.getcwd() + "/" + constants.SSL_CONFIG.CRYPTO_MATERIAL_PASSWORD

    if not os.path.exists(pwd_path):
        raise AssertionError('material_passwd is not present in directory: {}'.format(pwd_path))

    with open(pwd_path) as f:
        key_store_pwd = f.read()

    # remove special characters (due to bug in password materialized, should not be necessary when the bug is fixed)
    key_store_pwd = "".join(list(filter(lambda x: x in string.printable and not x == "@", key_store_pwd)))
    return key_store_pwd


def get_key_store_cert():
    """
    Get keystore certificate from local container

    Returns:
        Certificate password
    """
    cert_path = os.getcwd() + "/" + constants.SSL_CONFIG.K_CERTIFICATE_CONFIG

    if not os.path.exists(cert_path):
        raise AssertionError('k_certificate is not present in directory: {}'.format(cert_path))

    # read as bytes, don't try to use utf-8 encoding
    with open(cert_path, "rb") as f:
        key_store_cert = f.read()
        key_store_cert = base64.b64encode(key_store_cert)

    return key_store_cert


def get_key_store_pwd():
    """
    Get keystore password

    Returns:
         keystore password
    """
    return _get_cert_pw()


def get_trust_store_pwd():
    """
    Get truststore password

    Returns:
         truststore password
    """
    return _get_cert_pw()


def bytes_to_pem_str(der_bytes, pem_type):
    """
    Utility function for creating PEM files

    Args:
    :der_bytes: DER encoded bytes
    :pem_type: type of PEM, e.g Certificate, Private key, or RSA private key

    Returns:
         PEM String for a DER-encoded certificate or private key
    """
    pem_str = ""
    pem_str = pem_str + "-----BEGIN {}-----".format(pem_type) + "\n"
    pem_str = pem_str + "\r\n".join(textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64)) + "\n"
    pem_str = pem_str + "-----END {}-----".format(pem_type) + "\n"
    return pem_str


def convert_keystore_jks_to_pem(jks_path, pw):
    """
    Converts a keystore JKS that contains client private key,
     client certificate and CA certificate that was used to
     sign the certificate, to three PEM-format strings.

    Args:
    :jks_path: path to the JKS file
    :pw: password for decrypting the JKS file

    Returns:
         strings: (client_cert, client_key, ca_cert)
    """
    # load the keystore and decrypt it with password
    ks = jks.KeyStore.load(jks_path, pw, try_decrypt_keys=True)
    client_cert = ""
    client_key = ""
    ca_cert = ""

    for alias, pk in ks.private_keys.items():
        if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
            client_key = bytes_to_pem_str(pk.pkey, "RSA PRIVATE KEY")
        else:
            client_key = bytes_to_pem_str(pk.pkey_pkcs8, "PRIVATE KEY")
        for c in pk.cert_chain:
            # c[0] contains type of cert, i.e X.509
            client_cert = bytes_to_pem_str(c[1], "CERTIFICATE")

    # Convert CA Certificates into PEM format and append to string
    for alias, c in ks.certs.items():
        ca_cert = ca_cert + bytes_to_pem_str(c.cert, "CERTIFICATE")
    return client_cert, client_key, ca_cert

def write_pem(jks_path, pw, client_cert_path, client_key_path, ca_cert_path, kstore_pem_path):
    """
    Converts a JKS keystore to three PEM files containing
    client certificate, client key, and ca certificate

    Args:
    :jks_path: path to the keystore JKS file
    :pw: password for decrypting the JKS file
    :output_path: path to write the PEM file

    """
    client_cert, client_key, ca_cert = convert_keystore_jks_to_pem(jks_path, pw)
    with open(client_cert_path, "w") as f:
        f.write(client_cert)
    with open(client_key_path, "w") as f:
        f.write(client_key)
    with open(ca_cert_path, "w") as f:
        f.write(ca_cert)
    with open(kstore_pem_path, "w") as f:
        f.write(client_key + "\n" + client_cert + "\n" + ca_cert)

def get_ca_certificate_location():
    """
    Get location of trusted CA certificate (server.pem) for 2-way TLS authentication with Kafka cluster

    Returns:
        string path to ca certificate (server.pem)
    """
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CA_CERTIFICATE_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CA_CERTIFICATE_CONFIG

def get_client_key_location():
    """
    Get location of client private key (client.key) for 2-way TLS authentication with Kafka cluster

    Returns:
        string path to client private key (client.key)
    """
    # Convert JKS to PEMs if they don't exists already
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG

def get_client_certificate_location():
    """
    Get location of client certificate (client.pem) for 2-way TLS authentication with Kafka cluster

    Returns:
         string path to client certificate (client.pem)
    """
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG

def write_pems():
    """
    Converts JKS keystore file into PEM to be compatible with Python libraries
    """
    k_jks_path = os.getcwd() + "/" + constants.SSL_CONFIG.K_CERTIFICATE_CONFIG
    client_cert_pem_path = get_client_certificate_location()
    client_key_pem_path = get_client_key_location()
    ca_cert_pem_path = get_ca_certificate_location()
    kstore_pem_path = os.getcwd() + "/k_certificate.pem"
    write_pem(k_jks_path, get_key_store_pwd(), client_cert_pem_path, client_key_pem_path, ca_cert_pem_path, kstore_pem_path)
