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


def convert_jks_to_pem(jks_path, keystore_pw):
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
    ks = jks.KeyStore.load(jks_path, keystore_pw, try_decrypt_keys=True)
    private_keys_certs = ""
    private_keys = ""
    ca_certs = ""

    # Convert private keys and their certificates into PEM format and append to string
    for alias, pk in ks.private_keys.items():
        if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
            private_keys = bytes_to_pem_str(pk.pkey, "RSA PRIVATE KEY")
        else:
            private_keys = bytes_to_pem_str(pk.pkey_pkcs8, "PRIVATE KEY")
        for c in pk.cert_chain:
            # c[0] contains type of cert, i.e X.509
            private_keys_certs = bytes_to_pem_str(c[1], "CERTIFICATE")

    # Convert CA Certificates into PEM format and append to string
    for alias, c in ks.certs.items():
        ca_certs = ca_certs + bytes_to_pem_str(c.cert, "CERTIFICATE")
    return private_keys_certs, private_keys, ca_certs

def write_pem(jks_key_store_path, jks_trust_store_path, keystore_pw, client_cert_path, client_key_path, ca_cert_path, ca_root_pub_pem_path):
    """
    Converts the JKS keystore, JKS truststore, and the root ca.pem
    client certificate, client key, and ca certificate

    Args:
    :jks_key_store_path: path to the JKS keystore
    :jks_trust_store_path: path to the JKS truststore
    :keystore_pw: path to file with passphrase for the keystores
    :client_cert_path: path to write the client's certificate for its private key in PEM format
    :client_key_path: path to write the client's private key in PEM format
    :ca_cert_path: path to write the chain of CA certificates required to validate certificates
    :ca_root_pub_pem_path: path to root CA on the host (root CA certificate is not present in the keystores, only the intermediate CA certificate is)

    """
    keystore_cert, keystore_key, keystore_cert = convert_jks_to_pem(jks_key_store_path, keystore_pw)
    truststore_cert, truststore_key, truststore_cert = convert_jks_to_pem(jks_trust_store_path, keystore_pw)
    with open(ca_root_pub_pem_path, "r") as f:
        ca_root_cert = f.read()
    with open(client_cert_path, "w") as f:
        f.write(keystore_cert)
    with open(client_key_path, "w") as f:
        f.write(keystore_key)
    with open(ca_cert_path, "w") as f:
        f.write(truststore_cert + "\n" + ca_root_cert)

def get_client_certificate_location():
    """
    Get location of client certificate (PEM format) for the private key signed by trusted CA
    used for 2-way TLS authentication, for example with Kafka cluster

    Returns:
        string path to client certificate in PEM format
    """
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG

def get_client_key_location():
    """
    Get location of client private key (PEM format)
    used for for 2-way TLS authentication, for example with Kafka cluster

    Returns:
        string path to client private key in PEM format
    """
    # Convert JKS to PEMs if they don't exists already
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG

def get_ca_chain_location():
    """
    Get location of chain of CA certificates (PEM format) that are required to validate the
    private key certificate of the client
    used for 2-way TLS authentication, for example with Kafka cluster

    Returns:
         string path to ca chain of certificate
    """
    if not os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CA_CHAIN_CERTIFICATE_CONFIG:
        write_pems()
    return os.getcwd() + "/" + constants.SSL_CONFIG.PEM_CA_CHAIN_CERTIFICATE_CONFIG

def write_pems():
    """
    Converts JKS keystore file into PEM to be compatible with Python libraries
    """
    t_jks_path = os.getcwd() + "/" + constants.SSL_CONFIG.T_CERTIFICATE_CONFIG
    k_jks_path = os.getcwd() + "/" + constants.SSL_CONFIG.K_CERTIFICATE_CONFIG
    write_pem(k_jks_path, t_jks_path, get_key_store_pwd(), get_client_certificate_location(), get_client_key_location(), get_ca_chain_location(), constants.SSL_CONFIG.PEM_CA_ROOT_CERT)
