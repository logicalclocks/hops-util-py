"""

Utility functions to export models to the Models dataset and get information about models currently being served in the project.

"""

from hops import hdfs

import os
import pydoop.hdfs
import json
import base64

try:
    import http.client as http
except ImportError:
    import httplib as http

# model_path could be local or in HDFS, return path in hopsworks where it is placed
def export(local_model_path, model_name, model_version):
    """

    Args:
        local_model_path:
        model_name:
        model_version:

    Returns:

    """

    project_path = hdfs.project_path()

    # Create directory with model name
    hdfs_handle = hdfs.get()
    model_name_root_directory = project_path + '/Models/' + str(model_name) + '/' + str(model_version) + '/'
    hdfs_handle.create_directory(model_name_root_directory)

    for (path, dirs, files) in os.walk(local_model_path):

        hdfs_export_subpath = path.replace(local_model_path, '')

        current_hdfs_dir = model_name_root_directory + '/' + hdfs_export_subpath

        if not hdfs_handle.exists(current_hdfs_dir):
            hdfs_handle.create_directory(model_name_root_directory)

        for f in files:
            if not hdfs_handle.exists(current_hdfs_dir + '/' + f):
                pydoop.hdfs.put(path + '/' + f, current_hdfs_dir)

        for d in dirs:
            if not hdfs_handle.exists(current_hdfs_dir + '/' + d):
                pydoop.hdfs.put(path + '/' + d, current_hdfs_dir + '/')
        break

def get_serving_endpoint(model, project=None):
    """

    Args:
        model:
        project:

    Returns:

    """

    endpoint = os.environ['REST_ENDPOINT']

    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index+1:]

    host_port_pair = endpoint.split(':')

    #hardcode disabled for now
    os.environ['SSL_ENABLED'] = 'false'

    if os.environ['SSL_ENABLED'] == 'true':
        connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    else:
        connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))

    headers = {'Content-type': 'application/json'}

    material_passwd = os.getcwd() + '/material_passwd'

    if not os.path.exists(material_passwd):
        raise AssertionError('material_passwd is not present in current working directory')

    with open(material_passwd) as f:
        keyStorePwd = f.read()

    k_certificate = os.getcwd() + '/k_certificate'

    if not os.path.exists(material_passwd):
        raise AssertionError('k_certificate is not present in current working directory')

    with open(k_certificate) as f:
        keyStore = f.read()
        keyStore = base64.b64encode(keyStore)

    if not project:
        project = hdfs.project_name()

    json_contents = {'project': project,
                     'model': model,
                     'keyStorePwd': keyStorePwd,
                     'keyStore': keyStore
                     }

    json_embeddable = json.dumps(json_contents)

    connection.request('POST', '/hopsworks-api/api/appservice/tfserving', json_embeddable, headers)

    response = connection.getresponse()
    respBody = response.read()
    responseObject = json.loads(respBody)

    host = responseObject['host']
    port = responseObject['port']

    return str(host) + ':' + str(port)

