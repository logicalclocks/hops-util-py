"""
Online Feature Store functions
"""
from hops import constants
from hops.featurestore_impl.exceptions.exceptions import OnlineFeaturestorePasswordOrUserNotFound

def _get_online_feature_store_password_and_user(storage_connector):
    """
    Extracts the password and user from an online featurestore storage connector

    Args:
        :storage_connector: the storage connector of the online feature store

    Returns:
        The password and username for the online feature store of the storage connector

    Raises:
        :OnlineFeaturestorePasswordOrUserNotFound: if a password or user could not be found
    """
    args = storage_connector.arguments.split(constants.DELIMITERS.COMMA_DELIMITER)
    pw = ""
    user = ""
    for arg in args:
        if "password=" in arg:
            pw = arg.replace("password=", "")
        if "user=" in arg:
            user = arg.replace("user=", "")
    if pw == "" or user =="":
        raise OnlineFeaturestorePasswordOrUserNotFound("A password/user for the online feature store was not found")
    return pw, user


def _write_jdbc_dataframe(df, storage_connector, table_name, write_mode):
    """
    Writes a Spark dataframe to the online feature store using a JDBC connector

    Args:
        :df: the dataframe to write
        :storage_connector: the storage connector to connect to the MySQL database for the online featurestore
        :table_name: name of the table to write to in the MySQL database
        :write_mode: write mode (overwrite or append)

    Returns:
        None
    """
    pw, user = _get_online_feature_store_password_and_user(storage_connector)
    df.write.format(constants.SPARK_CONFIG.SPARK_JDBC_FORMAT) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_URL, storage_connector.connection_string) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_DBTABLE, table_name) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_USER, user) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_PW, pw) \
        .mode(write_mode) \
        .save()


def _read_jdbc_dataframe(spark, storage_connector, query):
    """
    Reads a Spark dataframe from the online feature store using a JDBC connector

    Args:
        :spark: the spark session
        :storage_connector: the storage connector to connect to the MySQL database for the online featurestore
        :query: SQL query to read from the online feature store

    Returns:
        The resulting spark dataframe
    """
    pw, user = _get_online_feature_store_password_and_user(storage_connector)
    return spark.read.format(constants.SPARK_CONFIG.SPARK_JDBC_FORMAT) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_URL, storage_connector.connection_string) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_DBTABLE, query) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_USER, user) \
        .option(constants.SPARK_CONFIG.SPARK_JDBC_PW, pw) \
        .load()