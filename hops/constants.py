"""
String Constants used in Hops-Util: Environment variables, Kafka Config, SSL Config etc.
"""


class HTTP_CONFIG:
    """
    HTTP String constants
    """
    HTTP_CONTENT_TYPE = "Content-type"
    HTTP_APPLICATION_JSON = "application/json"
    HTTP_POST = "POST"
    HTTP_PUT = "PUT"


class ENV_VARIABLES:
    """
    Environment variable names (accessible in os.environ)
    """
    KAFKA_BROKERS_ENV_VAR = "KAFKA_BROKERS"
    ELASTIC_ENDPOINT_ENV_VAR = "ELASTIC_ENDPOINT"
    PWD_ENV_VAR = "PWD"
    KAFKA_VERSION_ENV_VAR = "KAFKA_VERSION"
    LIVY_VERSION_ENV_VAR = "LIVY_VERSION"
    SPARK_VERSION_ENV_VAR = "SPARK_VERSION"
    CUDA_VERSION_ENV_VAR = "SPARK_VERSION"
    REST_ENDPOINT_END_VAR = "REST_ENDPOINT"
    TENSORFLOW_VERSION_ENV_VAR = "TENSORFLOW_VERSION"
    CUDA_VERSION_ENV_VAR = "CUDA_VERSION"
    HOPSWORKS_VERSION_ENV_VAR = "HOPSWORKS_VERSION"
    HADOOP_VERSION_ENV_VAR = "HADOOP_VERSION"
    HOPSWORKS_USER_ENV_VAR = "HOPSWORKS_USER"
    PATH_ENV_VAR = "PATH"
    PYTHONPATH_ENV_VAR = "PYTHONPATH"
    JOB_NAME_ENV_VAR = "HOPSWORKS_JOB_NAME"


class KAFKA_SSL_CONFIG:
    """
    Kafka SSL constant strings for configuration
    """
    SSL = "SSL"
    SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location"
    SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file. "
    SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password"
    SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. If a password is not set access to the truststore is still available, but integrity checking is disabled."
    SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location"
    SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password"
    SSL_KEY_PASSWORD_CONFIG = "ssl.key.password"
    SECURITY_PROTOCOL_CONFIG = "security.protocol"
    SSL_CERTIFICATE_LOCATION_CONFIG = "ssl.certificate.location"
    SSL_CA_LOCATION_CONFIG = "ssl.ca.location"
    SSL_PRIVATE_KEY_LOCATION_CONFIG = "ssl.key.location"


# General SSL config properties

class SSL_CONFIG:
    """
    General SSL configuration constants for Hops-TLS
    """
    K_CERTIFICATE_CONFIG = "k_certificate"
    T_CERTIFICATE_CONFIG = "t_certificate"
    PEM_CLIENT_CERTIFICATE_CONFIG = "client.pem"
    PEM_CLIENT_KEY_CONFIG = "client_key.pem"
    PEM_CA_CHAIN_CERTIFICATE_CONFIG = "ca_chain.pem"
    DOMAIN_CA_TRUSTSTORE = "domain_ca_truststore"
    CRYPTO_MATERIAL_PASSWORD = "material_passwd"
    PEM_CA_ROOT_CERT = "/srv/hops/kagent/host-certs/hops_root_ca.pem"


class KAFKA_PRODUCER_CONFIG:
    """
    Constant strings for Kafka producers
    """
    BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"
    KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
    VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"


class KAFKA_CONSUMER_CONFIG:
    """
    Constant strings for Kafka consumers
    """
    GROUP_ID_CONFIG = "group.id"
    ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
    AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms"
    SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms"
    KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer"
    VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer"
    AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset"
    ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
    KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer"
    VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer"


class SPARK_CONFIG:
    """
    Spark string constants
    """
    SPARK_SCHEMA_FIELD_METADATA = "metadata"
    SPARK_SCHEMA_FIELDS = "fields"
    SPARK_SCHEMA_FIELD_NAME = "name"
    SPARK_SCHEMA_FIELD_TYPE = "type"
    SPARK_SCHEMA_ELEMENT_TYPE = "elementType"
    SPARK_OVERWRITE_MODE = "overwrite"
    SPARK_APPEND_MODE = "append"
    SPARK_WRITE_DELIMITER = "delimiter"
    SPARK_WRITE_HEADER = "header"
    SPARK_TF_CONNECTOR_RECORD_TYPE = "recordType"
    SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE = "Example"
    SPARK_LONG_TYPE = "long"
    SPARK_SHORT_TYPE = "short"
    SPARK_BYTE_TYPE = "byte"
    SPARK_INTEGER_TYPE = "integer"
    SPARK_INT_TYPE = "int"
    SPARK_FLOAT_TYPE = "float"
    SPARK_DOUBLE_TYPE = 'double'
    SPARK_DECIMAL_TYPE = "decimal"
    SPARK_BIGINT_TYPE = "bigint"
    SPARK_SMALLINT_TYPE = "smallint"
    SPARK_STRING_TYPE = "string"
    SPARK_BINARY_TYPE = "binary"
    SPARK_NUMERIC_TYPES = [SPARK_BIGINT_TYPE,
                           SPARK_DECIMAL_TYPE,
                           SPARK_INTEGER_TYPE,
                           SPARK_INT_TYPE,
                           SPARK_DOUBLE_TYPE,
                           SPARK_LONG_TYPE,
                           SPARK_FLOAT_TYPE,
                           SPARK_SHORT_TYPE]
    SPARK_STRUCT = "struct"
    SPARK_ARRAY = "array"
    SPARK_ARRAY_DOUBLE = "array<double>"
    SPARK_ARRAY_INTEGER = "array<integer>"
    SPARK_ARRAY_INT = "array<int>"
    SPARK_ARRAY_BIGINT = "array<bigint>"
    SPARK_ARRAY_FLOAT = "array<float>"
    SPARK_ARRAY_DECIMAL = "array<decimal>"
    SPARK_ARRAY_STRING = "array<string>"
    SPARK_ARRAY_LONG = "array<long>"
    SPARK_ARRAY_BINARY = "array<binary>"
    SPARK_VECTOR = "vector"

class FEATURE_STORE:
    """
     Featurestore constants
    """
    MAX_CORRELATION_MATRIX_COLUMNS = 50
    TRAINING_DATASET_CSV_FORMAT = "csv"
    TRAINING_DATASET_TSV_FORMAT = "tsv"
    TRAINING_DATASET_PARQUET_FORMAT = "parquet"
    TRAINING_DATASET_TFRECORDS_FORMAT = "tfrecords"
    TRAINING_DATASET_NPY_FORMAT = "npy"
    TRAINING_DATASET_HDF5_FORMAT = "hdf5"
    TRAINING_DATASET_NPY_SUFFIX = ".npy"
    TRAINING_DATASET_HDF5_SUFFIX = ".hdf5"
    TRAINING_DATASET_CSV_SUFFIX = ".csv"
    TRAINING_DATASET_TSV_SUFFIX = ".tsv"
    TRAINING_DATASET_PARQUET_SUFFIX = ".parquet"
    TRAINING_DATASET_TFRECORDS_SUFFIX = ".tfrecords"
    CLUSTERING_ANALYSIS_INPUT_COLUMN = "featurestore_feature_clustering_analysis_input_col"
    CLUSTERING_ANALYSIS_OUTPUT_COLUMN = "featurestore_feature_clustering_analysis_output_col"
    CLUSTERING_ANALYSIS_PCA_COLUMN = "featurestore_feature_clustering_analysis_pca_col"
    CLUSTERING_ANALYSIS_FEATURES_COLUMN = "features"
    CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN = "clusters"
    CLUSTERING_ANALYSIS_CLUSTERS_COLUMN = "featurestore_feature_clustering_analysis_pca_col"
    CLUSTERING_ANALYSIS_ARRAY_COLUMN = "array"
    CLUSTERING_ANALYSIS_SAMPLE_SIZE = 50
    FEATURE_GROUP_INSERT_APPEND_MODE = "append"
    FEATURE_GROUP_INSERT_OVERWRITE_MODE = "overwrite"
    DESCRIPTIVE_STATS_SUMMARY_COL= "summary"
    DESCRIPTIVE_STATS_METRIC_NAME_COL= "metricName"
    DESCRIPTIVE_STATS_VALUE_COL= "value"
    HISTOGRAM_FREQUENCY = "frequency"
    HISTOGRAM_FEATURE = "feature"
    FEATURESTORE_SUFFIX =  "_featurestore"
    TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME = "tf_record_schema.txt"
    TF_RECORD_SCHEMA_FEATURE = "feature"
    TF_RECORD_SCHEMA_FEATURE_FIXED = "fixed_len"
    TF_RECORD_SCHEMA_FEATURE_VAR = "var_len"
    TF_RECORD_SCHEMA_TYPE = "type"
    TF_RECORD_SCHEMA_SHAPE = "shape"
    TF_RECORD_INT_TYPE = "int"
    TF_RECORD_FLOAT_TYPE = "float"
    TF_RECORD_STRING_TYPE = "string"
    TF_RECORD_INT_ARRAY_SPARK_TYPES = [SPARK_CONFIG.SPARK_ARRAY_INTEGER, SPARK_CONFIG.SPARK_ARRAY_BIGINT,
                                       SPARK_CONFIG.SPARK_ARRAY_INT, SPARK_CONFIG.SPARK_ARRAY_LONG]
    TF_RECORD_INT_SPARK_TYPES = [SPARK_CONFIG.SPARK_INTEGER_TYPE, SPARK_CONFIG.SPARK_BIGINT_TYPE,
                                 SPARK_CONFIG.SPARK_INT_TYPE, SPARK_CONFIG.SPARK_LONG_TYPE]
    TF_RECORD_STRING_SPARK_TYPES = [SPARK_CONFIG.SPARK_STRING_TYPE, SPARK_CONFIG.SPARK_BINARY_TYPE]
    TF_RECORD_STRING_ARRAY_SPARK_TYPES = [SPARK_CONFIG.SPARK_ARRAY_STRING, SPARK_CONFIG.SPARK_ARRAY_BINARY]
    TF_RECORD_FLOAT_SPARK_TYPES = [SPARK_CONFIG.SPARK_FLOAT_TYPE, SPARK_CONFIG.SPARK_DECIMAL_TYPE,
                                   SPARK_CONFIG.SPARK_DOUBLE_TYPE]
    TF_RECORD_FLOAT_ARRAY_SPARK_TYPES = [SPARK_CONFIG.SPARK_ARRAY_FLOAT, SPARK_CONFIG.SPARK_ARRAY_DECIMAL,
                                   SPARK_CONFIG.SPARK_ARRAY_DOUBLE, SPARK_CONFIG.SPARK_VECTOR]
    DATAFRAME_TYPE_SPARK = "spark"
    DATAFRAME_TYPE_NUMPY = "numpy"
    DATAFRAME_TYPE_PYTHON = "python"
    DATAFRAME_TYPE_PANDAS = "pandas"

class HIVE_CONFIG:
    """
    Hive string constants
    """
    HIVE_DATA_TYPES = [
        "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
        "DECIMAL", "TIMESTAMP", "DATE", "INTERVAL", "STRING", "VARCHAR",
        "CHAR", "BOOLEAN", "BINARY", "ARRAY", "MAP", "STRUCT", "UNIONTYPE"
    ]
    HIVE_BIGINT_TYPE = "BIGINT"
    HIVE_INT_TYPE = "INT"
    HIVE_CHAR_TYPE = "CHAR"


class REST_CONFIG:
    """
    REST endpoints and JSON properties used for communicating with Hopsworks REST API
    """
    JSON_KEYSTOREPWD = "keyStorePwd"
    JSON_SCHEMA_CONTENTS = "contents"
    JSON_SCHEMA_TOPICNAME = "topicName"

    JSON_FEATURESTORENAME = "featurestoreName"

    JSON_FEATUREGROUPNAME = "name"
    JSON_FEATUREGROUP_VERSION = "version"
    JSON_FEATUREGROUP_JOBNAME = "jobName"
    JSON_FEATUREGROUP_FEATURES = "features"
    JSON_FEATUREGROUP_DEPENDENCIES = "dependencies"
    JSON_FEATUREGROUP_DESCRIPTION = "description"
    JSON_FEATUREGROUP_FEATURE_CORRELATION = "featureCorrelationMatrix"
    JSON_FEATUREGROUP_DESC_STATS = "descriptiveStatistics"
    JSON_FEATUREGROUP_UPDATE_METADATA = "updateMetadata"
    JSON_FEATUREGROUP_UPDATE_STATS = "updateStats"
    JSON_FEATUREGROUP_FEATURES_HISTOGRAM = "featuresHistogram"
    JSON_FEATUREGROUP_FEATURES_CLUSTERS = "clusterAnalysis"
    JSON_FEATUREGROUPS = "featuregroups"

    JSON_FEATURE_NAME = "name"
    JSON_FEATURE_TYPE = "type"
    JSON_FEATURE_DESCRIPTION = "description"
    JSON_FEATURE_PRIMARY = "primary"

    JSON_TRAINING_DATASET_NAME = "name"
    JSON_TRAINING_DATASETS = "trainingDatasets"
    JSON_TRAINING_DATASET_HDFS_STORE_PATH = "hdfsStorePath"
    JSON_TRAINING_DATASET_FORMAT = "dataFormat"
    JSON_TRAINING_DATASET_SCHEMA = "features"
    JSON_TRAINING_DATASET_VERSION = "version"
    JSON_TRAINING_DATASET_DEPENDENCIES = "dependencies"
    JSON_TRAINING_DATASET_DESCRIPTION = "description"
    JSON_TRAINING_DATASET_FEATURE_CORRELATION = "featureCorrelationMatrix"
    JSON_TRAINING_DATASET_FEATURES_HISTOGRAM = "featuresHistogram"
    JSON_TRAINING_DATASET_CLUSTERS = "clusterAnalysis"
    JSON_TRAINING_DATASET_DESC_STATS = "descriptiveStatistics"
    JSON_TRAINING_DATASET_JOBNAME = "jobName"
    JSON_TRAINING_DATASET_UPDATE_METADATA = "updateMetadata"
    JSON_TRAINING_DATASET_UPDATE_STATS = "updateStats"

    JSON_SCHEMA_VERSION = "version"
    JSON_KEYSTORE = "keyStore"

    HOPSWORKS_REST_RESOURCE = "hopsworks-api/api"
    HOPSWORKS_REST_APPSERVICE = "appservice"
    HOPSWORKS_SCHEMA_RESOURCE = "schema"
    HOPSWORKS_FEATURESTORE_RESOURCE = "featurestore"
    HOPSWORKS_FEATURESTORES_RESOURCE = "featurestores"
    HOPSWORKS_CLEAR_FEATUREGROUP_RESOURCE = "featurestore/featuregroup/clear"
    HOPSWORKS_CREATE_FEATUREGROUP_RESOURCE = "featurestore/featuregroups"
    HOPSWORKS_UPDATE_FEATUREGROUP_METADATA = "featurestore/featuregroup"
    HOPSWORKS_CREATE_TRAINING_DATASET_RESOURCE = "featurestore/trainingdatasets"
    HOPSWORKS_UPDATE_TRAINING_DATASET_METADATA = "featurestore/trainingdataset"

    JSON_DESCRIPTIVE_STATS_FEATURE_NAME= "featureName"
    JSON_DESCRIPTIVE_STATS_METRIC_VALUES= "metricValues"
    JSON_DESCRIPTIVE_STATS= "descriptiveStats"

    JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME = "datapointName"
    JSON_CLUSTERING_ANALYSIS_FIRST_DIMENSION = "firstDimension"
    JSON_CLUSTERING_ANALYSIS_SECOND_DIMENSION = "secondDimension"
    JSON_CLUSTERING_ANALYSIS_CLUSTER = "cluster"
    JSON_CLUSTERING_ANALYSIS_CLUSTERS = "clusters"
    JSON_CLUSTERING_ANALYSIS_DATA_POINTS = "dataPoints"

    JSON_HISTOGRAM_FREQUENCY = "frequency"
    JSON_HISTOGRAM_BIN = "bin"
    JSON_HISTOGRAM_FEATURE_NAME = "featureName"
    JSON_HISTOGRAM_FREQUENCY_DISTRIBUTION = "frequencyDistribution"
    JSON_HISTOGRAM_FEATURE_DISTRIBUTIONS = "featureDistributions"

    JSON_CORRELATION_FEATURE_NAME = "featureName"
    JSON_CORRELATION = "correlation"
    JSON_CORRELATION_VALUES = "correlationValues"

    JSON_FEATURE_CORRELATIONS = "featureCorrelations"

    JSON_ERROR_CODE = "errorCode"
    JSON_ERROR_MSG = "errorMsg"
    JSON_USR_MSG = "usrMsg"

class DELIMITERS:
    """
    String delimiters constants
    """
    SLASH_DELIMITER = "/"
    COMMA_DELIMITER = ","
    TAB_DELIMITER = "\t"
    COLON_DELIMITER = ":"
