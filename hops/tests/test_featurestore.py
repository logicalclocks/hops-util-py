"""
Unit tests for the feature store python client on Hops.

The tests uses spark-local mode in combination with tests to test functionality of feature store client.
HDFS/integration with hopsworks is not tested. The tests are structured as follows:

1. Create sample hive featurestore db locally
2. Create sample training datasets
3. Run isolated unit tests against the sample data
"""

# Regular imports (do not need to be mocked and are not dependent on mocked imports)
import tensorflow as tf
import pytest
import mock
import logging
from petastorm.unischema import Unischema, UnischemaField
from petastorm.codecs import ScalarCodec
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, ArrayType
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
import pyspark
from random import choice
from string import ascii_uppercase
import json
import tensorflow as tf
import os
import h5py
import shutil

# Mocked imports and modules that depends on mocked imports
orig_import = __import__
pydoop_mock = mock.Mock()
pydoop_hdfs_mock = mock.Mock()
pydoop_hdfs_path_mock = mock.Mock()

# For mocking pydoop imports to run tests locally (pydoop requires HADOOP_HOME etc to be set just to import,
# so therefore we mock it)
def import_mock(name, *args):
    if name == 'pydoop':
        return pydoop_mock
    if name == 'pydoop.hdfs':
        return pydoop_hdfs_mock
    if name == 'pydoop.hdfs.path':
        return pydoop_hdfs_path_mock
    return orig_import(name, *args)

import sys

if (sys.version_info > (3, 0)):
    # Mock imports for Python 3
    with mock.patch('builtins.__import__', side_effect=import_mock):
        import pydoop.hdfs as pydoop
        from hops import hdfs
        from hops import featurestore
        from hops import constants
        from hops import util
        from hops import tls

else:
    # Python 2
    with mock.patch('__builtin__.__import__', side_effect=import_mock):
        import pydoop.hdfs as pydoop
        from hops import hdfs
        from hops import featurestore
        from hops import constants
        from hops import util
        from hops import tls


class TestFeaturestoreSuite(object):
    """
    Unit Test Suite for the Featurestore Python Client
    """

    pytest.logger = logging.getLogger("featurestore_tests")

    @pytest.fixture
    def sample_metadata(self):
        """ Fixture for setting up some sample metadata for tests """
        with open("./hops/tests/test_resources/featurestore_metadata.json") as f:
            metadata = json.load(f)
            f.close()
            return metadata

    @pytest.fixture
    def sample_featurestores(self):
        """ Returns a sample featurestore config for testing against """
        return [
            {'featurestoreId': 1, 'featurestoreName': 'demo_featurestore_admin000_featurestore',
             'featurestoreDescription': 'Featurestore database for project: demo_featurestore_admin000',
             'hdfsStorePath': 'hdfs://10.0.2.15:8020/apps/hive/warehouse/demo_featurestore_admin000_featurestore.db',
             'projectName': 'demo_featurestore_admin000',
             'projectId': 1, 'inodeId': 100289}]

    def _sample_spark_dataframe(self, spark):
        """ Creates a sample dataframe for testing"""
        sqlContext = SQLContext(spark.sparkContext)
        schema = StructType([StructField("equipo_id", IntegerType(), True),
                             StructField("equipo_presupuesto", FloatType(), True),
                             StructField("equipo_posicion", IntegerType(), True)
                             ])
        sample_df = sqlContext.createDataFrame([(999, 41251.52, 1), (998, 1319.4, 8), (997, 21219.1, 2)], schema)
        return sample_df

    def spark_session(self):
        """ Creates spark session if it do not exists, otherwise returns the existing one """
        spark = SparkSession \
            .builder \
            .appName('hops_featurestore_test') \
            .config("spark.jars",
                    "./hops/tests/test_resources/spark-tensorflow-connector_2.11-1.12.0.jar,"
                    "./hops/tests/test_resources/spark-avro_2.11-2.4.0.jar") \
            .master('local[*]') \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    @pytest.fixture
    def prepare_featurestore_db_and_training_Datsets(self):
        """
        Creates the featurestore DB and inserts sample data, it also creates some training datasets.
        This is run before all tests in this suite.
        """
        pytest.logger.info("Creating Test Hive Database: test_project_featurestore")
        spark = self.spark_session()
        # Create test_project_featurestore
        spark.sql("DROP DATABASE IF EXISTS test_project_featurestore CASCADE")
        spark.sql("create database IF NOT EXISTS test_project_featurestore")
        spark.sql("use test_project_featurestore")
        games_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/games_features.csv")
        games_features_df.write.format("hive").mode("overwrite").saveAsTable("games_features_1")
        players_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/players_features.csv")
        players_features_df.write.format("hive").mode("overwrite").saveAsTable("players_features_1")
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        teams_features_df.write.format("hive").mode("overwrite").saveAsTable("teams_features_1")
        season_scores_features_df = spark.read.format("csv").option("header", "true").option("inferSchema",
                                                                                             "true").load(
            "./hops/tests/test_resources/season_scores_features.csv")
        season_scores_features_df.write.format("hive").mode("overwrite").saveAsTable("season_scores_features_1")
        attendances_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/attendances_features.csv")
        attendances_features_df.write.format("hive").mode("overwrite").saveAsTable("attendances_features_1")
        attendances_features_df.write.format("hive").mode("overwrite").saveAsTable("attendances_features_2")
        # Create other_featurestore
        pytest.logger.info("Creating Test Hive Database: other_featurestore")
        spark.sql("DROP DATABASE IF EXISTS other_featurestore CASCADE")
        spark.sql("create database IF NOT EXISTS other_featurestore")
        spark.sql("use other_featurestore")
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        teams_features_2_df = teams_features_df. \
            withColumnRenamed("team_id", "equipo_id"). \
            withColumnRenamed("team_budget", "equipo_presupuesto"). \
            withColumnRenamed("team_position", "equipo_posicion")
        teams_features_2_df.write.format("hive").mode("overwrite").saveAsTable("teams_features_spanish_1")
        # Create Training Datasets
        pytest.logger.info("Creating Test Training Datasets")
        if os.path.exists("./training_datasets"):
            shutil.rmtree("training_datasets", ignore_errors=True)
        os.mkdir("training_datasets")
        spark.sql("use test_project_featurestore")
        features_df = spark.sql(
            "SELECT team_budget, average_position, sum_player_rating, average_attendance, average_player_worth, "
            "sum_player_worth, sum_position, sum_attendance, average_player_rating, team_position, "
            "sum_player_age, average_player_age FROM teams_features_1 JOIN season_scores_features_1 "
            "JOIN players_features_1 JOIN attendances_features_1 "
            "ON teams_features_1.`team_id`=season_scores_features_1.`team_id` "
            "AND teams_features_1.`team_id`=players_features_1.`team_id` "
            "AND teams_features_1.`team_id`=attendances_features_1.`team_id`")
        features_df.write.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT) \
            .option(constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                    constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(
            constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE).save("./training_datasets/team_position_prediction_1")
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(features_df)
        with open('training_datasets/schema.json', 'w') as f:
            json.dump(json_schema, f)
        features_df.write.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT) \
            .option(constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                    constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(
            constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE).save("./training_datasets/team_position_prediction_2")
        features_df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                                 constants.DELIMITERS.COMMA_DELIMITER).mode(
            constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE).csv("./training_datasets/team_position_prediction_csv_1")
        features_df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                                 constants.DELIMITERS.TAB_DELIMITER).mode(
            constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE).csv("./training_datasets/team_position_prediction_tsv_1")
        features_df.write.mode(constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE).parquet(
            "./training_datasets/team_position_prediction_parquet_1")
        features_df.write.mode(constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE) \
            .format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT) \
            .save("./training_datasets/team_position_prediction_avro_1")
        features_df.write.mode(constants.SPARK_CONFIG.SPARK_OVERWRITE_MODE) \
            .format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT) \
            .save("./training_datasets/team_position_prediction_orc_1")
        features_npy = np.array(features_df.collect())
        np.save("./training_datasets/team_position_prediction_npy_1", features_npy)
        hdf5_file = h5py.File(
            "./training_datasets/team_position_prediction_hdf5_1" +
            constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        hdf5_file.create_dataset("team_position_prediction_hdf5", data=features_npy)

    @pytest.mark.prepare
    def test_prepare(self, prepare_featurestore_db_and_training_Datsets):
        """ Prepares the Hive Database for the tests, this should run before anything else"""
        assert True

    def test_project_featurestore(self):
        """ Tests that project_featurestore() returns the correct name"""
        featurestore_name = "test_project_featurestore"
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        assert featurestore.project_featurestore() == featurestore_name
        hdfs.project_name = mock.MagicMock(return_value="TEST_PROJECT")
        assert featurestore.project_featurestore() == featurestore_name

    def test_get_table_name(self):
        """ Tests that _get_table_name returns the correct Hive table name"""
        assert featurestore._get_table_name("test_fg", 1) == "test_fg_1"
        assert featurestore._get_table_name("test_fg", 2) == "test_fg_2"

    def test_parse_featuregroups_json(self, sample_metadata):
        """ Tests that featuregroups are parsed correctly given a valid json metadata object"""
        featuregroups = sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS]
        parsed_featuregroups = featurestore._parse_featuregroups_json(featuregroups)
        names = []
        for p_fg in parsed_featuregroups:
            assert set(p_fg.keys()) == set([
                constants.REST_CONFIG.JSON_FEATUREGROUPNAME,
                constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES,
                constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION])
            names.append(p_fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME])
            for p_f in p_fg[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES]:
                assert set(p_f.keys()) == set([
                    constants.REST_CONFIG.JSON_FEATURE_NAME,
                    constants.REST_CONFIG.JSON_FEATURE_TYPE,
                    constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION,
                    constants.REST_CONFIG.JSON_FEATURE_PRIMARY
                ])
        assert set(names) == set(
            ["games_features", "season_scores_features", "attendances_features", "players_features",
             "teams_features"])

    def test_find_featuregroup_that_contains_feature(self, sample_metadata):
        """ Tests the _find_featuregroup_that_contains_feature method for the query planner"""
        featuregroups = featurestore._parse_featuregroups_json(
            sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS])
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "average_attendance")
        assert len(matches) == 1
        assert matches[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "attendances_features"
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "average_position")
        assert len(matches) == 1
        assert matches[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "season_scores_features"
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "score")
        assert len(matches) == 1
        assert matches[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "games_features"
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "team_position")
        assert len(matches) == 1
        assert matches[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "teams_features"
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "average_player_worth")
        assert len(matches) == 1
        assert matches[0][constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "players_features"
        matches = featurestore._find_featuregroup_that_contains_feature(featuregroups, "team_id")
        assert len(matches) == 4

    def test_run_and_log_sql(self):
        """ Test for _run_and_log_sql, verifies that the sql method on the sparksession is called correctly"""
        spark_mock = mock.Mock()
        spark_mock.sql = mock.MagicMock(return_value=None)
        sql = "select * from test"
        featurestore._run_and_log_sql(spark_mock, sql)
        spark_mock.sql.assert_called_with(sql)

    def test_use_database(self):
        """ Test for _use_database, verfifies that the hive database is selected correctly"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        database = featurestore.project_featurestore()
        spark = self.spark_session()
        featurestore._use_featurestore(spark, database)
        selected_db = spark.sql("select current_database()").toPandas()["current_database()"][0]
        assert selected_db == database
        featurestore._use_featurestore(spark)
        selected_db = spark.sql("select current_database()").toPandas()["current_database()"][0]
        assert selected_db == database

    def test_return_dataframe_type(self):
        """ Test for the return_dataframe_type method"""
        spark = self.spark_session()
        sample_df = self._sample_spark_dataframe(spark)
        assert sample_df.count() == 3
        converted_df = featurestore._return_dataframe_type(sample_df, constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert isinstance(converted_df, DataFrame)
        converted_df = featurestore._return_dataframe_type(sample_df, constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS)
        assert isinstance(converted_df, pd.DataFrame)
        converted_df = featurestore._return_dataframe_type(sample_df, constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY)
        assert isinstance(converted_df, np.ndarray)
        converted_df = featurestore._return_dataframe_type(sample_df, constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON)
        assert isinstance(converted_df, list)

    def test_convert_dataframe_to_spark(self):
        """ Test for the _convert_dataframe_to_spark method """
        data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
        pandas_df = pd.DataFrame.from_dict(data)
        converted_pandas = featurestore._convert_dataframe_to_spark(pandas_df)
        assert converted_pandas.count() == len(pandas_df)
        assert len(converted_pandas.schema.fields) == len(pandas_df.columns)
        numpy_df = np.random.rand(50, 2)
        converted_numpy = featurestore._convert_dataframe_to_spark(numpy_df)
        assert converted_numpy.count() == len(numpy_df)
        assert len(converted_numpy.schema.fields) == numpy_df.shape[1]
        python_df = [[1, 2, 3], [1, 2, 3]]
        converted_python = featurestore._convert_dataframe_to_spark(python_df)
        assert converted_python.count() == len(python_df)
        assert len(converted_python.schema.fields) == len(python_df[0])
        numpy_df = np.random.rand(50, 2, 3)
        with pytest.raises(ValueError) as ex:
            featurestore._convert_dataframe_to_spark(numpy_df)
            assert "Cannot convert numpy array that do not have two dimensions to a dataframe." in ex.value

    def test_get_featuregroup(self):
        """ Test for get_featuregroup() method"""
        spark = self.spark_session()
        teams_fg_df = featurestore.get_featuregroup("teams_features")
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        assert teams_fg_df.count() == teams_features_df.count()
        games_fg_df = featurestore.get_featuregroup("games_features")
        games_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/games_features.csv")
        assert games_fg_df.count() == games_features_df.count()
        players_fg_df = featurestore.get_featuregroup("players_features")
        players_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/players_features.csv")
        assert players_fg_df.count() == players_features_df.count()
        season_scores_fg_df = featurestore.get_featuregroup("season_scores_features")
        season_scores_features_df = spark.read.format("csv").option("header", "true").option("inferSchema",
                                                                                             "true").load(
            "./hops/tests/test_resources/season_scores_features.csv")
        assert season_scores_fg_df.count() == season_scores_features_df.count()
        attendances_fg_df = featurestore.get_featuregroup("attendances_features")
        attendances_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/attendances_features.csv")
        assert attendances_fg_df.count() == attendances_features_df.count()
        spark_df = featurestore.get_featuregroup("attendances_features", dataframe_type="spark")
        assert isinstance(spark_df, DataFrame)
        python_df = featurestore.get_featuregroup("attendances_features", dataframe_type="python")
        assert isinstance(python_df, list)
        numpy_df = featurestore.get_featuregroup("attendances_features", dataframe_type="numpy")
        assert isinstance(numpy_df, np.ndarray)
        pandas_df = featurestore.get_featuregroup("attendances_features", dataframe_type="pandas")
        assert isinstance(pandas_df, pd.DataFrame)
        attendances_fg_df = featurestore.get_featuregroup("attendances_features", dataframe_type="spark",
                                                          featuregroup_version=2)
        assert attendances_fg_df.count() == attendances_features_df.count()
        with pytest.raises(pyspark.sql.utils.AnalysisException) as ex:
            featurestore.get_featuregroup("attendances_features", dataframe_type="spark", featuregroup_version=3)
            assert "Table or view not found: attendances_features_3" in ex.value
        with pytest.raises(pyspark.sql.utils.AnalysisException) as ex:
            featurestore.get_featuregroup("teams_features_spanish")
            assert "Table or view not found: teams_features_spanish_1" in ex.value
        teams_features_spanish_fg_df = featurestore.get_featuregroup("teams_features_spanish",
                                                                     featurestore="other_featurestore",
                                                                     featuregroup_version=1)
        assert teams_features_spanish_fg_df.count() == teams_features_df.count()

    def test_find_feature(self, sample_metadata):
        """ Test _find_feature"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        featuregroups = featurestore._parse_featuregroups_json(
            sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS])
        matched_fg = featurestore._find_feature("team_budget", featurestore.project_featurestore(), featuregroups)
        assert matched_fg[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] == "teams_features"
        with pytest.raises(AssertionError) as ex:
            featurestore._find_feature("team_id", featurestore.project_featurestore(), featuregroups)
            assert "Found the feature" in ex.value \
                   and "in more than one of the featuregroups of the featurestore" in ex.value
        with pytest.raises(AssertionError) as ex:
            featurestore._find_feature("non_existent_feature", featurestore.project_featurestore(), featuregroups)
            assert "Could not find the feature" in ex.value

    def test_do_get_feature(self, sample_metadata):
        """ Test _do_get_feature() method """
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        spark = self.spark_session()
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        df = featurestore._do_get_feature("team_budget", sample_metadata)
        assert df.count() == teams_features_df.count()
        assert len(df.schema.fields) == 1
        df = featurestore._do_get_feature("team_budget", sample_metadata, featuregroup_version=1,
                                          featuregroup="teams_features",
                                          featurestore=featurestore.project_featurestore())
        assert df.count() == teams_features_df.count()
        assert len(df.schema.fields) == 1
        with pytest.raises(AssertionError) as ex:
            featurestore._do_get_feature("feature_that_do_not_exist", sample_metadata)
            assert "Could not find any featuregroups in the metastore that contains the given feature" in ex.value

    def test_get_join_str(self, sample_metadata):
        """ Test for the method that constructs the join-string in the featurestore query planner"""
        all_featuregroups = featurestore._parse_featuregroups_json(
            sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS])
        select = ["attendances_features", "players_features", "season_scores_features", "teams_features"]
        featuregroups = list(filter(lambda x:
                                    x[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] in select and
                                    x[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] == 1, all_featuregroups))
        join_key = "team_id"
        join_str = featurestore._get_join_str(featuregroups, join_key)
        assert join_str == "JOIN attendances_features_1 JOIN players_features_1 JOIN teams_features_1 " \
                           "ON season_scores_features_1.`team_id`=attendances_features_1.`team_id` " \
                           "AND season_scores_features_1.`team_id`=players_features_1.`team_id` " \
                           "AND season_scores_features_1.`team_id`=teams_features_1.`team_id`"

    def test_get_join_col(self, sample_metadata):
        """ Test for the get_join_col in the query planner"""
        all_featuregroups = featurestore._parse_featuregroups_json(
            sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS])
        select = ["attendances_features", "players_features", "season_scores_features", "teams_features"]
        featuregroups = list(filter(lambda x:
                                    x[constants.REST_CONFIG.JSON_FEATUREGROUPNAME] in select and
                                    x[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION] == 1, all_featuregroups))
        join_col = featurestore._get_join_col(featuregroups)
        assert join_col == "team_id"

    def test_validate_metadata(self):
        """ Test the validate_metadata() function"""
        featurestore._validate_metadata("test",
                                        [('team_budget', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                        ["input1", "input2"],
                                        "description")
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test-",
                                            [('team_budget', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                            ["input1", "input2"],
                                            "description")
            assert "must match the regular expression: ^[a-zA-Z0-9_]+$" in ex.value
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test",
                                            [],
                                            ["input1", "input2"],
                                            "description")
            assert "Cannot create a feature group from an empty spark dataframe" in ex.value
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test",
                                            [('team_budget-', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                            ["input1", "input2"],
                                            "description")
            assert "must match the regular expression: ^[a-zA-Z0-9_]+$" in ex.value
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test",
                                            [('', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                            ["input1", "input2"],
                                            "description")
            assert "Name of feature column cannot be empty" in ex.value
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test",
                                            [('', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                            ["input1", "input1"],
                                            "description")
            assert "The list of data dependencies contains duplicates" in ex.value
        description = ''.join(choice(ascii_uppercase) for i in range(3000))
        with pytest.raises(ValueError) as ex:
            featurestore._validate_metadata("test",
                                            [('', 'float'), ('team_id', 'int'), ('team_position', 'int')],
                                            ["input1", "input1"],
                                            description)
            assert "Feature group/Training dataset description should " \
                   "not exceed the maximum length of 2000 characters" in ex.value

    def test_convert_featuregroup_version_dict(self):
        """ Test the convert_featuregroup_version_dict function"""
        featuregroups_version_dict = {
            "teams_features": 1,
            "attendances_features": 1,
            "players_features": 1
        }
        converted = featurestore._convert_featuregroup_version_dict(featuregroups_version_dict)
        assert len(converted) == len(featuregroups_version_dict)
        names = list(map(lambda x: x[constants.REST_CONFIG.JSON_FEATUREGROUPNAME], converted))
        versions = list(map(lambda x: x[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION], converted))
        assert set(names) == set(featuregroups_version_dict.keys())
        assert set(versions) == set(featuregroups_version_dict.values())

    def test_do_get_features(self, sample_metadata):
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        df = featurestore._do_get_features(["team_budget", "average_attendance", "average_player_age"], sample_metadata)
        assert df.count() > 0
        assert len(df.schema.fields) == 3
        df = featurestore._do_get_features(["teams_features_1.team_budget", "attendances_features_1.average_attendance",
                                            "players_features_1.average_player_age"], sample_metadata)
        assert df.count() > 0
        assert len(df.schema.fields) == 3
        df = featurestore._do_get_features(["teams_features_1.team_budget", "attendances_features_1.average_attendance",
                                            "players_features_1.average_player_age"],
                                           sample_metadata,
                                           featurestore=featurestore.project_featurestore(),
                                           featuregroups_version_dict={
                                               "teams_features": 1,
                                               "attendances_features": 1,
                                               "players_features": 1
                                           }
                                           )
        assert df.count() > 0
        assert len(df.schema.fields) == 3
        df = featurestore._do_get_features(["team_budget", "average_attendance", "average_player_age",
                                            "team_position", "sum_attendance",
                                            "average_player_rating", "average_player_worth", "sum_player_age",
                                            "sum_player_rating", "sum_player_worth", "sum_position",
                                            "average_position"
                                            ],
                                           sample_metadata)
        assert df.count() > 0
        assert len(df.schema.fields) == 12
        with pytest.raises(AssertionError) as ex:
            featurestore._do_get_features(["dummy_feature1", "dummy_feature2"],
                                          sample_metadata)
            assert "Could not find any featuregroups containing the features in the metastore" in ex.value

    def test_check_if_list_of_featuregroups_contains_featuregroup(self, sample_metadata):
        """ Test of the _check_if_list_of_featuregroups_contains_featuregroup function"""
        all_featuregroups = featurestore._parse_featuregroups_json(
            sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS])
        assert featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups, "games_features",
                                                                                  1)
        assert featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups,
                                                                                  "attendances_features", 1)
        assert featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups, "players_features",
                                                                                  1)
        assert featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups, "teams_features",
                                                                                  1)
        assert featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups,
                                                                                  "season_scores_features", 1)
        assert not featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups,
                                                                                      "season_scores_features", 2)
        assert not featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups,
                                                                                      "games_features", 2)
        assert not featurestore._check_if_list_of_featuregroups_contains_featuregroup(all_featuregroups, "dummy", 2)

    def test_sql(self):
        """ Test the sql interface to the feature store"""
        spark = self.spark_session()
        games_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/games_features.csv")
        df = featurestore.sql("SELECT * FROM games_features_1")
        assert df.count() == games_features_df.count()
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        df = featurestore.sql("SELECT * FROM teams_features_spanish_1", featurestore="other_featurestore")
        assert df.count() == teams_features_df.count()

    def test_write_featuregroup_hive(self):
        """ Test write_featuregroup_hive method"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        spark = self.spark_session()
        teams_features_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/teams_features.csv")
        featurestore._write_featuregroup_hive(teams_features_df, "teams_features", featurestore.project_featurestore(),
                                              1, "append")
        with pytest.raises(ValueError) as ex:
            featurestore._write_featuregroup_hive(teams_features_df, "teams_features",
                                                  featurestore.project_featurestore(), 1, "test")
            assert "The provided write mode test does not match the supported modes" in ex.value

    def test_update_featuregroup_stats_rest(self, sample_metadata):
        """ Test _update_featuregroup_stats_rest"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        featurestore_id = \
            sample_metadata[constants.REST_CONFIG.JSON_FEATURESTORE][constants.REST_CONFIG.JSON_FEATURESTORE_ID]
        featurestore._get_featurestore_id = mock.MagicMock(return_value=featurestore_id)
        featuregroup_id = 1
        featurestore._get_featuregroup_id = mock.MagicMock(return_value=featuregroup_id)
        with open("./hops/tests/test_resources/token.jwt", "r") as jwt:
            jwt = jwt.read()
        util.get_jwt = mock.MagicMock(return_value=jwt)
        os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = "1"
        connection.request = mock.MagicMock(return_value=True)
        response = mock.Mock()
        response.code = 200
        response.status = 200
        data = {}
        response.read = mock.MagicMock(return_value=json.dumps(data))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._update_featuregroup_stats_rest("test", featurestore.project_featurestore(), 1, None,
                                                              None, None, None)
        assert result == data
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._update_featuregroup_stats_rest("test", featurestore.project_featurestore(), 1,
                                                         None, None, None, None)
            assert "Could not update featuregroup stats" in ex.value

    def test_insert_into_featuregroup(self):
        """ Test insert_into_featuregroup"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        featurestore._update_featuregroup_stats_rest = mock.MagicMock(return_value=None)
        teams_features_df = featurestore.get_featuregroup("teams_features")
        old_count = teams_features_df.count()
        featurestore.insert_into_featuregroup(teams_features_df, "teams_features")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        assert teams_features_df.count() == (2 * old_count)

    def test_convert_spark_dtype_to_hive_dtype(self):
        """Test converstion between spark datatype and Hive datatype"""
        assert featurestore._convert_spark_dtype_to_hive_dtype("long") == "BIGINT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("LONG") == "BIGINT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("short") == "INT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("SHORT") == "INT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("byte") == "CHAR"
        assert featurestore._convert_spark_dtype_to_hive_dtype("BYTE") == "CHAR"
        assert featurestore._convert_spark_dtype_to_hive_dtype("integer") == "INT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("INTEGER") == "INT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("decimal(10,3)") == "DECIMAL(10,3)"
        assert featurestore._convert_spark_dtype_to_hive_dtype("DECIMAL(10,3)") == "DECIMAL(10,3)"
        assert featurestore._convert_spark_dtype_to_hive_dtype("DECIMAL(9,2)") == "DECIMAL(9,2)"
        assert featurestore._convert_spark_dtype_to_hive_dtype("decimal") == "DECIMAL"
        assert featurestore._convert_spark_dtype_to_hive_dtype("binary") == "BINARY"
        assert featurestore._convert_spark_dtype_to_hive_dtype("smallint") == "SMALLINT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("string") == "STRING"
        assert featurestore._convert_spark_dtype_to_hive_dtype("bigint") == "BIGINT"
        assert featurestore._convert_spark_dtype_to_hive_dtype("double") == "DOUBLE"
        assert featurestore._convert_spark_dtype_to_hive_dtype("float") == "FLOAT"
        assert featurestore._convert_spark_dtype_to_hive_dtype(
            {'containsNull': True, 'elementType': 'float', 'type': 'array'}) == "ARRAY<FLOAT>"
        assert featurestore._convert_spark_dtype_to_hive_dtype(
            {'fields': [{'metadata': {}, 'name': 'origin', 'nullable': True, 'type': 'string'},
                        {'metadata': {}, 'name': 'height', 'nullable': True, 'type': 'integer'},
                        {'metadata': {}, 'name': 'width', 'nullable': True, 'type': 'integer'},
                        {'metadata': {}, 'name': 'nChannels', 'nullable': True, 'type': 'integer'},
                        {'metadata': {}, 'name': 'mode', 'nullable': True, 'type': 'integer'},
                        {'metadata': {}, 'name': 'data', 'nullable': True, 'type': 'binary'}],
             'type': 'struct'}) == "STRUCT<origin:STRING,height:INT,width:INT,nChannels:INT,mode:INT,data:BINARY>"

    def test_convert_field_to_feature(self):
        """Tests the conversion of spark field to feature to save in NDB hopsworks"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        raw_schema = json.loads(teams_features_df.schema.json())
        raw_fields = raw_schema[constants.SPARK_CONFIG.SPARK_SCHEMA_FIELDS]
        primary_key = "team_id"
        parsed_feature = featurestore._convert_field_to_feature(raw_fields[0], primary_key)
        assert constants.REST_CONFIG.JSON_FEATURE_NAME in parsed_feature
        assert constants.REST_CONFIG.JSON_FEATURE_TYPE in parsed_feature
        assert constants.REST_CONFIG.JSON_FEATURE_DESCRIPTION in parsed_feature
        assert constants.REST_CONFIG.JSON_FEATURE_PRIMARY in parsed_feature
        assert parsed_feature[constants.REST_CONFIG.JSON_FEATURE_NAME] == "team_budget"

    def test_parse_spark_features_schema(self):
        """ Test parse_spark_features_schema into hopsworks schema"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        parsed_schema = featurestore._parse_spark_features_schema(teams_features_df.schema, "team_id")
        assert len(parsed_schema) == len(teams_features_df.dtypes)

    def test_filter_spark_df_numeric(self):
        """ Test _filter_spark_df_numeric """
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        num_columns = len(teams_features_df.dtypes)
        filtered_df = featurestore._filter_spark_df_numeric(teams_features_df)
        assert len(filtered_df.dtypes) == num_columns  # dataframe is only numeric so all columns should be left
        data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
        pandas_df = pd.DataFrame.from_dict(data)
        spark_df = featurestore._convert_dataframe_to_spark(pandas_df)
        filtered_spark_df = featurestore._filter_spark_df_numeric(spark_df)
        assert len(filtered_spark_df.dtypes) == 1  # should have dropped the string column

    def test_compute_corr_matrix(self):
        """ Test compute correlation matrix on a feature dataframe"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        num_columns = len(numeric_df.dtypes)
        corr_matrix = featurestore._compute_corr_matrix(numeric_df)
        assert corr_matrix.values.shape == (num_columns, num_columns)  # should be a square correlation matrix
        numeric_df = numeric_df.select("team_position")
        with pytest.raises(ValueError) as ex:
            featurestore._compute_corr_matrix(numeric_df)
            assert "The provided spark dataframe only contains one numeric column." in ex.value
        data = {'col_2': ['a', 'b', 'c', 'd']}
        pandas_df = pd.DataFrame.from_dict(data)
        spark_df = featurestore._convert_dataframe_to_spark(pandas_df)
        spark_df = featurestore._filter_spark_df_numeric(spark_df)
        with pytest.raises(ValueError) as ex:
            featurestore._compute_corr_matrix(spark_df)
            assert "The provided spark dataframe does not contain any numeric columns." in ex.value
        np_df = np.random.rand(100, 60)
        spark_df = featurestore._convert_dataframe_to_spark(np_df)
        with pytest.raises(ValueError) as ex:
            featurestore._compute_corr_matrix(spark_df)
            assert "due to scalability reasons (number of correlatons grows quadratically " \
                   "with the number of columns." in ex.value

    def test_compute_cluster_analysis(self):
        """ Test compute cluster analysis on a sample dataframe """
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        result = featurestore._compute_cluster_analysis(numeric_df, clusters=5)
        assert len(set(result["clusters"].values())) <= 5

    def test_compute_descriptive_statistics(self):
        """ Test compute descriptive statistics on a sample dataframe"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        result = featurestore._compute_descriptive_statistics(teams_features_df)
        assert len(result) > 0

    def test_is_type_numeric(self):
        """ Test _is_type_numeric """
        assert featurestore._is_type_numeric(('test', "bigint"))
        assert featurestore._is_type_numeric(('test', "BIGINT"))
        assert featurestore._is_type_numeric(('test', "float"))
        assert featurestore._is_type_numeric(('test', "long"))
        assert featurestore._is_type_numeric(('test', "int"))
        assert featurestore._is_type_numeric(('test', "decimal(10,3)"))
        assert not featurestore._is_type_numeric(('test', "string"))
        assert not featurestore._is_type_numeric(('test', "binary"))
        assert not featurestore._is_type_numeric(('test', "array<float>"))
        assert not featurestore._is_type_numeric(('test', "struct<float, int>"))

    def test_compute_feature_histograms(self):
        """ Test compute descriptive statistics on a sample dataframe"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        result = featurestore._compute_feature_histograms(numeric_df)
        assert len(result) > 0

    def test_compute_dataframe_stats(self):
        """ Test compute stats on a sample dataframe"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        feature_corr_data, desc_stats_data, features_histograms_data, cluster_analysis_data =  \
            featurestore._compute_dataframe_stats("teams_features", teams_features_df)
        assert feature_corr_data is not None
        assert desc_stats_data is not None
        assert features_histograms_data is not None

    def test_structure_descriptive_stats_json(self):
        """ Test _structure_descriptive_stats_json"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        result = featurestore._compute_descriptive_statistics(teams_features_df)
        featurestore._structure_descriptive_stats_json(result)

    def test_structure_cluster_analysis_json(self):
        """ Test _structure_cluster_analysis_json"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        result = featurestore._compute_cluster_analysis(numeric_df)
        featurestore._structure_cluster_analysis_json(result)

    def test_structure_feature_histograms_json(self):
        """ Test _structure_feature_histograms_json"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        result = featurestore._compute_feature_histograms(numeric_df)
        featurestore._structure_feature_histograms_json(result)

    def test_structure_feature_corr_json(self):
        """ Test _structure_feature_histograms_json"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        numeric_df = featurestore._filter_spark_df_numeric(teams_features_df)
        result = featurestore._compute_corr_matrix(numeric_df)
        featurestore._structure_feature_corr_json(result)

    def test_update_featuregroup_stats(self):
        """ Test update_featuregroup_stats"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        featurestore._update_featuregroup_stats_rest = mock.MagicMock(return_value=None)
        featurestore.update_featuregroup_stats("teams_features")

    def test_get_default_primary_key(self):
        """ Test _get_default_primary_key """
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        assert featurestore._get_default_primary_key(teams_features_df) == "team_budget"

    def test_validate_primary_key(self):
        """ Test _validate_primary_key"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("teams_features")
        assert featurestore._validate_primary_key(teams_features_df, "team_budget")
        assert featurestore._validate_primary_key(teams_features_df, "team_id")
        assert featurestore._validate_primary_key(teams_features_df, "team_position")
        with pytest.raises(AssertionError) as ex:
            featurestore._validate_primary_key(teams_features_df, "wrong_key")
            assert "Invalid primary key" in ex.value

    def test_delete_table_contents(self):
        """ Test _delete_table_contents"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        connection.request = mock.MagicMock(return_value=True)
        response = mock.Mock()
        response.status = 200
        response.code = 200
        data = {}
        response.read = mock.MagicMock(return_value=json.dumps(data))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._delete_table_contents(featurestore.project_featurestore(), "test", 1)
        assert result == data
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._delete_table_contents(featurestore.project_featurestore(), "test", 1)
            assert "Could not clear featuregroup contents" in ex.value

    def test_get_featurestores(self):
        """ Test _get_featurestores"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        with open("./hops/tests/test_resources/token.jwt", "r") as jwt:
            jwt = jwt.read()
        util.get_jwt = mock.MagicMock(return_value=jwt)
        connection.request = mock.MagicMock(return_value=True)
        os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = "1"
        response = mock.Mock()
        response.status = 200
        response.code = 200
        data = {}
        response.read = mock.MagicMock(return_value=json.dumps(data))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._get_featurestores()
        assert result == data
        response.status = 200
        response.code = 500
        result = featurestore._get_featurestores()
        assert result == data
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._get_featurestores()
            assert "Could not fetch feature stores" in ex.value

    def test_create_featuregroup_rest(self, sample_metadata):
        """ Test _create_featuregroup_rest"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        spark = self.spark_session()
        spark_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            "./hops/tests/test_resources/games_features.csv")
        features_schema = featurestore._parse_spark_features_schema(spark_df.schema, None)
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        connection.request = mock.MagicMock(return_value=True)
        response = mock.Mock()
        response.code = 201
        response.status = 201
        response.read = mock.MagicMock(
            return_value=json.dumps(sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS][0]))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._create_featuregroup_rest("test", featurestore.project_featurestore(), "",
                                                        1, None, [], features_schema,
                                                        None, None, None, None)
        assert result == sample_metadata[constants.REST_CONFIG.JSON_FEATUREGROUPS][0]
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._create_featuregroup_rest("test", featurestore.project_featurestore(), "",
                                                   1, None, [], features_schema,
                                                   None, None, None, None)
            assert "Could not create feature group" in ex.value

    def test_create_featuregroup(self, sample_metadata):
        """ Test create_featuregroup"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        featurestore._create_featuregroup_rest = mock.MagicMock(return_value=None)
        featurestore._get_featurestore_metadata = mock.MagicMock(return_value=sample_metadata)
        teams_features_df = featurestore.get_featuregroup("teams_features")
        featurestore.create_featuregroup(teams_features_df, "teams_features")

    def test_get_featurestore_metadata(self, sample_metadata):
        """ Test get_featurestore_metadata"""
        featurestore._get_featurestore_metadata = mock.MagicMock(return_value=sample_metadata)
        assert featurestore.get_featurestore_metadata() == sample_metadata

    def test_do_get_featuregroups(self, sample_metadata):
        """ Test do_get_featuregroups"""
        result = featurestore._do_get_featuregroups(sample_metadata)
        assert len(result) == 5
        assert set(result) == set(["games_features_1", "players_features_1", "season_scores_features_1",
                                   "attendances_features_1", "teams_features_1"])

    def test_do_get_features_list(self, sample_metadata):
        """ Test do_get_features_list"""
        result = featurestore._do_get_features_list(sample_metadata)
        assert len(result) == 19
        assert set(result) == set(['away_team_id', 'home_team_id', 'score', 'average_position',
                                   'sum_position', 'team_id', 'average_attendance', 'sum_attendance',
                                   'team_id', 'average_player_age', 'average_player_rating',
                                   'average_player_worth', 'sum_player_age', 'sum_player_rating',
                                   'sum_player_worth', 'team_id',
                                   'team_budget', 'team_id', 'team_position'])

    def test_get_project_featurestores(self, sample_featurestores):
        """ Test get_project_featurestores()"""
        featurestore._get_featurestores = mock.MagicMock(return_value=sample_featurestores)
        result = featurestore.get_project_featurestores()
        assert len(result) == 1

    def test_get_dataframe_tf_record_schema_json(self):
        """ Test get_dataframe_tf_record_schema_json"""
        spark = self.spark_session()
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("players_features")
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(teams_features_df)
        assert tf_schema == {'team_id': tf.FixedLenFeature(shape=[], dtype=tf.int64, default_value=None),
                             'average_player_rating': tf.FixedLenFeature(shape=[], dtype=tf.float32,
                                                                         default_value=None),
                             'average_player_age': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                             'average_player_worth': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                             'sum_player_rating': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                             'sum_player_age': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                             'sum_player_worth': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None)}
        assert json_schema == {'team_id': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                               constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                           constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                               constants.FEATURE_STORE.TF_RECORD_INT_TYPE,},
                               'average_player_rating': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                             constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                         constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                             constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE,},
                               'average_player_age': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                          constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                      constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                          constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE},
                               'average_player_worth': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                            constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                            constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE},
                               'sum_player_rating': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                         constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                     constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                         constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE},
                               'sum_player_age': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                      constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                  constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                      constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE},
                               'sum_player_worth': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                                        constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                                    constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                                        constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE}
                               }
        # Test that tf record schema can be inferred correctly with array types
        sqlContext = SQLContext(spark.sparkContext)
        schema = StructType([StructField("val", ArrayType(FloatType()), True)
                             ])
        sample_df = sqlContext.createDataFrame([{'val': [1.0,2.0,3.0,4.0]},
                                                {'val': [5.0,6.0,7.0,8.0]},
                                                {'val': [9.0,10.0,11.0,12.0]},
                                                {'val': [13.0,14.0,15.0,16.0]},
                                                {'val': [17.0,18.0,19.0,20.0]}], schema)
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(sample_df)
        assert tf_schema == {'val': tf.FixedLenFeature(shape=[4], dtype=tf.float32, default_value=None)}
        assert json_schema == {'val': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                           constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                       constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                           constants.FEATURE_STORE.TF_RECORD_FLOAT_TYPE,
                                       constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [4]}}
        # Test that the tf.type is correct
        schema = StructType([StructField("val", ArrayType(IntegerType()), True)
                             ])
        sample_df = sqlContext.createDataFrame([{'val': [1,2,3,4]},
                                                {'val': [5,6,7,8]},
                                                {'val': [9,10,11,12]},
                                                {'val': [13,14,15,16]},
                                                {'val': [17,18,19,20]}], schema)
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(sample_df)
        assert tf_schema == {'val': tf.FixedLenFeature(shape=[4], dtype=tf.int64, default_value=None)}
        assert json_schema == {'val': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                           constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_FIXED,
                                       constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                           constants.FEATURE_STORE.TF_RECORD_INT_TYPE,
                                       constants.FEATURE_STORE.TF_RECORD_SCHEMA_SHAPE: [4]}}
        # Test that variable length arrays schemas are correctly inferred
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(sample_df, fixed=False)
        assert tf_schema == {'val': tf.VarLenFeature(tf.int64)}
        assert json_schema == {'val': {constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE:
                                           constants.FEATURE_STORE.TF_RECORD_SCHEMA_FEATURE_VAR,
                                       constants.FEATURE_STORE.TF_RECORD_SCHEMA_TYPE:
                                           constants.FEATURE_STORE.TF_RECORD_INT_TYPE}}


    def test_convert_tf_record_schema_json(self):
        """ Test convert_tf_record_schema_json"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        teams_features_df = featurestore.get_featuregroup("players_features")
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(teams_features_df)
        assert featurestore._convert_tf_record_schema_json_to_dict(json_schema) == tf_schema

    def test_store_tf_record_schema_hdfs(self):
        """ Test store_tf_record_schema_hdfs"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        hdfs.dump = mock.MagicMock(return_value=None)
        teams_features_df = featurestore.get_featuregroup("players_features")
        tf_schema, json_schema = featurestore._get_dataframe_tf_record_schema_json(teams_features_df)
        featurestore._store_tf_record_schema_hdfs(json_schema, "./schema.json")

    def test_find_training_dataset(self, sample_metadata):
        """ Test _find_training_dataset """
        training_datasets = sample_metadata[constants.REST_CONFIG.JSON_TRAINING_DATASETS]
        td = featurestore._find_training_dataset(training_datasets, "team_position_prediction", 1)
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] == "team_position_prediction"
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] == 1
        td = featurestore._find_training_dataset(training_datasets, "team_position_prediction", 2)
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] == "team_position_prediction"
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] == 2
        td = featurestore._find_training_dataset(training_datasets, "team_position_prediction_parquet", 1)
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_NAME] == "team_position_prediction_parquet"
        assert td[constants.REST_CONFIG.JSON_TRAINING_DATASET_VERSION] == 1
        with pytest.raises(AssertionError) as ex:
            featurestore._find_training_dataset(training_datasets, "team_position_prediction_parquet", 2)
            assert "Could not find the requested training dataset" in ex.value
        with pytest.raises(AssertionError) as ex:
            featurestore._find_training_dataset(training_datasets, "non_existent", 1)
            assert "Could not find the requested training dataset" in ex.value

    def test_do_get_latest_training_dataset_version(self, sample_metadata):
        """ Test _do_get_latest_training_dataset_version """
        version = featurestore._do_get_latest_training_dataset_version("team_position_prediction", sample_metadata)
        assert version == 2
        version = featurestore._do_get_latest_training_dataset_version("team_position_prediction_parquet",
                                                                       sample_metadata)
        assert version == 1

    def test_do_get_featuregroup_version(self, sample_metadata):
        """ Test _do_get_featuregroup_version """
        version = featurestore._do_get_latest_featuregroup_version("games_features", sample_metadata)
        assert version == 1
        version = featurestore._do_get_latest_featuregroup_version("players_features", sample_metadata)
        assert version == 1

    def test_update_training_dataset_stats_rest(self, sample_metadata):
        """ Test _update_training_dataset_stats_rest"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        connection.request = mock.MagicMock(return_value=True)
        response = mock.Mock()
        response.code = 200
        response.status = 200
        featurestore_id = \
            sample_metadata[constants.REST_CONFIG.JSON_FEATURESTORE][constants.REST_CONFIG.JSON_FEATURESTORE_ID]
        featurestore._get_featurestore_id = mock.MagicMock(return_value=featurestore_id)
        training_dataset_id = 1
        featurestore._get_training_dataset_id = mock.MagicMock(return_value=training_dataset_id)
        with open("./hops/tests/test_resources/token.jwt", "r") as jwt:
            jwt = jwt.read()
        util.get_jwt = mock.MagicMock(return_value=jwt)
        data = {}
        response.read = mock.MagicMock(return_value=json.dumps(data))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._update_training_dataset_stats_rest("test", featurestore.project_featurestore(), 1, [],
                                                                  None, None, None, None)
        assert result == data
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._update_training_dataset_stats_rest("test", featurestore.project_featurestore(), 1, [],
                                                             None, None, None, None)
            assert "Could not update training dataset stats" in ex.value

    def test_update_training_dataset_stats(self):
        """ Test _do_get_featuregroup_version"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        spark = self.spark_session()
        df = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore.get_training_dataset = mock.MagicMock(return_value=df)
        featurestore._update_training_dataset_stats_rest = mock.MagicMock(return_value=None)
        featurestore.update_featuregroup_stats("teams_features")

    def test_do_get_training_dataset_tf_record_schema(self, sample_metadata):
        """ Test _do_get_training_dataset_tf_record_schema """
        with open("./training_datasets/schema.json") as f:
            schema_json = json.load(f)
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/schema.json")
        hdfs.load = mock.MagicMock(return_value=json.dumps(schema_json))
        result = featurestore._do_get_training_dataset_tf_record_schema("team_position_prediction", sample_metadata,
                                                                        training_dataset_version=1)
        assert result == {'team_budget': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'average_position': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'sum_player_rating': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'average_attendance': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'average_player_worth': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'sum_player_worth': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'sum_position': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'sum_attendance': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'average_player_rating': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'team_position': tf.FixedLenFeature(shape=[], dtype=tf.int64, default_value=None),
                          'sum_player_age': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
                          'average_player_age': tf.FixedLenFeature(shape=[], dtype=tf.float32, default_value=None)}
        with pytest.raises(AssertionError) as ex:
            featurestore._do_get_training_dataset_tf_record_schema("team_position_prediction_parquet", sample_metadata,
                                                                   training_dataset_version=1)
            assert "Cannot fetch tf records schema for a training dataset " \
                   "that is not stored in tfrecords format" in ex.value

    def test_do_get_training_datasets(self, sample_metadata):
        """ Test do_get_training_datasets"""
        result = featurestore._do_get_training_datasets(sample_metadata)
        assert len(result) == 7
        assert set(result) == set(['team_position_prediction_1', 'team_position_prediction_csv_1',
                                   'team_position_prediction_tsv_1', 'team_position_prediction_parquet_1',
                                   'team_position_prediction_hdf5_1', 'team_position_prediction_npy_1',
                                   'team_position_prediction_2'])

    def test_do_get_training_dataset_tsv(self):
        """ Test _do_get_training_dataset_tsv"""
        spark = self.spark_session()
        df_compare = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
            constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER).load(
            "./training_datasets/team_position_prediction_tsv_1")
        df = featurestore._do_get_training_dataset_tsv("./training_datasets/team_position_prediction_tsv_1",
                                                       constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()

    def test_do_get_training_dataset_parquet(self):
        """ Test _do_get_training_dataset_parquet"""
        spark = self.spark_session()
        df_compare = spark.read.parquet("./training_datasets/team_position_prediction_parquet_1")
        df = featurestore._do_get_training_dataset_parquet("./training_datasets/team_position_prediction_parquet_1",
                                                           constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()


    def test_do_get_training_dataset_avro(self):
        """ Test _do_get_training_dataset_avro"""
        spark = self.spark_session()
        df_compare = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT) \
            .load("./training_datasets/team_position_prediction_avro_1")
        df = featurestore._do_get_training_dataset_avro("./training_datasets/team_position_prediction_avro_1",
                                                        constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()


    def test_do_get_training_dataset_orc(self):
        """ Test _do_get_training_dataset_orc"""
        spark = self.spark_session()
        df_compare = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT) \
            .load("./training_datasets/team_position_prediction_orc_1")
        df = featurestore._do_get_training_dataset_orc("./training_datasets/team_position_prediction_orc_1",
                                                       constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()

    def test_do_get_training_dataset_image(self):
        """ Test _do_get_training_dataset_image"""
        spark = self.spark_session()
        df_compare = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT) \
            .load("./hops/tests/test_resources/mnist")
        df = featurestore._do_get_training_dataset_image("./hops/tests/test_resources/mnist",
                                                         constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()

    def test_do_get_training_dataset_tfrecords(self):
        """ Test _do_get_training_dataset_tfrecords"""
        spark = self.spark_session()
        df_compare = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        df = featurestore._do_get_training_dataset_tfrecords("./training_datasets/team_position_prediction_1",
                                                             constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert df.count() == df_compare.count()

    def test_do_get_training_dataset(self, sample_metadata):
        """ Test _do_get_training_dataset """
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_1")
        hdfs.exists = mock.MagicMock(return_value=True)
        spark = self.spark_session()
        df_compare = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata)
        assert df.count() == df_compare.count()
        df_compare = spark.read.parquet("./training_datasets/team_position_prediction_parquet_1")
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_parquet_1")
        df = featurestore._do_get_training_dataset("team_position_prediction_parquet", sample_metadata)
        assert df.count() == df_compare.count()
        df_compare = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
            constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER).load(
            "./training_datasets/team_position_prediction_csv_1")
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_csv_1")
        df = featurestore._do_get_training_dataset("team_position_prediction_csv", sample_metadata)
        assert df.count() == df_compare.count()
        df_compare = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
            constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER).load(
            "./training_datasets/team_position_prediction_tsv_1")
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_tsv_1")
        df = featurestore._do_get_training_dataset("team_position_prediction_tsv", sample_metadata)
        assert df.count() == df_compare.count()
        df_compare = np.load(
            "./training_datasets/team_position_prediction_npy_1" +
            constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        with open("./training_datasets/team_position_prediction_npy_1" +
                          constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX,'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)
        pydoop.path.abspath = mock.MagicMock(
            return_value="./training_datasets/team_position_prediction_npy_1" +
                         constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        df = featurestore._do_get_training_dataset("team_position_prediction_npy", sample_metadata)
        assert df.count() == len(df_compare)
        hdf5_file = h5py.File(
            "./training_datasets/team_position_prediction_hdf5_1" +
            constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        df_compare = hdf5_file["team_position_prediction_hdf5"][()]
        with open("./training_datasets/team_position_prediction_hdf5_1" +
                          constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX,
                'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)
        pydoop.path.abspath = mock.MagicMock(
            return_value="./training_datasets/team_position_prediction_hdf5_1" +
                         constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        df = featurestore._do_get_training_dataset("team_position_prediction_hdf5", sample_metadata)
        assert df.count() == len(df_compare)
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_1")
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata,
                                                   dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK)
        assert isinstance(df, DataFrame)
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata,
                                                   dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS)
        assert isinstance(df, pd.DataFrame)
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata,
                                                   dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY)
        assert isinstance(df, np.ndarray)
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata,
                                                   dataframe_type=constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON)
        assert isinstance(df, list)
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/non_existent")
        with pytest.raises(AssertionError) as ex:
            featurestore._do_get_training_dataset("non_existent", sample_metadata)
            assert "Could not find the requested training dataset" in ex.value

    def test_write_training_dataset_csv(self):
        """ Test _write_training_dataset_csv"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_csv(df_1,
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "./training_datasets/test_write_hdfs_csv" +
                                                      constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true"
        ).option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER
                 ).load("./training_datasets/test_write_hdfs_csv" + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_tsv(self):
        """ Test _write_training_dataset_tsv"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_tsv(df_1,
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "./training_datasets/test_write_hdfs_tsv" +
                                                      constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true"
        ).option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER
                 ).load("./training_datasets/test_write_hdfs_tsv" +
                        constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_parquet(self):
        """ Test _write_training_dataset_parquet"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_parquet(df_1,
                                                          constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                          "./training_datasets/test_write_hdfs_parquet" +
                                                          constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        df_2 = spark.read.parquet(
            "./training_datasets/test_write_hdfs_parquet" +
            constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_orc(self):
        """ Test _write_training_dataset_orc"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_orc(df_1,
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "./training_datasets/test_write_hdfs_orc" +
                                                      constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT).load(
            "./training_datasets/test_write_hdfs_orc" + constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_avro(self):
        """ Test _write_training_dataset_avro"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_avro(df_1,
                                                       constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                       "./training_datasets/test_write_hdfs_avro" +
                                                       constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT).load(
            "./training_datasets/test_write_hdfs_avro" + constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_tfrecords(self):
        """ Test _write_training_dataset_tfrecords"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs_tfrecords(df_1,
                                                            constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                            "./training_datasets/test_write_hdfs_tfrecords" +
                                                            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE
        ).load(
            "./training_datasets/test_write_hdfs_tfrecords" + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_npy(self):
        """ Test _write_training_dataset_npy"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        np.save("./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX,
                featurestore._return_dataframe_type(df_1, constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY))
        with open("./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX,
                  'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)

        def hdfs_dump_side_effect(data, path):
            """ This function is called when hdfs.dump() is called inside the featurestore module"""
            with open(path, 'wb') as f:
                f.write(data)

        hdfs.dump = mock.MagicMock(side_effect=hdfs_dump_side_effect)
        featurestore._write_training_dataset_hdfs_npy(df_1,
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "./training_datasets/test_write_hdfs_npy"
                                                      )
        df_2 = np.load("./training_datasets/test_write_hdfs_npy" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        assert df_1.count() == len(df_2)

    def test_write_training_dataset_hdf5(self):
        """ Test _write_training_dataset_hdf5"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        hdf5_file = h5py.File(
            "./training_datasets/write_hdfs_test_ref_hdf5" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        hdf5_file.create_dataset("write_hdfs_test_ref_hdf5" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX,
                                 data=featurestore._return_dataframe_type(df_1,
                                                                          constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY))
        with open("./training_datasets/write_hdfs_test_ref_hdf5" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX,
                  'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)

        def hdfs_dump_side_effect(data, path):
            """ This function is called when hdfs.dump() is called inside the featurestore module"""
            with open(path, 'wb') as f:
                f.write(data)

        hdfs.dump = mock.MagicMock(side_effect=hdfs_dump_side_effect)
        featurestore._write_training_dataset_hdfs_hdf5(df_1,
                                                       constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "./training_datasets/test_write_hdfs_hdf5",
                                                  "test_write_hdfs_hdf5" +
                                                       constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        hdf5_file = h5py.File(
            "./training_datasets/write_hdfs_test_ref_hdf5" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        df_2 = hdf5_file["write_hdfs_test_ref_hdf5" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX][()]
        assert df_1.count() == len(df_2)

    def test_write_training_dataset_petastorm(self):
        """ Test _write_training_dataset_petastorm"""
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        PetastormSchema = Unischema('team_position_prediction_petastorm_schema', [
            UnischemaField('team_budget', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('average_position', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('sum_player_rating', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('average_attendance', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('average_player_worth', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('sum_player_worth', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('sum_position', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('average_player_rating', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('team_position', np.int32, (), ScalarCodec(IntegerType()), False),
            UnischemaField('sum_player_age', np.float32, (), ScalarCodec(FloatType()), False),
            UnischemaField('average_player_age', np.float32, (), ScalarCodec(FloatType()), False),
        ])
        petastorm_args = {
            "schema": PetastormSchema,
            "pyarrow_filesystem": None
        }
        path = "file://" + os.getcwd() + "/training_datasets/test_write_hdfs_petastorm" + \
               constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX
        featurestore._write_training_dataset_hdfs_petastorm(df_1,
                                                            constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                            path,
                                                            petastorm_args)

        df_2 = spark.read.parquet(
            "./training_datasets/test_write_hdfs_petastorm" + constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX)
        assert df_1.count() == df_2.count()

    def test_write_training_dataset_hdfs(self):
        """ Test _write_training_dataset_hdfs """
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/team_position_prediction_1")
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX,
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        df_2 = spark.read.parquet("./training_datasets/test_write_hdfs" +
                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        assert df_1.count() == df_2.count()
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX,
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        df_2 = spark.read.parquet(
            "./training_datasets/test_write_hdfs" + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
        assert df_1.count() * 2 == df_2.count()
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX,
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE
        ).load("./training_datasets/test_write_hdfs" + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
        assert df_1.count() == df_2.count()
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX,
                                                  constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true"
        ).option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER
                 ).load("./training_datasets/test_write_hdfs" + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
        assert df_1.count() == df_2.count()
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX,
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
        df_2 = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT) \
            .option(constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true") \
            .option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER) \
            .load("./training_datasets/test_write_hdfs" + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
        assert df_1.count() == df_2.count()
        np.save("./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX,
                featurestore._return_dataframe_type(df_1, constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY))
        with open("./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX,
                  'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)

        def hdfs_dump_side_effect(data, path):
            """ This function is called when hdfs.dump() is called inside the featurestore module"""
            with open(path, 'wb') as f:
                f.write(data)

        hdfs.dump = mock.MagicMock(side_effect=hdfs_dump_side_effect)
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs",
                                                  constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        df_2 = np.load("./training_datasets/test_write_hdfs" + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        assert df_1.count() == len(df_2)
        hdf5_file = h5py.File(
            "./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        hdf5_file.create_dataset("write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX,
                                 data=featurestore._return_dataframe_type(df_1,
                                                                          constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY))
        with open("./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX,
                  'rb') as f:
            data = f.read()
        hdfs.load = mock.MagicMock(return_value=data)

        def hdfs_dump_side_effect(data, path):
            """ This function is called when hdfs.dump() is called inside the featurestore module"""
            with open(path, 'wb') as f:
                f.write(data)

        hdfs.dump = mock.MagicMock(side_effect=hdfs_dump_side_effect)
        featurestore._write_training_dataset_hdfs(df_1,
                                                  "./training_datasets/test_write_hdfs",
                                                  constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT,
                                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                  "test_write_hdfs" +
                                                  constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        hdf5_file = h5py.File(
            "./training_datasets/write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        df_2 = hdf5_file["write_hdfs_test_ref" + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX][()]
        assert df_1.count() == len(df_2)
        with pytest.raises(AssertionError) as ex:
            featurestore._write_training_dataset_hdfs(df_1,
                                                      "./training_datasets/test_write_hdfs",
                                                      "non_existent_format",
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "test_write_hdfs")
            assert "Can not write dataframe in image format" in ex.value
        with pytest.raises(AssertionError) as ex:
            featurestore._write_training_dataset_hdfs(df_1,
                                                      "./training_datasets/test_write_hdfs",
                                                      "image",
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE,
                                                      "test_write_hdfs")
            assert "Invalid data format to materialize training dataset." in ex.value

    def test_create_training_dataset_rest(self):
        """ Test _create_training_dataset_rest"""
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        connection = mock.Mock()
        util._get_http_connection = mock.MagicMock(return_value=connection)
        connection.request = mock.MagicMock(return_value=True)
        response = mock.Mock()
        response.code = 201
        response.status = 201
        data = {}
        response.read = mock.MagicMock(return_value=json.dumps(data))
        connection.getresponse = mock.MagicMock(return_value=response)
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        tls._prepare_rest_appservice_json_request = mock.MagicMock(return_value={})
        result = featurestore._create_training_dataset_rest("test", featurestore.project_featurestore(), "", 1,
                                                            "", "", [], [], None, None, None, None)
        assert result == data
        response.code = 500
        response.status = 500
        with pytest.raises(AssertionError) as ex:
            featurestore._create_training_dataset_rest("test", featurestore.project_featurestore(), "", 1,
                                                       "", "", [], [], None, None, None, None)
            assert "Could not create training dataset" in ex.value

    def test_create_training_dataset(self, sample_metadata):
        """ Test _create_training_dataset """
        hdfs.project_name = mock.MagicMock(return_value="test_project")
        df = featurestore.get_featuregroup("players_features")
        featurestore._create_training_dataset_rest = mock.MagicMock(return_value={
            constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH: "./training_datasets/test_create_td"})
        featurestore._get_featurestore_metadata = mock.MagicMock(return_value=sample_metadata)
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/test_create_td")
        featurestore.create_training_dataset(df, "test_create_training_dataset",
                                             data_format=constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT)
        spark = self.spark_session()
        df_1 = spark.read.format(
            constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
            "./training_datasets/test_create_td/test_create_training_dataset")
        assert df_1.count() == df.count()

    def test_do_insert_into_training_dataset(self, sample_metadata):
        """ Test _do_insert_into_training_dataset """
        pydoop.path.abspath = mock.MagicMock(return_value="./training_datasets/team_position_prediction_1")
        hdfs.exists = mock.MagicMock(return_value=True)
        df = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata)
        old_count = df.count()
        featurestore._update_training_dataset_stats_rest = mock.MagicMock(return_value={
            constants.REST_CONFIG.JSON_TRAINING_DATASET_HDFS_STORE_PATH:
                "./training_datasets/team_position_prediction_1"})
        featurestore._do_insert_into_training_dataset(df, "team_position_prediction", sample_metadata,
                                                      write_mode=
                                                      constants.FEATURE_STORE.FEATURE_GROUP_INSERT_OVERWRITE_MODE)
        pydoop.path.abspath = mock.MagicMock(
            return_value="./training_datasets/team_position_prediction_1/team_position_prediction")
        df_2 = featurestore._do_get_training_dataset("team_position_prediction", sample_metadata)
        new_count = df_2.count()
        assert old_count == new_count
        with pytest.raises(AssertionError) as ex:
            featurestore._do_insert_into_training_dataset(df, "team_position_prediction", sample_metadata,
                                                          write_mode=
                                                          constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE)
            assert "Append is not supported for training datasets stored in tf-records format" in ex.value
