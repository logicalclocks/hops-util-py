from hops import hdfs, constants, util
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.exceptions.exceptions import TrainingDatasetNotFound, CouldNotConvertDataframe, \
    NumpyDatasetFormatNotSupportedForExternalTrainingDatasets, HDF5DatasetFormatNotSupportedForExternalTrainingDatasets
from tempfile import TemporaryFile
import pandas as pd
import numpy as np
import pyarrow as pa
import abc

# for backwards compatibility
try:
    import h5py
except:
    pass

# in case importing in %%local
try:
    from pyspark.sql import SQLContext, DataFrame
    from pyspark.rdd import RDD
    from petastorm.etl.dataset_metadata import materialize_dataset
except:
    pass

class FeatureFrame(object):
    """
    Abstract feature frame (a dataframe of feature data to be saved to the feature store)
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        """
        Initialize state with information for reading/writing the featureframe

        Args:
            :kwargs: key-value arguments to set the object variables
        """
        self.__dict__.update(kwargs)


    @staticmethod
    def get_featureframe(**kwargs):
        """
        Returns the appropriate FeatureFrame subclass depending on the data format

        Args:
            :kwargs: key-value arguments with the featureframe state (must contain data_format key)

        Returns:
            FeatureFrame implementation

        Raises:
              :ValueError: if the requested featureframe type could is not supported
        """
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT:
            return CSVFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_TSV_FORMAT:
            return TSVFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT:
            return ParquetFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT:
            return AvroFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT:
            return ORCFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT:
            return ImageFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_FORMAT or \
                kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_TFRECORD_FORMAT:
            return TFRecordsFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
            return NumpyFeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
            return HDF5FeatureFrame(**kwargs)
        if kwargs["data_format"] == constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_FORMAT:
            return PetastormFeatureFrame(**kwargs)
        if kwargs["data_format"] not in constants.FEATURE_STORE.TRAINING_DATASET_SUPPORTED_FORMATS:
            raise ValueError(
                "invalid data format to materialize training dataset. The provided format: {} "
                "is not in the list of supported formats: {}".format(
                    kwargs["data_format"], ",".join(constants.FEATURE_STORE.TRAINING_DATASET_SUPPORTED_FORMATS)))


    @abc.abstractmethod
    def read_featureframe(self):
        """
        Abstract method for reading a featureframe as a training dataset in HopsFS
        Implemented by subclasses
        """
        pass


    @abc.abstractmethod
    def write_featureframe(self):
        """
        Abstract method for writing a featureframe as a training dataset in HopsFS
        Implemented by subclasses
        """
        pass



class AvroFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for Avro training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(AvroFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in avro format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
                        self.training_dataset.training_dataset_type != \
                        constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT).load(
                    self.path + constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_AVRO_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT).load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)

    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the avro format

        Returns:
            None

        """
        self.df.write.mode(self.write_mode).format(constants.FEATURE_STORE.TRAINING_DATASET_AVRO_FORMAT).save(self.path)


class ORCFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for ORC training datasets
    """

    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(ORCFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in orc format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
                        self.training_dataset.training_dataset_type != \
                        constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT).load(
                    self.path + constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_ORC_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT).load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the orc format

        Returns:
            None
        """
        self.df.write.mode(self.write_mode).format(constants.FEATURE_STORE.TRAINING_DATASET_ORC_FORMAT).save(self.path)


class TFRecordsFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for TFRecords training datasets
    """

    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(TFRecordsFeatureFrame, self).__init__(**kwargs)
        self._spark_tfrecord_format = kwargs["data_format"]

    def read_featureframe(self, spark):
        """
        Reads a training dataset in tfrecords format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and self.training_dataset.training_dataset_type != \
                constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(self._spark_tfrecord_format).option(
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX):
                spark_df = spark.read.format(self._spark_tfrecord_format).option(
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(
                    self.path + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(
                            self.path + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_TFRECORDS_SUFFIX))
        else:
            spark_df = spark.read.format(self._spark_tfrecord_format).option(
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
                constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the tfrecords format

        Returns:
            None

        Raises:
              :ValueError: if the user supplied a write mode that is not supported
        """
        self.df.write.format(self._spark_tfrecord_format).option(
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE,
            constants.SPARK_CONFIG.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(self.write_mode).save(self.path)


class NumpyFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for Numpy training datasets
    """

    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(NumpyFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in numpy format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
              :CouldNotConvertDataframe: if the numpy dataset could not be converted to a spark dataframe
              :NumpyDatasetFormatNotSupportedForExternalTrainingDatasets: if the user tries to read an
                                                                          external training dataset in the .npy format.
        """
        if not hasattr(self, 'training_dataset') or \
                        self.training_dataset.training_dataset_type \
                        == constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            raise NumpyDatasetFormatNotSupportedForExternalTrainingDatasets("The .npy dataset format is not "
                                                                            "supported for external training datasets.")
        if not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX):
            raise TrainingDatasetNotFound("Could not find a training dataset in file {}".format(
                self.path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX))
        tf = TemporaryFile()
        data = hdfs.load(self.path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)
        tf.write(data)
        tf.seek(0)
        np_array = np.load(tf)
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
            return np_array
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
            return np_array.tolist()
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK or \
                        self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
            if np_array.ndim != 2:
                raise CouldNotConvertDataframe(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                    "The number of dimensions are: {}".format(np_array.ndim))
            num_cols = np_array.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = np_array[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            sc = spark.sparkContext
            sql_context = SQLContext(sc)
            return fs_utils._return_dataframe_type(sql_context.createDataFrame(pandas_df), self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the npy format

        Returns:
            None

        Raises:
              :ValueError: if the user supplied a write mode that is not supported
              :NumpyDatasetFormatNotSupportedForExternalTrainingDatasets: if the user tries to write an
                                                                          external training dataset in the .npy format.
        """
        if not hasattr(self, 'training_dataset') or \
                        self.training_dataset.training_dataset_type \
                        == constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            raise NumpyDatasetFormatNotSupportedForExternalTrainingDatasets("The .npy dataset format is not "
                                                                            "supported for external training datasets.")
        if (self.write_mode == constants.SPARK_CONFIG.SPARK_APPEND_MODE):
            raise ValueError(
                "Append is not supported for training datasets stored in .npy format, only overwrite, "
                "set the optional argument write_mode='overwrite'")
        if not isinstance(self.df, np.ndarray):
            if isinstance(self.df, DataFrame) or isinstance(self.df, RDD):
                df = np.array(self.df.collect())
            if isinstance(df, pd.DataFrame):
                df = df.values
            if isinstance(df, list):
                df = np.array(df)
        tf = TemporaryFile()
        tf.seek(0)
        np.save(tf, df)
        tf.seek(0)
        hdfs.dump(tf.read(), self.path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX)


class HDF5FeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for HDF5 training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(HDF5FeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in hdf5 format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
              :CouldNotConvertDataframe: if the hdf5 dataset could not be converted to a spark dataframe
              :HDF5DatasetFormatNotSupportedForExternalTrainingDatasets: if the user tries to read an
                                                                          external training dataset in the .hdf5 format.
        """
        if not hasattr(self, 'training_dataset') or \
                        self.training_dataset.training_dataset_type \
                        == constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            raise HDF5DatasetFormatNotSupportedForExternalTrainingDatasets("The .hdf5 dataset format is not "
                                                                            "supported for external training datasets.")
        if not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX):
            raise TrainingDatasetNotFound("Could not find a training dataset in file {}".format(
                self.path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX))
        tf = TemporaryFile()
        data = hdfs.load(self.path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)
        tf.write(data)
        tf.seek(0)
        hdf5_file = h5py.File(tf)
        np_array = hdf5_file[self.training_dataset.name][()]
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_NUMPY:
            return np_array
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PYTHON:
            return np_array.tolist()
        if self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_SPARK \
                or self.dataframe_type == constants.FEATURE_STORE.DATAFRAME_TYPE_PANDAS:
            if np_array.ndim != 2:
                raise CouldNotConvertDataframe(
                    "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                    "The number of dimensions are: {}".format(
                        np_array.ndim))
            num_cols = np_array.shape[1]
            dataframe_dict = {}
            for n_col in list(range(num_cols)):
                col_name = "col_" + str(n_col)
                dataframe_dict[col_name] = np_array[:, n_col]
            pandas_df = pd.DataFrame(dataframe_dict)
            sc = spark.sparkContext
            sql_context = SQLContext(sc)
            return fs_utils._return_dataframe_type(sql_context.createDataFrame(pandas_df), self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the hdf5 format

        Returns:
            None

        Raises:
              :ValueError: if the user supplied a write mode that is not supported
              :HDF5DatasetFormatNotSupportedForExternalTrainingDatasets: if the user tries to write an
                                                                          external training dataset in the .hdf5 format.
        """
        if not hasattr(self, 'training_dataset') or \
                        self.training_dataset.training_dataset_type \
                        == constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            raise HDF5DatasetFormatNotSupportedForExternalTrainingDatasets("The .hdf5 dataset format is not "
                                                                           "supported for external training datasets.")
        if (self.write_mode == constants.SPARK_CONFIG.SPARK_APPEND_MODE):
            raise ValueError(
                "Append is not supported for training datasets stored in .hdf5 format, only overwrite, "
                "set the optional argument write_mode='overwrite'")
        if not isinstance(self.df, np.ndarray):
            if isinstance(self.df, DataFrame) or isinstance(self.df, RDD):
                df = np.array(self.df.collect())
            if isinstance(self.df, pd.DataFrame):
                df = df.values
            if isinstance(self.df, list):
                df = np.array(self.df)
        tf = TemporaryFile()
        tf.seek(0)
        hdf5_file = h5py.File(tf)
        tf.seek(0)
        hdf5_file.create_dataset(self.training_dataset.name, data=df)
        tf.seek(0)
        hdf5_file.close()
        tf.seek(0)
        hdfs.dump(tf.read(), self.path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX)


class PetastormFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for Petastorm training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(PetastormFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in petastorm format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
            self.training_dataset.training_dataset_type != constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.parquet(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX):
                spark_df = spark.read.parquet(self.path + constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(
                            self.path + constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} "
                                              "or in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_PETASTORM_SUFFIX))
        else:
            spark_df = spark.read.parquet(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the petastorm format

        Returns:
            None

        Raises:
              :ValueError: if not petastorm schema was provided
        """
        spark = util._find_spark()
        if constants.PETASTORM_CONFIG.SCHEMA in self.petastorm_args:
            schema = self.petastorm_args[constants.PETASTORM_CONFIG.SCHEMA]
            del self.petastorm_args[constants.PETASTORM_CONFIG.SCHEMA]
        else:
            raise ValueError("Required petastorm argument 'schema' is not defined in petastorm_args dict")
        if constants.PETASTORM_CONFIG.FILESYSTEM_FACTORY in self.petastorm_args:
            filesystem_factory = self.petastorm_args[constants.PETASTORM_CONFIG.FILESYSTEM_FACTORY]
            del self.petastorm_args[constants.PETASTORM_CONFIG.FILESYSTEM_FACTORY]
        else:
            filesystem_factory = lambda: pa.hdfs.connect(driver=constants.PETASTORM_CONFIG.LIBHDFS)
        with materialize_dataset(spark, self.path, schema, filesystem_factory=filesystem_factory,
                                 **self.petastorm_args):
            self.df.write.mode(self.write_mode).parquet(self.path)


class ImageFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for image training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(ImageFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in image format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
                        self.training_dataset.training_dataset_type \
                        != constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_SUFFIX):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT).load(
                    self.path + constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(
                            self.path + constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or"
                                              " in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT).load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the image format

        Returns:
            None

        Raises:
              :ValueError: if this method is called, writing datasets in "image" format is not supported with Spark
        """
        raise ValueError("Can not write dataframe in image format. "
                         "To create a training dataset in image format you should manually "
                         "upload the images to the training dataset folder"
                         " in your project.")


class ParquetFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for Parquet training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(ParquetFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in Parquet format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
                        self.training_dataset.training_dataset_type \
                        != constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.parquet(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX):
                spark_df = spark.read.parquet(self.path + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX)
            if not hdfs.exists(self.path) and not hdfs.exists(
                            self.path + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or "
                                              "in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_PARQUET_FORMAT).load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the Parquet format

        Returns:
            None

        """
        self.df.write.mode(self.write_mode).parquet(self.path)


class TSVFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for TSV training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(TSVFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in TSV format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') and \
                        self.training_dataset.training_dataset_type \
                        != constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                    constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                    constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                    constants.DELIMITERS.TAB_DELIMITER).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                    constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                    constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                    constants.DELIMITERS.TAB_DELIMITER).load(
                    self.path + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX)
            if not hdfs.exists(self.path) \
                    and not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or "
                                              "in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_TSV_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT) \
                .option(constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true") \
                .option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER) \
                .option(constants.SPARK_CONFIG.SPARK_INFER_SCHEMA, "true") \
                .load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the TSV format

        Returns:
            None

        """
        self.df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.TAB_DELIMITER).mode(
            self.write_mode).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").csv(self.path)


class CSVFeatureFrame(FeatureFrame):
    """
    FeatureFrame implementation for CSV training datasets
    """


    def __init__(self, **kwargs):
        """
        Initialize featureframe state using the parent class constructor
        """
        super(CSVFeatureFrame, self).__init__(**kwargs)


    def read_featureframe(self, spark):
        """
        Reads a training dataset in CSV format from HopsFS

        Args:
            :spark: the spark session

        Returns:
            dataframe with the data of the training dataset

        Raises:
              :TrainingDatasetNotFound: if the requested training dataset could not be found
        """
        if hasattr(self, 'training_dataset') \
                and self.training_dataset.training_dataset_type \
                        != constants.REST_CONFIG.JSON_TRAINING_DATASET_EXTERNAL_TYPE:
            if hdfs.exists(self.path):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                    constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                    constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                    constants.DELIMITERS.COMMA_DELIMITER).load(self.path)
            elif hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX):
                spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT).option(
                    constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").option(
                    constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER,
                    constants.DELIMITERS.COMMA_DELIMITER).load(self.path +
                                                               constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX)
            if not hdfs.exists(self.path) \
                    and not hdfs.exists(self.path + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX):
                raise TrainingDatasetNotFound("Could not find a training dataset in folder {} or in file {}".format(
                    self.path, self.path + constants.FEATURE_STORE.TRAINING_DATASET_CSV_SUFFIX))
        else:
            spark_df = spark.read.format(constants.FEATURE_STORE.TRAINING_DATASET_CSV_FORMAT) \
                .option(constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true") \
                .option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER) \
                .option(constants.SPARK_CONFIG.SPARK_INFER_SCHEMA, "true") \
                .load(self.path)
        return fs_utils._return_dataframe_type(spark_df, self.dataframe_type)


    def write_featureframe(self):
        """
        Writes a dataframe of data as a training dataset on HDFS in the CSV format

        Returns:
            None

        """
        self.df.write.option(constants.SPARK_CONFIG.SPARK_WRITE_DELIMITER, constants.DELIMITERS.COMMA_DELIMITER).mode(
            self.write_mode).option(
            constants.SPARK_CONFIG.SPARK_WRITE_HEADER, "true").csv(self.path)
