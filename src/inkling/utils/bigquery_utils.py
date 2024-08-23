import json

from collections import OrderedDict
from datetime import datetime, date, time
from typing import List, Optional

import pyspark.pandas

from pyarrow.lib import Date32Scalar, Time32Scalar, Time64Scalar
from pydantic import BaseModel
from pyspark.sql.types import Row, StructType

from tunip.spark_utils import SparkConnector
from tunip.google_cloud_utils import BigQueryDriver
from tunip.datetime_utils import TIME_DT_FORMAT

from inkling import LOGGER
from inkling.utils.spark_schema_infer import SparkDataFrameSchemaInferer


def has_json_str(value):
    if isinstance(value, str):
        stripped = value.strip()
        if len(stripped) < 2:
            return False
        if stripped[0] == '{' and stripped[-1] == '}':
            if stripped.find('"') > 0:
                try:
                    _ = json.loads(stripped)
                    return True
                except:
                    return False


class SparkBatchWriterForPandasChunkIterator:
    def __init__(self, service_config, save_path, batch_size=25000, predefined_fields={}):
        self.service_config = service_config
        self.batch_size = batch_size
        self.save_path = save_path
        self.df_schema: Optional[StructType] = None
        self.predefined_fields = predefined_fields
        self.has_valid_value = None
        self.parse_nested = True

    def __call__(self, df_iterable):
        LOGGER.info(f'START FOR {__class__.__name__}')
        spark = SparkConnector.getOrCreate(local=True)

        row_number = 0
        pd_objects = []
        df_schema = None
        for records in df_iterable:
            for r in records.itertuples(index=False):
                # from :pandas.core.frame.Pandas to :Row

                pd_objects.append(r)
                # docs.append(Row(**r._asdict()))

                if row_number % 10000 == 0:
                    LOGGER.info(f"load {row_number} number of rows ...")

                row_number += 1
                if row_number % self.batch_size == 0:
                    LOGGER.info(f'save batch: {row_number}th rows ...')

                    df_schema = df_schema or self.get_my_schema(pd_objects)
                    self._save_batch(spark, pd_objects, row_number, df_schema, row_number == self.batch_size)
                    pd_objects = []

        if pd_objects:
            df_schema = df_schema or self.get_my_schema(pd_objects)
            self._save_batch(spark, pd_objects, row_number, df_schema)
        
        LOGGER.info(f"Finally, {row_number} number of documents are loaded.")

        LOGGER.info(f'==== END {__class__.__name__} ====')

    def _save_batch(self, spark, pd_objects, row_index, schema=None, first_batch=False):
        # rows = [Row(**pdo._asdict()) for pdo in pd_objects]
        rows = []
        for pdo in pd_objects:
            row_dict = OrderedDict()
            for key in self.has_valid_value:
                value = pdo.__getattribute__(key)
                if has_json_str(value) and self.parse_nested:
                    value = json.loads(value)
                elif isinstance(value, Date32Scalar):
                    value = value.as_py().date()
                elif isinstance(value, Time64Scalar) or isinstance(value, time):
                    value = f"{value.hour}:{value.minute}"
                row_dict[key] = value
            assert len(row_dict) == len(self.has_valid_value)
            rows.append(Row(**row_dict))

        if row_index <= self.batch_size or first_batch:
            write_mode = 'overwrite'
            LOGGER.info(f'Overwrite to {self.save_path} ...')
        else:
            write_mode = 'append'
        batch_df = spark.createDataFrame(rows, schema=schema)
        batch_df.write.option('compression', 'gzip').format('json').mode(write_mode).save(self.save_path)
    
    def get_my_schema(self, rows) -> StructType:
        if self.df_schema is None:
            inferer = SparkDataFrameSchemaInferer(predefined_fields=self.predefined_fields)
            self.df_schema, self.has_valid_value = inferer.get_schema_from_batch(rows)
        return self.df_schema


class SparkBatchWriterForArrowChunkIterator:
    def __init__(self, service_config, save_path, batch_size=25000, predefined_fields={}):
        self.service_config = service_config
        self.batch_size = batch_size
        self.save_path = save_path
        self.df_schema: Optional[StructType] = None
        self.predefined_fields = predefined_fields
        self.has_valid_value = None
        self.parse_nested = True

    def __call__(self, chunk_iterable):
        LOGGER.info(f'START FOR {__class__.__name__}')
        spark = SparkConnector.getOrCreate(local=True)

        row_number = 0
        prev_row_number = 0
        rows = []
        df_schema = None

        # chunk: pyarrow.lib.RecordBatch
        for chunk in chunk_iterable:

            for row_index in range(0, chunk.num_rows):
                row_dict = {}
                for column_index in range(0, chunk.num_columns):
                    column_name = chunk.schema[column_index].name
                    row_dict[column_name] = chunk[column_name][row_index].as_py()
                rows.append(row_dict)

            LOGGER.info(f"load {row_number} number of rows ...")

            row_number += chunk.num_rows
            if (row_number-prev_row_number) >= self.batch_size:
                LOGGER.info(f'save batch: {row_number}th rows ...')

                df_schema = df_schema or self.get_my_schema(rows)
                self._save_batch(spark, rows, row_number, df_schema, row_number == self.batch_size)
                rows= []
                prev_row_number = row_number

        if rows:
            df_schema = df_schema or self.get_my_schema(rows)
            self._save_batch(spark, rows, row_number, df_schema)
        
        LOGGER.info(f"Finally, {row_number} number of documents are loaded.")

        LOGGER.info(f'==== END {__class__.__name__} ====')

    def _save_batch(self, spark, batch, row_index, schema=None, first_batch=False):
        # rows = [Row(**pdo._asdict()) for pdo in pd_objects]
        rows = []
        for row in batch:
            row_dict = OrderedDict()
            for key in self.has_valid_value:
                value = row[key]
                if has_json_str(value) and self.parse_nested:
                    value = json.loads(value)
                elif isinstance(value, Date32Scalar):
                    value = value.as_py().date()
                elif isinstance(value, Time32Scalar) or isinstance(value, Time64Scalar) or isinstance(value, time):
                    value = value.strftime(TIME_DT_FORMAT)
                    # NOT WORKING
                    # value = datetime.strptime(str(value), '%H:%M:%S').time()
                row_dict[key] = value
            assert len(row_dict) == len(self.has_valid_value)
            rows.append(Row(**row_dict))

        if row_index <= self.batch_size or first_batch:
            write_mode = 'overwrite'
            LOGGER.info(f'Overwrite to {self.save_path} ...')
        else:
            write_mode = 'append'
        batch_df = spark.createDataFrame(rows, schema=schema)
        batch_df.write.option('compression', 'gzip').format('json').mode(write_mode).save(self.save_path)
    
    def get_my_schema(self, rows) -> StructType:
        if self.df_schema is None:
            inferer = SparkDataFrameSchemaInferer(predefined_fields=self.predefined_fields)
            self.df_schema, self.has_valid_value = inferer.get_schema_from_batch_for_arrow(rows)
        return self.df_schema


class SparkBatchWriterForPandasRowIterator:
    """
    writer supporting user callback
    """
    def __init__(self, service_config, save_path, batch_size=25000, predefined_fields={}, user_callback=None):
        self.service_config = service_config
        self.batch_size = batch_size
        self.save_path = save_path
        self.df_schema: Optional[StructType] = None
        self.predefined_fields = predefined_fields
        self.has_valid_value = None
        self.parse_nested = True

        self.user_callback = user_callback

    def __call__(self, df_iterable, **kwargs):
        LOGGER.info(f'START FOR {__class__.__name__}')
        spark = SparkConnector.getOrCreate(local=True)

        row_number = 0
        pd_objects = []
        df_schema = None
        # for records in df_iterable:
        #     for r in records.itertuples(index=False):
        for r in df_iterable.itertuples(index=False):
            # from :pandas.core.frame.Pandas to :Row

            pd_objects.append(r)
            # docs.append(Row(**r._asdict()))

            if row_number % 10000 == 0:
                LOGGER.info(f"load {row_number} number of rows ...")

            row_number += 1
            if row_number % self.batch_size == 0:
                LOGGER.info(f'save batch: {row_number}th rows ...')

                df_schema = df_schema or self.get_my_schema(pd_objects)
                self._save_batch(spark, pd_objects, row_number, df_schema, row_number == self.batch_size, **kwargs)
                pd_objects = []

        if pd_objects:
            df_schema = df_schema or self.get_my_schema(pd_objects)
            first_batch = True if row_number <= self.batch_size else False
            self._save_batch(spark, pd_objects, row_number, df_schema, first_batch, **kwargs)
        
        LOGGER.info(f"Finally, {row_number} number of documents are loaded.")

        LOGGER.info(f'==== END {__class__.__name__} ====')

    def _save_batch(self, spark, pd_objects, row_index, schema, first_batch, **kwargs):
        # rows = [Row(**pdo._asdict()) for pdo in pd_objects]
        rows = []
        for pdo in pd_objects:
            row_dict = OrderedDict()
            for key in self.has_valid_value:
                value = pdo.__getattribute__(key)
                if has_json_str(value) and self.parse_nested:
                    value = json.loads(value)
                elif isinstance(value, Date32Scalar):
                    value = value.as_py().date()
                elif isinstance(value, Time64Scalar) or isinstance(value, time):
                    value = f"{value.hour}:{value.minute}"
                row_dict[key] = value
            assert len(row_dict) == len(self.has_valid_value)
            rows.append(Row(**row_dict))

        if row_index <= self.batch_size or first_batch:
            write_mode = 'overwrite'
            LOGGER.info(f'Overwrite to {self.save_path} ...')
        else:
            write_mode = 'append'

        rows = self.user_callback(schema, rows, **kwargs)
        batch_df = spark.createDataFrame(rows, schema=schema)
        batch_df.write.option('compression', 'gzip').format('json').mode(write_mode).save(self.save_path)
    
    def get_my_schema(self, rows) -> StructType:
        if self.df_schema is None:
            inferer = SparkDataFrameSchemaInferer(predefined_fields=self.predefined_fields)
            self.df_schema, self.has_valid_value = inferer.get_schema_from_batch(rows)
        return self.df_schema


class Bigquery2PandasDataFrameResponse(BaseModel):
    dataframe: pyspark.pandas.DataFrame
    save_path: str

    class Config:
        arbitrary_types_allowed = True


class BigQuery2PandasDataFrame:
    """
    do querying bigquery,
    write down to storage,
    and finally get the pandas spark dataframe
    """
    def __init__(self, service_config, bigquery, save_path, predefined_fields={}, batch_size=25000):
        self.service_config = service_config
        self.bigquery = bigquery
        self.save_path = save_path
        self.batch_size = batch_size

        # set 1) column name and 2) value what you want the type of value
        self.predefined_fields = predefined_fields

    def save_and_load(self) -> Bigquery2PandasDataFrameResponse:
        bq_driver = BigQueryDriver(self.service_config.config)
        query_job = bq_driver.client.query(self.bigquery)
        results = query_job.result()

        # df_iter = results.to_dataframe_iterable()
        df_iter = results.to_arrow_iterable()

        batch_writer = SparkBatchWriterForArrowChunkIterator(self.service_config, self.save_path, self.batch_size, self.predefined_fields)
        batch_writer(df_iter)

        spark = SparkConnector.getOrCreate(local=True)
        df = spark.read.json(self.save_path)
        psdf = df.to_pandas_on_spark()
        return Bigquery2PandasDataFrameResponse(dataframe=psdf, save_path=self.save_path)

    def save(self):
        bq_driver = BigQueryDriver(self.service_config.config)
        query_job = bq_driver.client.query(self.bigquery)
        results = query_job.result()

        df_iter = results.to_arrow_iterable()

        batch_writer = SparkBatchWriterForArrowChunkIterator(self.service_config, self.save_path, self.batch_size, self.predefined_fields)
        batch_writer(df_iter)

    def load(self, path) -> Bigquery2PandasDataFrameResponse:
        spark = SparkConnector.getOrCreate(local=True)
        df = spark.read.json(path)
        psdf = df.to_pandas_on_spark()
        return Bigquery2PandasDataFrameResponse(dataframe=psdf, save_path=path)
