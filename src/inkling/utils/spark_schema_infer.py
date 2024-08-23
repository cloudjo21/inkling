import json

from collections import OrderedDict
from datetime import date, time
from pyarrow.lib import Date32Scalar, Time32Scalar, Time64Scalar
from typing import List, Union

from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    FloatType,
    Row,
    StructField,
    StringType,
    StructType,
)

from inkling import LOGGER


class SparkDataFrameSchemaInferer:
    """
    infer spark dataframe scheme from pandas.core.frame.Pandas
    """

    def __init__(self, predefined_fields=dict()):
        self.predefined_fields = predefined_fields
        # self.batch_keys = batch_keys
        self.parse_nested = True

    def has_json_str(self, value):
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

    def get_spark_primitive_type(self, key, value, with_struct=False):
        if isinstance(value, int):
            dtype = IntegerType()
        elif isinstance(value, float):
            dtype = FloatType()
        elif isinstance(value, str):
            dtype = StringType()
        elif isinstance(value, Date32Scalar) or isinstance(value, date):
            dtype = DateType()
        elif isinstance(value, Time32Scalar) or isinstance(value, Time64Scalar) or isinstance(value, time):
            dtype = StringType()
        else:
            # use default type although we can't infer dtype
            dtype = StringType()
            LOGGER.warning(f"Not supported data type: {key}:{value}, please check your meaning of value ...")
            # raise Exception(f'Not supported data type: {key}:{value}')
        
        if with_struct:
            return StructField(key, dtype)
        else:
            return dtype

    def get_schema(self, key, value, return_field=False) -> Union[StructType, StructField]:
        def nested_type(p_key, p_value):
            has_json = self.has_json_str(p_value)

            if (isinstance(p_value, dict) or has_json) and self.parse_nested:
                if has_json:
                    p_value = json.loads(p_value)
                value_fields = []
                for c_k, c_v in p_value.items():
                    strt_field = nested_type(c_k, c_v)
                    value_fields.append(strt_field)
                strt_type = StructType(value_fields)
                return StructField(p_key, strt_type)
            elif isinstance(p_value, list):
                if len(p_value) > 0:
                    prim_type = self.get_spark_primitive_type(None, p_value[0], with_struct=False)
                else:
                    prim_type = StringType() # TODO get default type for p_value, and then get spark primitive type
                return StructField(p_key, ArrayType(prim_type, True), True)
            else:
                strt_field = self.get_spark_primitive_type(p_key, p_value, with_struct=True)
                return strt_field

        root_field = nested_type(key, value)
        if return_field is False:
            return StructType([root_field])
        else:
            return root_field

    def get_schema_from_batch(self, pd_objects) -> tuple:
        """
        return : tuple [StructType, OrderedDict]
        """
        schema = StructType([])

        has_valid_value = OrderedDict()
        valid_value_map = {}
        for pd_object in pd_objects:
            row = Row(**pd_object._asdict())
            for key, value in row.asDict().items():
                if key in self.predefined_fields:
                    has_valid_value[key] = True
                    valid_value_map[key] = self.predefined_fields[key]
                    continue
                if key not in has_valid_value:
                    if value is not None:
                        has_valid_value[key] = True
                        valid_value_map[key] = value
                    else:
                        has_valid_value[key] = False
                else:
                    if key not in valid_value_map and value is not None:
                        valid_value_map[key] = value
        
        # for key, value in sorted(valid_value_map.items(), key=lambda item: item[0], reverse=False):
        for key, _ in has_valid_value.items():
            value = valid_value_map[key]
            struct_field = self.get_schema(key, value, return_field=True)
            schema.add(struct_field)

        return schema, has_valid_value

    def get_schema_from_batch_for_arrow(self, rows) -> tuple:
        """
        return : tuple [StructType, OrderedDict]
        """
        schema = StructType([])

        has_valid_value = OrderedDict()
        valid_value_map = {}
        for _row in rows:
            # row = Row(**_row)
            # for key, value in row.asDict().items():
            for key, value in _row.items():
                if key in self.predefined_fields:
                    has_valid_value[key] = True
                    valid_value_map[key] = self.predefined_fields[key]
                    continue
                if key not in has_valid_value:
                    if value is not None:
                        has_valid_value[key] = True
                        valid_value_map[key] = value
                    else:
                        has_valid_value[key] = False
                else:
                    if key not in valid_value_map and value is not None:
                        valid_value_map[key] = value
        
        # for key, value in sorted(valid_value_map.items(), key=lambda item: item[0], reverse=False):
        for key, _ in has_valid_value.items():
            value = valid_value_map[key]
            struct_field = self.get_schema(key, value, return_field=True)
            schema.add(struct_field)

        return schema, has_valid_value
