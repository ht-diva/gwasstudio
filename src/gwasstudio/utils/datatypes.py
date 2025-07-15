from enum import Enum

import numpy as np


class DataType(Enum):
    ASCII = "ascii"
    STRING = "str"
    FLOAT32_NP = np.float32
    UINT8_NP = np.uint8
    UINT32_NP = np.uint32
    STRING_PA = "string[pyarrow]"
    INT64_PA = "Int64[pyarrow]"
    UINT64_PA = "UInt64[pyarrow]"
    UINT16_PA = "UInt16[pyarrow]"
    CATEGORY = "category"
    FLOAT_PA = "Float64[pyarrow]"
