# https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#dtypes
DTYPES_MAP = {
    "string": "string[pyarrow]",
    "int": "Int64[pyarrow]",
    "UInt64": "UInt64[pyarrow]",
    "UInt16": "UInt16[pyarrow]",
    "category": "category",
    "float": "Float64[pyarrow]",
    "Float64": "Float64[pyarrow]",
}

DTYPES = {
    "input_table": {
        "project": DTYPES_MAP["category"],
        "study": DTYPES_MAP["category"],
        "file_path": DTYPES_MAP["string"],
        "category": DTYPES_MAP["category"],
        "build": DTYPES_MAP["category"],
        "notes_consortium": DTYPES_MAP["string"],
        "total_samples": DTYPES_MAP["UInt64"],
        "total_cases": DTYPES_MAP["UInt64"],
        "total_controls": DTYPES_MAP["UInt64"],
        "trait_desc": DTYPES_MAP["string"],
    },
}
