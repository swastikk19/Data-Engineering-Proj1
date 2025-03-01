from fastapi import UploadFile, encoders
import io
import pandas as pd
import chardet
import asyncio
from fastapi.encoders import jsonable_encoder
import re
import numpy as np

async def read_file_function(file: UploadFile):
    try:
        contents = await file.read()

        detected_encoding = chardet.detect(contents)["encoding"]
        encoding = detected_encoding if detected_encoding else "utf-8"

        if encoding.lower() in ["ascii", "utf-8", "utf-16", "utf-32", "latin1", "iso-8859-1"]:
            pass
        else:
            encoding = "utf-8"

        if file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(contents))
        elif file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents), encoding=encoding, encoding_errors = "replace")
        else:
            return {"error": "Unsupported file format. Please upload an Excel or CSV file."}

        return df
    
    except Exception as e:
        print(f"message: {e}")
        return {"message": "Error reading file"}

async def clean_data(fileData):
    try:
        fileData.columns = [re.sub(r'\W+', '_', col) for col in fileData.columns]
        for column in fileData.columns:
            numeric_count = fileData[column].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna().sum()
            total_count = len(fileData[column])

            if numeric_count / total_count >= 0.5:
                fileData[column] = pd.to_numeric(fileData[column], errors="coerce")


        return fileData
    except Exception as e:
        print(f"Error: {e}")
        return {"message": "Something Went Wrong at Data Cleaning!"}
    
async def detect_datatypes(cleanedData):
    try:
        print(cleanedData.dtypes)
        data_types = dict(zip(
            cleanedData.dtypes.index.tolist(),  # Convert Index to List
            await asyncio.gather(*(datatype_mapping(dtype) for dtype in cleanedData.dtypes))
        ))

        json_data = cleanedData.astype(str).to_dict(orient="records")  

        jsonatedData = jsonable_encoder({
            "data_types": data_types,
            "data": json_data
        })

        return jsonatedData
    except Exception as e:
        print(f"message: {e}")
        return {"message": "Something Went Wrong at DataType Detection!"}

async def datatype_mapping (datatype):
    try:
        mapping = {
            "int64": "BIGINT",
            "int32": "INT",
            "float64": "decimal(18,2)",
            "float32": "decimal(18,2)",
            "object": "NVARCHAR(MAX)",
            "bool": "BIT",
            "datetime64[ns]": "DATETIME",
            "timedelta64[ns]": "TIME"
        }

        return mapping.get(str(datatype), "NVARCHAR(MAX)")
    except Exception as e:
        print(f"message: {e}")
        return {"message": "Something Went Wrong at DataType Mapping!"}
