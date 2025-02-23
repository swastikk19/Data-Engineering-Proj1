from fastapi import UploadFile, encoders
import io
import pandas as pd
import re

async def read_file_function(file: UploadFile):
    try:
        contents = await file.read();

        if file.filename.endswith(".xlsx") or file.filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(contents))
        elif file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            return {"error": "Unsupported file format. Please upload an Excel or CSV file."}

        
        return df
    
    except Exception as e:
        print(f"message: {e}")
        return {"message": "Error reading file"}


async def clean_data(fileData):
    try:
        fileData = fileData.drop_duplicates(subset=None, keep="first")
        fileData = fileData.dropna(how = "all", subset = None)
        fileData.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col).strip().lower() for col in fileData.columns]
        
        return fileData
    except Exception as e:
        print(f"Error: {e}")
        return {"message": "Something Went Wrong at Data Cleaning!"}
    

async def detect_datatypes(cleanedData):
    try:
        data_types = {col: str(dtype) for col, dtype in cleanedData.dtypes.items()} 
        jsonatedData = {
            "data_types": encoders.jsonable_encoder(data_types),
            "data": encoders.jsonable_encoder(cleanedData.to_dict(orient="records"))
        }
        return jsonatedData
    except Exception as e:
        print(f"message: {e}")
        return {"message": "Something Went Wrong at DataType Detection!"}
