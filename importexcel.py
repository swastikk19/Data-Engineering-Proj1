from fastapi import UploadFile
from helpers import read_file_function, clean_data, detect_datatypes

async def process_excel(file: UploadFile):
    try:
        fileData = await read_file_function(file)
        cleanedData = await clean_data(fileData)
        dataWithDataTypes = await detect_datatypes(cleanedData)

        # print(dataWithDataTypes)
        return dataWithDataTypes

    except Exception as e:
        print(f"Error: {e}")
        return {"error": "Something Went Wrong! ❌"}
