from fastapi import FastAPI, File, UploadFile
from importexcel import process_excel

app = FastAPI()

@app.get("/")
async def main_func():
    return {"message": "This api is working again!"}


@app.post("/import/excel")
async def import_excel(file: UploadFile = File(...)):
    output = await process_excel(file)
    return output


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)