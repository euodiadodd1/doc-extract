from fastapi import FastAPI, File, UploadFile
import base64

app = FastAPI()

# Python function
def convert_pdf_to_base64(pdf_bytes: bytes) -> str:
    """Convert PDF bytes to base64 encoded string"""
    return base64.b64encode(pdf_bytes).decode('utf-8')

# Expose as API endpoint
@app.get("/convert-pdf")
async def convert_pdf_endpoint(file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.pdf'):
        return {"error": "File must be a PDF"}
    
    contents = await file.read()
    base64_encoded = convert_pdf_to_base64(contents)
    
    return {"base64_encoded": base64_encoded}
