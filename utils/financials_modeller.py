from fastapi import FastAPI, File, UploadFile
import dspy
import io
import pandas as pd
import pymupdf 
from PIL import Image
from utils.mongo import MongoDBClient, connect_to_database, save_csv_file_to_mongodb
import os

from utils.table_extractor import extract_tables_from_pdf

app = FastAPI()
mongo_client = MongoDBClient()

class FinancialModellingSignature(dspy.Signature):
    """Signature for analyzing financial data"""
    example_model: str = dspy.InputField(description="The example financial model in csv format")
    asset_csv: str = dspy.InputField(description="The CSV of financial data for the asset")
    model: str = dspy.OutputField(description="The financial forecast model with the same structure as the example model. The forecast is for the next 5 years. Return the model in MD format")
    
class FinancialModeller(dspy.Module):
    """DSPy agent that extracts tables from financial PDFs and analyzes them"""
    
    def __init__(self):
        super().__init__()

        lm = dspy.LM(
            model="openai/gpt-4.1",
            api_key=""
        )
        
        dspy.configure(lm=lm)
    
    def forward(self, csv_data):
        # read the example model from the csv file as a string
        try:
            example_model = pd.read_csv("example_model.csv").to_csv(index=False)
        except FileNotFoundError:
            # If example_model.csv doesn't exist, provide a basic template
            example_model = "Year,Revenue,Expenses,Net Income,Cash Flow\n2025,1000000,800000,200000,250000\n2026,1100000,850000,250000,300000\n2027,1200000,900000,300000,350000\n2028,1300000,950000,350000,400000\n2029,1400000,1000000,400000,450000"
     
        modelling = dspy.Predict(FinancialModellingSignature)
        model = modelling(asset_csv=csv_data, example_model=example_model)

        return model
    
def generate_financial_model(csv_data):
    modeller = FinancialModeller()
    model = modeller.forward(csv_data)
    return model


#Expose as API endpoint
@app.post("/build-financial-model")
async def build_financial_model(file: UploadFile = File(...)):
    connect_to_database()

    if not file.filename.lower().endswith('.pdf'):
        return {"error": "File must be a PDF"}

    contents = await file.read()
    csv = extract_tables_from_pdf(contents)
    model = generate_financial_model(csv)
 
    try:
        # Save CSV as actual file in MongoDB using GridFS with reference
        result = save_csv_file_to_mongodb(csv, file.filename)
        
        return {
            "csv": csv,
            "model": model,
            "mongodb_file_id": result["file_id"],
            "mongodb_document_id": result["document_id"],
            "filename": file.filename,
            "message": "CSV file extracted and saved to MongoDB GridFS with reference"
        }
    except Exception as e:
        return {
            "csv": csv,
            "model": model,
            "error": f"Failed to save CSV file to MongoDB: {str(e)}",
            "filename": file.filename
        }
    
# generate_financial_model("FY25_Q2_Consolidated_Financial_Statements.pdf")