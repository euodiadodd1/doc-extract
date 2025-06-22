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

class FinancialAnalysisSignature(dspy.Signature):
    """Signature for analyzing financial data"""
    csv_data: str = dspy.InputField(description="The CSV of financial data")
    analysis: str = dspy.OutputField(description="Comprehensive analysis of the financial data including key metrics, trends, and insights along with a recommendation for investment")
    
class FinancialAnalyzer(dspy.Module):
    """DSPy agent that extracts tables from financial PDFs and analyzes them"""
    
    def __init__(self):
        super().__init__()

        lm = dspy.LM(
            model="openai/gpt-4.1",
            api_key=""
        )
        
        self.agent = dspy.ReAct(
            signature=FinancialAnalysisSignature,
            tools=[]
        )
        dspy.configure(lm=lm)
    
    def forward(self, csv_data):
        response = self.agent(question="Analyses the financial data in the csv and provide a recommendation for investment.", csv_data=csv_data)

        return response["analysis"]

def extract_and_analyze_financials(pdf_file):
    csv = extract_tables_from_pdf(pdf_file)
    analyzer = FinancialAnalyzer()
    analysis = analyzer.forward(csv)
    return csv, analysis

#Expose as API endpoint
@app.post("/analyze-financials")
async def analyze_financials(file: UploadFile = File(...)):
    connect_to_database()

    if not file.filename.lower().endswith('.pdf'):
        return {"error": "File must be a PDF"}

    contents = await file.read()
    csv, analysis = extract_and_analyze_financials(contents)
 
    try:
        # Save CSV as actual file in MongoDB using GridFS with reference
        result = save_csv_file_to_mongodb(csv, file.filename)
        
        return {
            "csv": csv,
            "analysis": analysis,
            "mongodb_file_id": result["file_id"],
            "mongodb_document_id": result["document_id"],
            "filename": file.filename,
            "message": "CSV file extracted and saved to MongoDB GridFS with reference"
        }
    except Exception as e:
        return {
            "csv": csv,
            "analysis": analysis,
            "error": f"Failed to save CSV file to MongoDB: {str(e)}",
            "filename": file.filename
        }
    
# extract_and_analyze_financials("FY25_Q2_Consolidated_Financial_Statements.pdf")