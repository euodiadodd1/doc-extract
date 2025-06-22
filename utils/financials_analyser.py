from fastapi import FastAPI, File, UploadFile
import dspy
import io
import pandas as pd
import pymupdf 
from PIL import Image
from utils.mongo import MongoDBClient, connect_to_database, save_csv_file_to_mongodb
import os

from utils.table_extractor import extract_tables_from_pdf
from utils.financials_modeller import generate_financial_model

app = FastAPI()
mongo_client = MongoDBClient()

class FinancialAnalysisSignature(dspy.Signature):
    """Signature for analyzing financial data"""
    csv_data: str = dspy.InputField(description="The CSV of financial data")
    analysis: str = dspy.OutputField(description="Comprehensive analysis of the financial data including key metrics, trends, and insights along with a recommendation for investment. Use markdown, use newlines, headings, etc")
    
class FinancialAnalyzer(dspy.Module):
    """DSPy agent that extracts tables from financial PDFs and analyzes them"""
    
    def __init__(self):
        super().__init__()

        lm = dspy.LM(
        )
        
        self.agent = dspy.ReAct(
            signature=FinancialAnalysisSignature,
            tools=[]
        )
        dspy.configure(lm=lm)
    
    def forward(self, csv_data):
        response = self.agent(
            question="Analyses the financial data in the csv and provide a recommendation for investment. Use markdown.",
            csv_data=csv_data
            )

        return response["analysis"]

def extract_analyze_and_model_financials(pdf_file):
    csv = extract_tables_from_pdf(pdf_file)
    
    # Generate financial analysis
    analyzer = FinancialAnalyzer()
    analysis = analyzer.forward(csv)
    
    # Generate financial model
    financial_model = generate_financial_model(csv)
    
    return csv, analysis, financial_model.model

#Expose as API endpoint
@app.post("/analyze-financials")
async def analyze_financials(file: UploadFile = File(...)):
    connect_to_database()

    if not file.filename.lower().endswith('.pdf'):
        return {"error": "File must be a PDF"}

    contents = await file.read()
    csv, analysis, financial_model = extract_analyze_and_model_financials(contents)
 
    try:
        # Save CSV as actual file in MongoDB using GridFS with reference, analysis, and financial model
        result = save_csv_file_to_mongodb(csv, file.filename, analysis, financial_model)
        
        return {
            "csv": csv,
            "analysis": analysis,
            "financial_model": financial_model,
            "mongodb_file_id": result["file_id"],
            "mongodb_document_id": result["document_id"],
            "filename": file.filename,
            "message": "CSV file extracted and saved to MongoDB GridFS with reference, analysis, and financial model"
        }
    except Exception as e:
        return {
            "csv": csv,
            "analysis": analysis,
            "financial_model": financial_model,
            "error": f"Failed to save CSV file to MongoDB: {str(e)}",
            "filename": file.filename
        }
    
# extract_analyze_and_model_financials("FY25_Q2_Consolidated_Financial_Statements.pdf")