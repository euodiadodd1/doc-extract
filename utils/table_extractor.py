from fastapi import FastAPI, File, UploadFile
import dspy
import io
# from config import OPENAI_API_KEY
import pymupdf 
from PIL import Image
from utils.mongo import connect_to_database, save_csv_file_to_mongodb

app = FastAPI()


class TableExtractionSignature(dspy.Signature):
    """Signature for extracting tables from PDF files"""
    pdf_file: dspy.Image = dspy.InputField(description="The base64 encoded PDF file")
    extracted_csv: str = dspy.OutputField(description="The extracted CSV of table data from the PDF")


class PDFTableExtractor(dspy.Module):
    """DSPy agent that extracts tables from PDF files and returns them as CSV"""
    
    def __init__(self):
        super().__init__()
        self.table_extractor = dspy.LM(
            model="openai/gpt-4.1-mini", 
            api_key=""
            )
        
        dspy.configure(lm=self.table_extractor)
        
    def forward(self, pdf_file):
        """
        Extract tables from a PDF file and return as CSV
        
        Args:
            pdf_file: The PDF file to extract tables from
        
        Returns:
            str: CSV representation of the extracted tables
        """
        # Read the pdf file and convert to PIL image
        try:
            # Handle both file-like objects and bytes
            if isinstance(pdf_file, bytes):
                doc = pymupdf.open("pdf", pdf_file)
            else:
                doc = pymupdf.open(pdf_file)
                
            pages = [doc.load_page(i) for i in range(len(doc))]
            images = [Image.open(io.BytesIO(page.get_pixmap().tobytes("png"))) for page in pages]
            image = dspy.Image.from_PIL(images[0])
            
            # Extract tables using the signature
            extraction = dspy.Predict(TableExtractionSignature)
            return extraction(pdf_file=image).extracted_csv
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return f"Error extracting tables: {str(e)}"


# Usage example
def extract_tables_from_pdf(pdf_file):
    """
    Helper function to extract tables from a PDF file
    
    Args:
        pdf_file: File-like object or bytes of PDF
    
    Returns:
        str: CSV representation of tables in the PDF
    """
    extractor = PDFTableExtractor()
    # Fix: The forward method returns a string directly, not an object
    csv = extractor.forward(pdf_file)
    
    # Write csv to file
    with open("table.csv", "w") as f:
        f.write(csv)
    
    return csv