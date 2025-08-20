"""
FastAPI Extension for Healthcare Claims Resubmission Pipeline
=============================================================

A REST API interface for uploading datasets and processing claims
for resubmission eligibility.

Author: Glorry Sibomana
Date: 2025-08-20
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
from typing import List, Optional
import json
import csv
import io
import logging
from datetime import datetime
from pydantic import BaseModel

# Import from the main pipeline (assumes the main script is available)
from healthcare_pipeline import (
    ClaimsResubmissionPipeline, 
    ClaimsDataNormalizer,
    DenialReasonClassifier,
    ResubmissionCandidate,
    PipelineMetrics
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Healthcare Claims Resubmission API",
    description="API for processing insurance claims and identifying resubmission candidates",
    version="1.0.0"
)

class ProcessingRequest(BaseModel):
    """Request model for processing claims"""
    reference_date: Optional[str] = "2025-07-30"

class ProcessingResponse(BaseModel):
    """Response model for processing results"""
    candidates: List[dict]
    metrics: dict
    success: bool
    message: str

class HealthCheck(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    version: str

@app.get("/", response_model=HealthCheck)
async def root():
    """Health check endpoint"""
    return HealthCheck(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="1.0.0"
    )

@app.post("/process-claims", response_model=ProcessingResponse)
async def process_claims(
    alpha_file: Optional[UploadFile] = File(None),
    beta_file: Optional[UploadFile] = File(None),
    reference_date: str = Form("2025-07-30")
):
    """
    Process uploaded claims files and return resubmission candidates
    
    Args:
        alpha_file: Optional CSV file from Alpha EMR system
        beta_file: Optional JSON file from Beta EMR system  
        reference_date: Reference date for calculating claim age
        
    Returns:
        ProcessingResponse: Processing results with candidates and metrics
    """
    
    if not alpha_file and not beta_file:
        raise HTTPException(
            status_code=400,
            detail="At least one file (alpha_file or beta_file) must be provided"
        )
    
    try:
        # Initialize pipeline
        pipeline = ClaimsResubmissionPipeline(reference_date=reference_date)
        
        # Process alpha file if provided
        alpha_data = None
        if alpha_file:
            if not alpha_file.filename.endswith('.csv'):
                raise HTTPException(
                    status_code=400,
                    detail="Alpha file must be a CSV file"
                )
            
            content = await alpha_file.read()
            csv_content = content.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            alpha_data = list(csv_reader)
            
            # Save temporarily and process
            with open("temp_alpha.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=alpha_data[0].keys() if alpha_data else [])
                writer.writeheader()
                writer.writerows(alpha_data)
        
        # Process beta file if provided
        beta_data = None
        if beta_file:
            if not beta_file.filename.endswith('.json'):
                raise HTTPException(
                    status_code=400,
                    detail="Beta file must be a JSON file"
                )
            
            content = await beta_file.read()
            json_content = content.decode('utf-8')
            beta_data = json.loads(json_content)
            
            # Save temporarily and process
            with open("temp_beta.json", "w", encoding="utf-8") as f:
                json.dump(beta_data, f, indent=2)
        
        # Process claims
        candidates = pipeline.process_claims(
            alpha_file="temp_alpha.csv" if alpha_file else None,
            beta_file="temp_beta.json" if beta_file else None
        )
        
        # Convert candidates to dictionaries
        candidates_dict = [candidate.to_dict() for candidate in candidates]
        
        # Prepare metrics
        metrics_dict = {
            "total_claims_processed": pipeline.metrics.total_claims_processed,
            "claims_by_source": pipeline.metrics.claims_by_source,
            "flagged_for_resubmission": pipeline.metrics.flagged_for_resubmission,
            "excluded_claims": pipeline.metrics.excluded_claims,
            "processing_errors": pipeline.metrics.processing_errors
        }
        
        # Clean up temporary files
        import os
        try:
            if alpha_file and os.path.exists("temp_alpha.csv"):
                os.remove("temp_alpha.csv")
            if beta_file and os.path.exists("temp_beta.json"):
                os.remove("temp_beta.json")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary files: {e}")
        
        return ProcessingResponse(
            candidates=candidates_dict,
            metrics=metrics_dict,
            success=True,
            message=f"Successfully processed {pipeline.metrics.total_claims_processed} claims"
        )
        
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON format in beta file"
        )
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="File encoding not supported. Please use UTF-8 encoding"
        )
    except Exception as e:
        logger.error(f"Processing error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal processing error: {str(e)}"
        )

@app.post("/classify-denial-reason")
async def classify_denial_reason(denial_reason: str = Form(...)):
    """
    Classify a denial reason as retryable or non-retryable
    
    Args:
        denial_reason: The denial reason to classify
        
    Returns:
        dict: Classification result with recommendation
    """
    try:
        classifier = DenialReasonClassifier()
        is_retryable = classifier.is_retryable(denial_reason)
        recommended_changes = classifier.get_recommended_changes(denial_reason)
        
        return {
            "denial_reason": denial_reason,
            "is_retryable": is_retryable,
            "recommended_changes": recommended_changes,
            "classification": "retryable" if is_retryable else "non-retryable"
        }
    except Exception as e:
        logger.error(f"Classification error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Classification error: {str(e)}"
        )

@app.get("/supported-formats")
async def get_supported_formats():
    """Get information about supported file formats"""
    return {
        "supported_formats": {
            "alpha_emr": {
                "format": "CSV",
                "required_fields": [
                    "claim_id", "patient_id", "procedure_code", 
                    "denial_reason", "submitted_at", "status"
                ],
                "description": "Flat CSV format from Alpha EMR system"
            },
            "beta_emr": {
                "format": "JSON",
                "required_fields": [
                    "id", "member", "code", "error_msg", "date", "status"
                ],
                "description": "Nested JSON format from Beta EMR system"
            }
        },
        "resubmission_criteria": {
            "status": "must be 'denied'",
            "patient_id": "must not be null/empty",
            "submission_age": "must be more than 7 days old",
            "denial_reason": "must be classified as retryable"
        }
    }

@app.get("/metrics-schema")
async def get_metrics_schema():
    """Get the schema for pipeline metrics"""
    return {
        "metrics_schema": {
            "total_claims_processed": "Total number of claims processed",
            "claims_by_source": "Breakdown of claims by source system",
            "flagged_for_resubmission": "Number of claims flagged for resubmission",
            "excluded_claims": "Breakdown of excluded claims by reason",
            "processing_errors": "Number of processing errors encountered"
        },
        "exclusion_reasons": [
            "status_not_denied", "missing_patient_id", "too_recent",
            "missing_date", "non_retryable_reason"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "fastapi_extension:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True,
        log_level="info"
    )