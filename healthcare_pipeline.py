"""
Healthcare Claims Resubmission Pipeline
=======================================

A robust pipeline for ingesting, normalizing, and analyzing insurance claim data
from multiple EMR systems to identify claims eligible for resubmission.

Author: Glorry Sibomana
Date: 2025-08-20
"""

import json
import csv
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('claims_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClaimStatus(Enum):
    """Enum for claim status values"""
    APPROVED = "approved"
    DENIED = "denied"

class SourceSystem(Enum):
    """Enum for source system identifiers"""
    ALPHA = "alpha"
    BETA = "beta"

@dataclass
class NormalizedClaim:
    """Unified schema for normalized claim data"""
    claim_id: str
    patient_id: Optional[str]
    procedure_code: str
    denial_reason: Optional[str]
    status: str
    submitted_at: str  # ISO date format
    source_system: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class ResubmissionCandidate:
    """Schema for claims eligible for resubmission"""
    claim_id: str
    resubmission_reason: str
    source_system: str
    recommended_changes: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    total_claims_processed: int = 0
    claims_by_source: Dict[str, int] = None
    flagged_for_resubmission: int = 0
    excluded_claims: Dict[str, int] = None
    processing_errors: int = 0

    def __post_init__(self):
        if self.claims_by_source is None:
            self.claims_by_source = {}
        if self.excluded_claims is None:
            self.excluded_claims = {}

class DenialReasonClassifier:
    """Mock LLM classifier for ambiguous denial reasons"""
    
    # Known retryable reasons
    RETRYABLE_REASONS = {
        "missing modifier",
        "incorrect npi", 
        "prior auth required"
    }
    
    # Known non-retryable reasons
    NON_RETRYABLE_REASONS = {
        "authorization expired",
        "incorrect provider type"
    }
    
    # Hardcoded mappings for ambiguous cases
    AMBIGUOUS_MAPPINGS = {
        "incorrect procedure": False,  # Non-retryable
        "form incomplete": True,       # Retryable
        "not billable": False,         # Non-retryable
        None: False                    # Default to non-retryable
    }

    @classmethod
    def is_retryable(cls, denial_reason: Optional[str]) -> bool:
        """
        Classify if a denial reason is retryable
        
        Args:
            denial_reason: The denial reason string or None
            
        Returns:
            bool: True if retryable, False otherwise
        """
        if not denial_reason:
            return cls.AMBIGUOUS_MAPPINGS.get(None, False)
        
        # Normalize the denial reason
        normalized_reason = denial_reason.lower().strip()
        
        # Check known retryable reasons
        if normalized_reason in cls.RETRYABLE_REASONS:
            return True
            
        # Check known non-retryable reasons
        if normalized_reason in cls.NON_RETRYABLE_REASONS:
            return False
            
        # Check ambiguous mappings
        return cls.AMBIGUOUS_MAPPINGS.get(normalized_reason, False)

    @classmethod
    def get_recommended_changes(cls, denial_reason: Optional[str]) -> str:
        """
        Get recommended changes for a denial reason
        
        Args:
            denial_reason: The denial reason string
            
        Returns:
            str: Recommended changes for resubmission
        """
        if not denial_reason:
            return "Review claim details and resubmit"
        
        normalized_reason = denial_reason.lower().strip()
        
        recommendations = {
            "missing modifier": "Add appropriate modifier and resubmit",
            "incorrect npi": "Review NPI number and resubmit",
            "prior auth required": "Obtain prior authorization before resubmission",
            "form incomplete": "Complete all required form fields and resubmit"
        }
        
        return recommendations.get(normalized_reason, f"Address '{denial_reason}' and resubmit")

class ClaimsDataNormalizer:
    """Handles normalization of claims data from different sources"""
    
    @staticmethod
    def normalize_date(date_value: Union[str, None]) -> Optional[str]:
        """
        Normalize date to ISO format
        
        Args:
            date_value: Date string in various formats
            
        Returns:
            str: ISO formatted date string or None
        """
        if not date_value:
            return None
            
        try:
            # Handle different date formats
            if 'T' in str(date_value):  # ISO format with time
                dt = datetime.fromisoformat(date_value.replace('T00:00:00', ''))
            else:  # Simple date format
                dt = datetime.strptime(str(date_value), '%Y-%m-%d')
            
            return dt.date().isoformat()
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to normalize date '{date_value}': {e}")
            return None

    @classmethod
    def normalize_alpha_data(cls, raw_data: List[Dict]) -> List[NormalizedClaim]:
        """
        Normalize data from Alpha EMR system (CSV format)
        
        Args:
            raw_data: List of dictionaries from CSV
            
        Returns:
            List[NormalizedClaim]: Normalized claim objects
        """
        normalized_claims = []
        
        for record in raw_data:
            try:
                # Handle empty patient_id
                patient_id = record.get('patient_id')
                if patient_id == '':
                    patient_id = None
                
                # Handle denial reason (None for approved claims)
                denial_reason = record.get('denial_reason')
                if denial_reason == 'None':
                    denial_reason = None
                
                normalized_claim = NormalizedClaim(
                    claim_id=record['claim_id'],
                    patient_id=patient_id,
                    procedure_code=record['procedure_code'],
                    denial_reason=denial_reason,
                    status=record['status'].lower(),
                    submitted_at=cls.normalize_date(record['submitted_at']),
                    source_system=SourceSystem.ALPHA.value
                )
                normalized_claims.append(normalized_claim)
                
            except KeyError as e:
                logger.error(f"Missing required field in Alpha data: {e}")
            except Exception as e:
                logger.error(f"Error normalizing Alpha record {record}: {e}")
        
        return normalized_claims

    @classmethod
    def normalize_beta_data(cls, raw_data: List[Dict]) -> List[NormalizedClaim]:
        """
        Normalize data from Beta EMR system (JSON format)
        
        Args:
            raw_data: List of dictionaries from JSON
            
        Returns:
            List[NormalizedClaim]: Normalized claim objects
        """
        normalized_claims = []
        
        for record in raw_data:
            try:
                normalized_claim = NormalizedClaim(
                    claim_id=record['id'],
                    patient_id=record.get('member'),  # Different field name in Beta
                    procedure_code=record['code'],
                    denial_reason=record.get('error_msg'),  # Different field name in Beta
                    status=record['status'].lower(),
                    submitted_at=cls.normalize_date(record['date']),
                    source_system=SourceSystem.BETA.value
                )
                normalized_claims.append(normalized_claim)
                
            except KeyError as e:
                logger.error(f"Missing required field in Beta data: {e}")
            except Exception as e:
                logger.error(f"Error normalizing Beta record {record}: {e}")
        
        return normalized_claims

class ClaimsResubmissionPipeline:
    """Main pipeline class for processing claims data"""
    
    def __init__(self, reference_date: str = "2025-07-30"):
        """
        Initialize the pipeline
        
        Args:
            reference_date: Reference date for calculating claim age
        """
        self.reference_date = datetime.strptime(reference_date, "%Y-%m-%d").date()
        self.metrics = PipelineMetrics()
        self.normalizer = ClaimsDataNormalizer()
        self.classifier = DenialReasonClassifier()
        self.failed_records = []

    def load_alpha_data(self, file_path: str) -> List[Dict]:
        """
        Load data from Alpha EMR CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List[Dict]: Raw data from CSV
        """
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                data = list(reader)
                logger.info(f"Loaded {len(data)} records from Alpha EMR")
                return data
        except FileNotFoundError:
            logger.error(f"Alpha EMR file not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error loading Alpha EMR data: {e}")
            return []

    def load_beta_data(self, file_path: str) -> List[Dict]:
        """
        Load data from Beta EMR JSON file
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List[Dict]: Raw data from JSON
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                logger.info(f"Loaded {len(data)} records from Beta EMR")
                return data
        except FileNotFoundError:
            logger.error(f"Beta EMR file not found: {file_path}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in Beta EMR file: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading Beta EMR data: {e}")
            return []

    def is_eligible_for_resubmission(self, claim: NormalizedClaim) -> bool:
        """
        Determine if a claim is eligible for resubmission
        
        Args:
            claim: Normalized claim object
            
        Returns:
            bool: True if eligible for resubmission
        """
        # Check all eligibility criteria
        
        # 1. Status must be denied
        if claim.status != ClaimStatus.DENIED.value:
            self.metrics.excluded_claims["status_not_denied"] = \
                self.metrics.excluded_claims.get("status_not_denied", 0) + 1
            return False
        
        # 2. Patient ID must not be null
        if not claim.patient_id:
            self.metrics.excluded_claims["missing_patient_id"] = \
                self.metrics.excluded_claims.get("missing_patient_id", 0) + 1
            return False
        
        # 3. Claim must be more than 7 days old
        if claim.submitted_at:
            try:
                submitted_date = datetime.fromisoformat(claim.submitted_at).date()
                days_ago = (self.reference_date - submitted_date).days
                if days_ago <= 7:
                    self.metrics.excluded_claims["too_recent"] = \
                        self.metrics.excluded_claims.get("too_recent", 0) + 1
                    return False
            except ValueError:
                logger.warning(f"Invalid date format for claim {claim.claim_id}")
                return False
        else:
            # No submission date, can't determine age
            self.metrics.excluded_claims["missing_date"] = \
                self.metrics.excluded_claims.get("missing_date", 0) + 1
            return False
        
        # 4. Denial reason must be retryable
        if not self.classifier.is_retryable(claim.denial_reason):
            self.metrics.excluded_claims["non_retryable_reason"] = \
                self.metrics.excluded_claims.get("non_retryable_reason", 0) + 1
            return False
        
        return True

    def process_claims(self, alpha_file: str = None, beta_file: str = None) -> List[ResubmissionCandidate]:
        """
        Process claims from multiple sources and identify resubmission candidates
        
        Args:
            alpha_file: Path to Alpha EMR CSV file
            beta_file: Path to Beta EMR JSON file
            
        Returns:
            List[ResubmissionCandidate]: Claims eligible for resubmission
        """
        all_normalized_claims = []
        resubmission_candidates = []
        
        # Process Alpha EMR data
        if alpha_file:
            try:
                alpha_raw_data = self.load_alpha_data(alpha_file)
                alpha_normalized = self.normalizer.normalize_alpha_data(alpha_raw_data)
                all_normalized_claims.extend(alpha_normalized)
                self.metrics.claims_by_source[SourceSystem.ALPHA.value] = len(alpha_normalized)
            except Exception as e:
                logger.error(f"Failed to process Alpha EMR data: {e}")
                self.metrics.processing_errors += 1
        
        # Process Beta EMR data
        if beta_file:
            try:
                beta_raw_data = self.load_beta_data(beta_file)
                beta_normalized = self.normalizer.normalize_beta_data(beta_raw_data)
                all_normalized_claims.extend(beta_normalized)
                self.metrics.claims_by_source[SourceSystem.BETA.value] = len(beta_normalized)
            except Exception as e:
                logger.error(f"Failed to process Beta EMR data: {e}")
                self.metrics.processing_errors += 1
        
        # Update total claims processed
        self.metrics.total_claims_processed = len(all_normalized_claims)
        
        # Process each claim for resubmission eligibility
        for claim in all_normalized_claims:
            try:
                if self.is_eligible_for_resubmission(claim):
                    candidate = ResubmissionCandidate(
                        claim_id=claim.claim_id,
                        resubmission_reason=claim.denial_reason or "Unknown reason",
                        source_system=claim.source_system,
                        recommended_changes=self.classifier.get_recommended_changes(claim.denial_reason)
                    )
                    resubmission_candidates.append(candidate)
                    self.metrics.flagged_for_resubmission += 1
            except Exception as e:
                logger.error(f"Error processing claim {claim.claim_id}: {e}")
                self.failed_records.append(claim.to_dict())
                self.metrics.processing_errors += 1
        
        logger.info(f"Processing complete. {len(resubmission_candidates)} claims flagged for resubmission")
        return resubmission_candidates

    def save_results(self, candidates: List[ResubmissionCandidate], output_file: str = "resubmission_candidates.json"):
        """
        Save resubmission candidates to JSON file
        
        Args:
            candidates: List of resubmission candidates
            output_file: Output file path
        """
        try:
            output_data = [candidate.to_dict() for candidate in candidates]
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(output_data, file, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def save_failed_records(self, output_file: str = "failed_records.json"):
        """
        Save failed records to a separate rejection log file
        
        Args:
            output_file: Output file path for failed records
        """
        # Always create the file, even if empty, for audit purposes
        try:
            rejection_data = {
                "timestamp": datetime.now().isoformat(),
                "total_failed_records": len(self.failed_records),
                "processing_errors": self.metrics.processing_errors,
                "failed_records": self.failed_records
            }
            
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(rejection_data, file, indent=2, ensure_ascii=False)
            
            if self.failed_records:
                logger.warning(f"Rejection log created with {len(self.failed_records)} failed records: {output_file}")
            else:
                logger.info(f"Rejection log created (no failed records): {output_file}")
                
        except Exception as e:
            logger.error(f"Failed to save rejection log: {e}")

    def print_metrics(self):
        """Print pipeline execution metrics"""
        print("\n" + "="*50)
        print("PIPELINE EXECUTION METRICS")
        print("="*50)
        print(f"Total claims processed: {self.metrics.total_claims_processed}")
        print(f"Processing errors: {self.metrics.processing_errors}")
        print(f"Claims flagged for resubmission: {self.metrics.flagged_for_resubmission}")
        
        print(f"\nClaims by source system:")
        for source, count in self.metrics.claims_by_source.items():
            print(f"  {source}: {count}")
        
        print(f"\nExcluded claims breakdown:")
        for reason, count in self.metrics.excluded_claims.items():
            print(f"  {reason.replace('_', ' ').title()}: {count}")
        print("="*50)

def create_sample_data():
    """Create sample data files for testing"""
    
    # Sample Alpha EMR data (CSV)
    alpha_data = [
        ["claim_id", "patient_id", "procedure_code", "denial_reason", "submitted_at", "status"],
        ["A123", "P001", "99213", "Missing modifier", "2025-07-01", "denied"],
        ["A124", "P002", "99214", "Incorrect NPI", "2025-07-10", "denied"],
        ["A125", "", "99215", "Authorization expired", "2025-07-05", "denied"],
        ["A126", "P003", "99381", "None", "2025-07-15", "approved"],
        ["A127", "P004", "99401", "Prior auth required", "2025-07-20", "denied"]
    ]
    
    with open("emr_alpha.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(alpha_data)
    
    # Sample Beta EMR data (JSON)
    beta_data = [
        {
            "id": "B987",
            "member": "P010",
            "code": "99213",
            "error_msg": "Incorrect provider type",
            "date": "2025-07-03T00:00:00",
            "status": "denied"
        },
        {
            "id": "B988",
            "member": "P011",
            "code": "99214",
            "error_msg": "Missing modifier",
            "date": "2025-07-09T00:00:00",
            "status": "denied"
        },
        {
            "id": "B989",
            "member": "P012",
            "code": "99215",
            "error_msg": None,
            "date": "2025-07-10T00:00:00",
            "status": "approved"
        },
        {
            "id": "B990",
            "member": None,
            "code": "99401",
            "error_msg": "incorrect procedure",
            "date": "2025-07-01T00:00:00",
            "status": "denied"
        }
    ]
    
    with open("emr_beta.json", "w", encoding="utf-8") as f:
        json.dump(beta_data, f, indent=2)
    
    print("Sample data files created: emr_alpha.csv, emr_beta.json")

def main():
    """Main execution function"""
    
    # Create sample data files for testing
    create_sample_data()
    
    # Initialize the pipeline
    pipeline = ClaimsResubmissionPipeline(reference_date="2025-07-30")
    
    # Process claims from both sources
    logger.info("Starting claims resubmission pipeline...")
    candidates = pipeline.process_claims(
        alpha_file="emr_alpha.csv",
        beta_file="emr_beta.json"
    )
    
    # Save results
    pipeline.save_results(candidates)
    pipeline.save_failed_records()
    
    # Print metrics
    pipeline.print_metrics()
    
    # Print sample results
    print(f"\nSample resubmission candidates:")
    for i, candidate in enumerate(candidates[:3], 1):
        print(f"{i}. Claim {candidate.claim_id}: {candidate.resubmission_reason}")
        print(f"   Source: {candidate.source_system}")
        print(f"   Recommended changes: {candidate.recommended_changes}")

if __name__ == "__main__":
    main()