"""
Dagster Orchestration Pipeline for Healthcare Claims Processing
===============================================================

A workflow orchestration pipeline using Dagster for processing
healthcare claims data with proper data lineage and monitoring.

Author: Glorry Sibomana
Date: 2025-08-20

Installation:
pip install dagster dagster-webserver dagster-postgres dagster-duckdb

Usage:
dagster-webserver -f dagster_pipeline.py
"""

from dagster import (
    asset, 
    AssetExecutionContext, 
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    Config,
    MaterializeResult,
    MetadataValue,
    job,
    op,
    In,
    Out,
    DependencyDefinition,
    GraphDefinition,
    schedule,
    sensor,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    resource,
    InitResourceContext,
    get_dagster_logger,
    Definitions
)
from dagster_duckdb import DuckDBResource
from typing import List, Dict, Any, Optional
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path

# Import our main pipeline components
try:
    from healthcare_pipeline import (
        ClaimsResubmissionPipeline,
        ResubmissionCandidate,
        DenialReasonClassifier
    )
except ImportError:
    # Mock implementation for standalone usage
    class ClaimsResubmissionPipeline:
        def __init__(self, reference_date):
            self.reference_date = reference_date
            
        def process_claims(self, alpha_file=None, beta_file=None):
            return []
            
        def save_results(self, candidates, output_file):
            pass
    
    class ResubmissionCandidate:
        pass

# Configuration classes
class PipelineConfig(Config):
    """Configuration for the claims processing pipeline"""
    reference_date: str = "2025-07-30"
    alpha_file_path: Optional[str] = None
    beta_file_path: Optional[str] = None
    output_directory: str = "./output"

# Resources
@resource
def claims_pipeline_resource(init_context: InitResourceContext):
    """Resource for the claims processing pipeline"""
    return ClaimsResubmissionPipeline(
        reference_date=init_context.resource_config.get("reference_date", "2025-07-30")
    )

# Partitions
daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")
source_partitions = StaticPartitionsDefinition(["alpha", "beta"])

# Assets
@asset(
    group_name="raw_data",
    partitions_def=source_partitions,
    description="Raw claims data from EMR systems"
)
def raw_claims_data(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset representing raw claims data from different EMR systems
    
    Returns:
        Dict containing raw claims data and metadata
    """
    logger = get_dagster_logger()
    partition_key = context.partition_key
    
    # Mock data loading based on partition
    if partition_key == "alpha":
        # Simulate loading Alpha EMR data
        sample_data = [
            {
                "claim_id": "A123",
                "patient_id": "P001",
                "procedure_code": "99213",
                "denial_reason": "Missing modifier",
                "submitted_at": "2025-07-01",
                "status": "denied"
            },
            {
                "claim_id": "A124",
                "patient_id": "P002",
                "procedure_code": "99214",
                "denial_reason": "Incorrect NPI",
                "submitted_at": "2025-07-10",
                "status": "denied"
            }
        ]
        logger.info(f"Loaded {len(sample_data)} records from Alpha EMR")
        
    elif partition_key == "beta":
        # Simulate loading Beta EMR data
        sample_data = [
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
            }
        ]
        logger.info(f"Loaded {len(sample_data)} records from Beta EMR")
    
    else:
        sample_data = []
    
    return {
        "data": sample_data,
        "source_system": partition_key,
        "record_count": len(sample_data),
        "loaded_at": datetime.now().isoformat()
    }

@asset(
    group_name="processed_data",
    description="Normalized claims data with unified schema",
    deps=[raw_claims_data]
)
def normalized_claims_data(context: AssetExecutionContext, raw_claims_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Asset representing normalized claims data
    
    Args:
        raw_claims_data: Raw claims data from EMR systems
        
    Returns:
        DataFrame containing normalized claims data
    """
    logger = get_dagster_logger()
    
    # Extract data from upstream asset
    data = raw_claims_data["data"]
    source_system = raw_claims_data["source_system"]
    
    if not data:
        logger.warning(f"No data available for source system: {source_system}")
        return pd.DataFrame()
    
    # Normalize based on source system
    if source_system == "alpha":
        # Alpha EMR normalization
        normalized_records = []
        for record in data:
            normalized_record = {
                "claim_id": record["claim_id"],
                "patient_id": record.get("patient_id"),
                "procedure_code": record["procedure_code"],
                "denial_reason": record.get("denial_reason"),
                "status": record["status"].lower(),
                "submitted_at": record["submitted_at"],
                "source_system": "alpha"
            }
            normalized_records.append(normalized_record)
    
    elif source_system == "beta":
        # Beta EMR normalization
        normalized_records = []
        for record in data:
            # Handle date normalization
            submitted_date = record["date"]
            if "T" in submitted_date:
                submitted_date = submitted_date.split("T")[0]
            
            normalized_record = {
                "claim_id": record["id"],
                "patient_id": record.get("member"),
                "procedure_code": record["code"],
                "denial_reason": record.get("error_msg"),
                "status": record["status"].lower(),
                "submitted_at": submitted_date,
                "source_system": "beta"
            }
            normalized_records.append(normalized_record)
    
    df = pd.DataFrame(normalized_records)
    logger.info(f"Normalized {len(df)} records from {source_system} system")
    
    context.add_output_metadata({
        "record_count": len(df),
        "source_system": source_system,
        "columns": MetadataValue.json(list(df.columns)),
        "preview": MetadataValue.md(df.head().to_markdown())
    })
    
    return df

@asset(
    group_name="processed_data",
    description="Combined normalized data from all sources",
    deps=[normalized_claims_data]
)
def unified_claims_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Asset representing unified claims data from all sources
    
    Returns:
        DataFrame containing all normalized claims data
    """
    logger = get_dagster_logger()
    
    # In a real implementation, this would combine data from multiple partitions
    # For this example, we'll simulate combining data
    
    # This would typically load all partitions of normalized_claims_data
    # For simplicity, we're creating sample unified data
    unified_data = [
        {
            "claim_id": "A123",
            "patient_id": "P001",
            "procedure_code": "99213",
            "denial_reason": "Missing modifier",
            "status": "denied",
            "submitted_at": "2025-07-01",
            "source_system": "alpha"
        },
        {
            "claim_id": "B988",
            "patient_id": "P011",
            "procedure_code": "99214",
            "denial_reason": "Missing modifier",
            "status": "denied",
            "submitted_at": "2025-07-09",
            "source_system": "beta"
        }
    ]
    
    df = pd.DataFrame(unified_data)
    logger.info(f"Unified {len(df)} total claims from all sources")
    
    context.add_output_metadata({
        "total_records": len(df),
        "sources": MetadataValue.json(df["source_system"].unique().tolist()),
        "status_distribution": MetadataValue.json(df["status"].value_counts().to_dict()),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
    return df

@asset(
    group_name="analysis",
    description="Claims eligible for resubmission",
    deps=[unified_claims_data]
)
def resubmission_candidates(
    context: AssetExecutionContext, 
    unified_claims_data: pd.DataFrame,
    config: PipelineConfig
) -> List[Dict[str, Any]]:
    """
    Asset representing claims eligible for resubmission
    
    Args:
        unified_claims_data: Unified claims data
        config: Pipeline configuration
        
    Returns:
        List of resubmission candidates
    """
    logger = get_dagster_logger()
    
    # Initialize the pipeline
    pipeline = ClaimsResubmissionPipeline(reference_date=config.reference_date)
    classifier = DenialReasonClassifier()
    
    candidates = []
    reference_date = datetime.strptime(config.reference_date, "%Y-%m-%d").date()
    
    for _, row in unified_claims_data.iterrows():
        # Apply resubmission eligibility logic
        if (row["status"] == "denied" and 
            pd.notna(row["patient_id"]) and 
            row["patient_id"] is not None):
            
            # Check if claim is old enough (more than 7 days)
            if pd.notna(row["submitted_at"]):
                try:
                    submitted_date = datetime.strptime(str(row["submitted_at"]), "%Y-%m-%d").date()
                    days_ago = (reference_date - submitted_date).days
                    
                    if days_ago > 7:
                        # Check if denial reason is retryable
                        if classifier.is_retryable(row["denial_reason"]):
                            candidate = {
                                "claim_id": row["claim_id"],
                                "resubmission_reason": row["denial_reason"] or "Unknown reason",
                                "source_system": row["source_system"],
                                "recommended_changes": classifier.get_recommended_changes(row["denial_reason"]),
                                "days_since_submission": days_ago
                            }
                            candidates.append(candidate)
                except ValueError:
                    logger.warning(f"Invalid date format for claim {row['claim_id']}")
    
    logger.info(f"Identified {len(candidates)} resubmission candidates")
    
    context.add_output_metadata({
        "candidate_count": len(candidates),
        "eligibility_rate": len(candidates) / len(unified_claims_data) if len(unified_claims_data) > 0 else 0,
        "candidates_by_source": MetadataValue.json(
            pd.DataFrame(candidates)["source_system"].value_counts().to_dict() if candidates else {}
        ),
        "sample_candidates": MetadataValue.json(candidates[:5])
    })
    
    return candidates

@asset(
    group_name="output",
    description="Exported resubmission candidates",
    deps=[resubmission_candidates]
)
def exported_resubmission_data(
    context: AssetExecutionContext,
    resubmission_candidates: List[Dict[str, Any]],
    config: PipelineConfig
) -> str:
    """
    Asset representing exported resubmission data
    
    Args:
        resubmission_candidates: List of resubmission candidates
        config: Pipeline configuration
        
    Returns:
        Path to exported file
    """
    logger = get_dagster_logger()
    
    # Create output directory
    output_dir = Path(config.output_directory)
    output_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"resubmission_candidates_{timestamp}.json"
    
    # Export data
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(resubmission_candidates, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Exported {len(resubmission_candidates)} candidates to {output_file}")
    
    context.add_output_metadata({
        "output_file": str(output_file),
        "file_size_bytes": output_file.stat().st_size,
        "export_timestamp": datetime.now().isoformat(),
        "record_count": len(resubmission_candidates)
    })
    
    return str(output_file)

# Operations for the job-based approach
@op(
    config_schema={"reference_date": str},
    out=Out(Dict[str, Any])
)
def load_raw_data(context) -> Dict[str, Any]:
    """Load raw claims data"""
    reference_date = context.op_config["reference_date"]
    
    # Mock data loading
    sample_data = [
        {
            "claim_id": "A123",
            "patient_id": "P001",
            "procedure_code": "99213",
            "denial_reason": "Missing modifier",
            "submitted_at": "2025-07-01",
            "status": "denied"
        }
    ]
    
    return {
        "data": sample_data,
        "loaded_at": datetime.now().isoformat(),
        "reference_date": reference_date
    }

@op(
    ins={"raw_data": In(Dict[str, Any])},
    out=Out(List[Dict[str, Any]])
)
def process_claims_op(context, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process claims and identify resubmission candidates"""
    
    # Initialize pipeline
    reference_date = raw_data["reference_date"]
    pipeline = ClaimsResubmissionPipeline(reference_date=reference_date)
    
    # Mock processing
    candidates = [
        {
            "claim_id": "A123",
            "resubmission_reason": "Missing modifier",
            "source_system": "alpha",
            "recommended_changes": "Add appropriate modifier and resubmit"
        }
    ]
    
    context.log.info(f"Processed claims and found {len(candidates)} resubmission candidates")
    
    return candidates

@op(
    ins={"candidates": In(List[Dict[str, Any]])},
    config_schema={"output_path": str}
)
def export_results_op(context, candidates: List[Dict[str, Any]]):
    """Export resubmission candidates to file"""
    
    output_path = context.op_config["output_path"]
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(candidates, f, indent=2)
    
    context.log.info(f"Exported {len(candidates)} candidates to {output_path}")

# Job definition
claims_processing_graph = GraphDefinition(
    name="claims_processing_graph",
    node_defs=[load_raw_data, process_claims_op, export_results_op],
    dependencies={
        "process_claims_op": {"raw_data": DependencyDefinition("load_raw_data")},
        "export_results_op": {"candidates": DependencyDefinition("process_claims_op")}
    }
)

@job
def claims_processing_job():
    """Job for processing claims data"""
    claims_processing_graph()

# Schedule
@schedule(
    job=claims_processing_job,
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
)
def daily_claims_processing_schedule(context):
    """Daily schedule for processing claims"""
    
    run_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    
    return {
        "ops": {
            "load_raw_data": {
                "config": {
                    "reference_date": run_date
                }
            },
            "export_results_op": {
                "config": {
                    "output_path": f"./output/resubmission_candidates_{run_date}.json"
                }
            }
        }
    }

# Sensor
@sensor(
    job=claims_processing_job,
    default_status=DefaultSensorStatus.STOPPED
)
def new_claims_file_sensor(context: SensorEvaluationContext):
    """Sensor that triggers when new claims files are detected"""
    
    # Check for new files in input directory
    input_dir = Path("./input")
    
    if not input_dir.exists():
        return SkipReason("Input directory does not exist")
    
    # Look for new CSV or JSON files
    new_files = [
        f for f in input_dir.iterdir() 
        if f.suffix in ['.csv', '.json'] and f.stat().st_mtime > context.cursor or 0
    ]
    
    if not new_files:
        return SkipReason("No new files detected")
    
    # Update cursor to latest file modification time
    latest_mtime = max(f.stat().st_mtime for f in new_files)
    context.update_cursor(str(latest_mtime))
    
    # Create run request for each new file
    for file_path in new_files:
        yield RunRequest(
            run_key=f"file_{file_path.stem}_{int(file_path.stat().st_mtime)}",
            run_config={
                "ops": {
                    "load_raw_data": {
                        "config": {
                            "reference_date": datetime.now().strftime("%Y-%m-%d")
                        }
                    },
                    "export_results_op": {
                        "config": {
                            "output_path": f"./output/resubmission_candidates_{file_path.stem}.json"
                        }
                    }
                }
            }
        )

# Resource definitions
duckdb_resource = DuckDBResource(database="claims_pipeline.duckdb")

# Definitions
defs = Definitions(
    assets=[
        raw_claims_data,
        normalized_claims_data,
        unified_claims_data,
        resubmission_candidates,
        exported_resubmission_data
    ],
    jobs=[claims_processing_job],
    schedules=[daily_claims_processing_schedule],
    sensors=[new_claims_file_sensor],
    resources={
        "duckdb": duckdb_resource,
        "claims_pipeline": claims_pipeline_resource
    }
)

if __name__ == "__main__":
    print("Dagster Pipeline Definition")
    print("=" * 50)
    print("Assets:")
    for asset in defs.get_asset_graph().all_asset_keys:
        print(f"  - {asset.to_user_string()}")
    
    print("\nJobs:")
    for job in defs.get_job_names():
        print(f"  - {job}")
    
    print("\nSchedules:")
    for schedule in defs.get_schedule_names():
        print(f"  - {schedule}")
    
    print("\nSensors:")
    for sensor in defs.get_sensor_names():
        print(f"  - {sensor}")
    
    print("\nTo run the Dagster webserver:")
    print("dagster-webserver -f dagster_pipeline.py")
    print("\nTo materialize assets:")
    print("dagster asset materialize -f dagster_pipeline.py --select '*'")