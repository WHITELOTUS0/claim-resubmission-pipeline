# Healthcare Claims Resubmission Pipeline

## Overview

This comprehensive solution provides a robust pipeline for healthcare data engineering teams to ingest, normalize, and analyze insurance claim data from multiple Electronic Medical Records (EMR) systems. The pipeline identifies claims eligible for resubmission based on defined business rules and machine learning inference.

## Features

- **Multi-source Data Ingestion**: Supports CSV and JSON formats from different EMR systems
- **Schema Normalization**: Unifies disparate data formats into a common schema
- **Intelligent Classification**: Uses rule-based and heuristic classification for denial reasons
- **Robust Error Handling**: Gracefully handles malformed and missing data
- **Comprehensive Logging**: Detailed logging and metrics for monitoring
- **API Interface**: FastAPI endpoints for integration with other systems
- **Workflow Orchestration**: Dagster pipeline for production deployment
- **Extensive Testing**: Complete test suite with unit and integration tests

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  EMR Alpha      │    │  EMR Beta        │    │  Other Sources  │
│  (CSV Format)   │    │  (JSON Format)   │    │                 │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          ▼                      ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Ingestion Layer                        │
│  • File format validation                                      │
│  • Error handling and logging                                  │
│  • Data quality checks                                         │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Normalization Layer                             │
│  • Schema mapping and transformation                           │
│  • Date normalization                                          │
│  • Data type conversion                                        │
│  • Null value handling                                         │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                Business Logic Layer                             │
│  • Eligibility criteria evaluation                             │
│  • Denial reason classification                                │
│  • Resubmission candidate identification                       │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Output Layer                                  │
│  • JSON export                                                 │
│  • Metrics and reporting                                       │
│  • Failed record handling                                      │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

```bash
# Python 3.8+ required
pip install pandas fastapi uvicorn dagster dagster-webserver pytest psutil
```

### Basic Usage

1. **Create sample data files**:
```python
from healthcare_pipeline import create_sample_data
create_sample_data()
```

2. **Run the basic pipeline**:
```python
from healthcare_pipeline import main
main()
```

3. **Check results**:
```bash
cat resubmission_candidates.json
```

### API Usage

Start the FastAPI server:
```bash
python -m uvicorn fastapi_extension:app --reload
```

Upload and process files:
```bash
curl -X POST "http://localhost:8000/process-claims" \
  -F "alpha_file=@emr_alpha.csv" \
  -F "beta_file=@emr_beta.json" \
  -F "reference_date=2025-07-30"
```

### Dagster Orchestration

Launch Dagster UI:
```bash
dagster-webserver -f dagster_pipeline.py
```

Access the UI at `http://localhost:3000` and materialize assets or run jobs.

## Data Formats

### Alpha EMR (CSV Format)
```csv
claim_id,patient_id,procedure_code,denial_reason,submitted_at,status
A123,P001,99213,Missing modifier,2025-07-01,denied
```

### Beta EMR (JSON Format)
```json
[
  {
    "id": "B987",
    "member": "P010", 
    "code": "99213",
    "error_msg": "Incorrect provider type",
    "date": "2025-07-03T00:00:00",
    "status": "denied"
  }
]
```

### Normalized Schema
```json
{
  "claim_id": "string",
  "patient_id": "string or null",
  "procedure_code": "string", 
  "denial_reason": "string or null",
  "status": "approved/denied",
  "submitted_at": "ISO date",
  "source_system": "alpha/beta"
}
```

## Business Rules

### Resubmission Eligibility Criteria

A claim is eligible for resubmission if **ALL** of the following are true:

1. **Status is "denied"**
2. **Patient ID is not null**
3. **Claim was submitted more than 7 days ago** (based on reference date)
4. **Denial reason is classified as retryable**

### Denial Reason Classification

#### Known Retryable Reasons:
- "Missing modifier"
- "Incorrect NPI" 
- "Prior auth required"

#### Known Non-Retryable Reasons:
- "Authorization expired"
- "Incorrect provider type"

#### Ambiguous Reasons (Hardcoded Mappings):
- "incorrect procedure" → Non-retryable
- "form incomplete" → Retryable  
- "not billable" → Non-retryable
- `null` → Non-retryable

## Configuration

### Pipeline Configuration
```python
pipeline = ClaimsResubmissionPipeline(reference_date="2025-07-30")
```

### Logging Configuration
```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('claims_pipeline.log'),
        logging.StreamHandler()
    ]
)
```

## API Endpoints

### Process Claims
```
POST /process-claims
```
- Upload CSV/JSON files
- Returns resubmission candidates and metrics

### Classify Denial Reason  
```
POST /classify-denial-reason
```
- Classify individual denial reasons
- Returns retryability and recommendations

### Get Supported Formats
```
GET /supported-formats
```
- Returns information about supported file formats and requirements

### Health Check
```
GET /
```
- Returns API health status

## Testing

### Run All Tests
```bash
python -m pytest test_suite.py -v
```

### Run Specific Test Classes
```bash
python -m pytest test_suite.py::TestDenialReasonClassifier -v
python -m pytest test_suite.py::TestIntegration -v
```

### Coverage Report
```bash
pip install pytest-cov
python -m pytest test_suite.py --cov=healthcare_pipeline --cov-report=html
```

## Monitoring and Metrics

### Pipeline Metrics
- Total claims processed
- Claims by source system
- Resubmission candidates flagged
- Exclusion reasons breakdown
- Processing errors

### Logging Levels
- **INFO**: Normal operation messages
- **WARNING**: Non-fatal issues (malformed dates, missing fields)
- **ERROR**: Processing errors, file access issues
- **DEBUG**: Detailed processing information

### Example Metrics Output
```
PIPELINE EXECUTION METRICS
==================================================
Total claims processed: 9
Processing errors: 0
Claims flagged for resubmission: 3

Claims by source system:
  alpha: 5
  beta: 4

Excluded claims breakdown:
  Status Not Denied: 2
  Missing Patient Id: 2
  Too Recent: 1
  Non Retryable Reason: 1
==================================================
```

## Deployment

### Development Environment
```bash
# Clone repository
git clone <repository-url>
cd healthcare-claims-pipeline

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest test_suite.py

# Start development server
python healthcare_pipeline.py
```

### Production Deployment

#### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000
CMD ["uvicorn", "fastapi_extension:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: claims-pipeline
spec:
  replicas: 3
  selector:
    matchLabels:
      app: claims-pipeline
  template:
    metadata:
      labels:
        app: claims-pipeline
    spec:
      containers:
      - name: claims-pipeline
        image: claims-pipeline:latest
        ports:
        - containerPort: 8000
        env:
        - name: LOG_LEVEL
          value: "INFO"
```

#### Environment Variables
```bash
# Production settings
export LOG_LEVEL=INFO
export REFERENCE_DATE=2025-07-30
export OUTPUT_DIRECTORY=/app/output
export DATABASE_URL=postgresql://user:pass@host:5432/claims
```

## Performance Considerations

### Optimization Tips
1. **Batch Processing**: Process files in batches for large datasets
2. **Memory Management**: Use generators for large file processing
3. **Caching**: Cache classification results for repeated denial reasons
4. **Database Integration**: Use database storage for large-scale operations
5. **Parallel Processing**: Use multiprocessing for independent file processing

### Scalability Guidelines
- **Small datasets** (< 1,000 claims): Single-threaded processing
- **Medium datasets** (1,000 - 100,000 claims): Batch processing
- **Large datasets** (> 100,000 claims): Distributed processing with Dagster

### Memory Usage
- Approximately 1MB RAM per 1,000 claims
- Temporary files cleaned up automatically
- Failed records logged separately to prevent memory buildup

## Troubleshooting

### Common Issues

#### 1. File Format Errors
```
Error: Invalid JSON format in beta file
Solution: Validate JSON syntax using online validator
```

#### 2. Date Parsing Issues  
```
Warning: Failed to normalize date '2025-13-01'
Solution: Ensure dates are in YYYY-MM-DD format
```

#### 3. Missing Required Fields
```
Error: Missing required field in Alpha data: 'claim_id' 
Solution: Verify CSV headers match expected format
```

#### 4. API Connection Issues
```
Error: Connection refused on port 8000
Solution: Ensure FastAPI server is running: uvicorn fastapi_extension:app
```

### Debug Mode
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)

pipeline = ClaimsResubmissionPipeline(reference_date="2025-07-30")
# Detailed debug output will be shown
```

### Log File Analysis
```bash
# View recent errors
grep "ERROR" claims_pipeline.log | tail -20

# Count processing statistics
grep "claims processed" claims_pipeline.log | wc -l

# Monitor real-time logs
tail -f claims_pipeline.log
```

## Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run test suite: `python -m pytest test_suite.py`
5. Update documentation as needed
6. Submit pull request

### Code Standards
- Follow PEP 8 style guidelines
- Add docstrings to all functions/classes
- Include type hints where appropriate
- Maintain > 90% test coverage
- Add logging for important operations

### Adding New EMR Sources
1. Extend `ClaimsDataNormalizer` with new normalization method
2. Add source enum to `SourceSystem`
3. Update `process_claims()` method 
4. Add comprehensive tests
5. Update documentation

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Support

For technical support:
- Create an issue in the repository
- Email: support@healthcare-pipeline.com
- Documentation: https://docs.healthcare-pipeline.com

---

**Version**: 1.0.0  
**Last Updated**: August 20, 2025  
**Author**: Glorry Sibomana