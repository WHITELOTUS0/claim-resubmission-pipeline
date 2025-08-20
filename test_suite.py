"""
Comprehensive Test Suite for Healthcare Claims Pipeline
======================================================

Unit tests and integration tests for the claims resubmission pipeline.

Author: Glorry Sibomana
Date: 2025-08-20

Run tests with: python -m pytest test_suite.py -v
"""

import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import csv
import tempfile
import os
from datetime import datetime, date
from io import StringIO
from typing import List, Dict

# Import modules to test
try:
    from healthcare_pipeline import (
        ClaimsResubmissionPipeline,
        ClaimsDataNormalizer,
        DenialReasonClassifier,
        NormalizedClaim,
        ResubmissionCandidate,
        ClaimStatus,
        SourceSystem
    )
except ImportError:
    # If running standalone, create mock classes
    print("Warning: Main pipeline module not found. Using mock classes for testing.")
    
    class ClaimsResubmissionPipeline:
        def __init__(self, reference_date="2025-07-30"):
            self.reference_date = reference_date
    
    class ClaimsDataNormalizer:
        pass
    
    class DenialReasonClassifier:
        pass
    
    class NormalizedClaim:
        pass
    
    class ResubmissionCandidate:
        pass

class TestDenialReasonClassifier(unittest.TestCase):
    """Test cases for the DenialReasonClassifier"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.classifier = DenialReasonClassifier()
    
    def test_retryable_reasons(self):
        """Test known retryable denial reasons"""
        retryable_reasons = [
            "Missing modifier",
            "Incorrect NPI",
            "Prior auth required",
            "MISSING MODIFIER",  # Test case insensitivity
            " incorrect npi "     # Test whitespace handling
        ]
        
        for reason in retryable_reasons:
            with self.subTest(reason=reason):
                self.assertTrue(
                    self.classifier.is_retryable(reason),
                    f"'{reason}' should be retryable"
                )
    
    def test_non_retryable_reasons(self):
        """Test known non-retryable denial reasons"""
        non_retryable_reasons = [
            "Authorization expired",
            "Incorrect provider type",
            "AUTHORIZATION EXPIRED",  # Test case insensitivity
            " incorrect provider type "  # Test whitespace handling
        ]
        
        for reason in non_retryable_reasons:
            with self.subTest(reason=reason):
                self.assertFalse(
                    self.classifier.is_retryable(reason),
                    f"'{reason}' should not be retryable"
                )
    
    def test_ambiguous_reasons(self):
        """Test ambiguous denial reasons with hardcoded mappings"""
        test_cases = [
            ("incorrect procedure", False),
            ("form incomplete", True),
            ("not billable", False),
            (None, False)
        ]
        
        for reason, expected in test_cases:
            with self.subTest(reason=reason):
                self.assertEqual(
                    self.classifier.is_retryable(reason),
                    expected,
                    f"'{reason}' classification incorrect"
                )
    
    def test_recommended_changes(self):
        """Test recommended changes for different denial reasons"""
        test_cases = [
            ("Missing modifier", "Add appropriate modifier and resubmit"),
            ("Incorrect NPI", "Review NPI number and resubmit"),
            ("Prior auth required", "Obtain prior authorization before resubmission"),
            ("Unknown reason", "Address 'Unknown reason' and resubmit"),
            (None, "Review claim details and resubmit")
        ]
        
        for reason, expected_change in test_cases:
            with self.subTest(reason=reason):
                result = self.classifier.get_recommended_changes(reason)
                self.assertIn(expected_change.split()[0].lower(), result.lower())

class TestClaimsDataNormalizer(unittest.TestCase):
    """Test cases for the ClaimsDataNormalizer"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.normalizer = ClaimsDataNormalizer()
    
    def test_normalize_date_iso_format(self):
        """Test date normalization with ISO format"""
        test_cases = [
            ("2025-07-01T00:00:00", "2025-07-01"),
            ("2025-07-15", "2025-07-15"),
            ("2025-12-31T23:59:59", "2025-12-31")
        ]
        
        for input_date, expected in test_cases:
            with self.subTest(input_date=input_date):
                result = self.normalizer.normalize_date(input_date)
                self.assertEqual(result, expected)
    
    def test_normalize_date_invalid(self):
        """Test date normalization with invalid dates"""
        invalid_dates = [None, "", "invalid-date", "2025-13-01", "not-a-date"]
        
        for invalid_date in invalid_dates:
            with self.subTest(invalid_date=invalid_date):
                result = self.normalizer.normalize_date(invalid_date)
                self.assertIsNone(result)
    
    def test_normalize_alpha_data(self):
        """Test normalization of Alpha EMR data"""
        alpha_data = [
            {
                "claim_id": "A123",
                "patient_id": "P001",
                "procedure_code": "99213",
                "denial_reason": "Missing modifier",
                "submitted_at": "2025-07-01",
                "status": "DENIED"
            },
            {
                "claim_id": "A124",
                "patient_id": "",  # Empty patient ID
                "procedure_code": "99214",
                "denial_reason": "None",  # Approved claim
                "submitted_at": "2025-07-15",
                "status": "approved"
            }
        ]
        
        result = self.normalizer.normalize_alpha_data(alpha_data)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].claim_id, "A123")
        self.assertEqual(result[0].status, "denied")
        self.assertEqual(result[0].source_system, "alpha")
        self.assertIsNone(result[1].patient_id)  # Empty string converted to None
        self.assertIsNone(result[1].denial_reason)  # "None" converted to None
    
    def test_normalize_beta_data(self):
        """Test normalization of Beta EMR data"""
        beta_data = [
            {
                "id": "B987",
                "member": "P010",
                "code": "99213",
                "error_msg": "Incorrect provider type",
                "date": "2025-07-03T00:00:00",
                "status": "DENIED"
            },
            {
                "id": "B988",
                "member": None,  # Null member
                "code": "99214",
                "error_msg": None,  # No error message
                "date": "2025-07-09T00:00:00",
                "status": "approved"
            }
        ]
        
        result = self.normalizer.normalize_beta_data(beta_data)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].claim_id, "B987")
        self.assertEqual(result[0].patient_id, "P010")
        self.assertEqual(result[0].submitted_at, "2025-07-03")
        self.assertEqual(result[0].source_system, "beta")
        self.assertIsNone(result[1].patient_id)
        self.assertIsNone(result[1].denial_reason)

class TestClaimsResubmissionPipeline(unittest.TestCase):
    """Test cases for the main ClaimsResubmissionPipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.pipeline = ClaimsResubmissionPipeline(reference_date="2025-07-30")
    
    def test_eligibility_all_criteria_met(self):
        """Test eligibility when all criteria are met"""
        claim = NormalizedClaim(
            claim_id="A123",
            patient_id="P001",
            procedure_code="99213",
            denial_reason="Missing modifier",
            status="denied",
            submitted_at="2025-07-01",  # More than 7 days ago
            source_system="alpha"
        )
        
        self.assertTrue(self.pipeline.is_eligible_for_resubmission(claim))
    
    def test_eligibility_status_not_denied(self):
        """Test eligibility when status is not denied"""
        claim = NormalizedClaim(
            claim_id="A126",
            patient_id="P003",
            procedure_code="99381",
            denial_reason=None,
            status="approved",  # Not denied
            submitted_at="2025-07-01",
            source_system="alpha"
        )
        
        self.assertFalse(self.pipeline.is_eligible_for_resubmission(claim))
    
    def test_eligibility_missing_patient_id(self):
        """Test eligibility when patient ID is missing"""
        claim = NormalizedClaim(
            claim_id="A125",
            patient_id=None,  # Missing patient ID
            procedure_code="99215",
            denial_reason="Authorization expired",
            status="denied",
            submitted_at="2025-07-01",
            source_system="alpha"
        )
        
        self.assertFalse(self.pipeline.is_eligible_for_resubmission(claim))
    
    def test_eligibility_too_recent(self):
        """Test eligibility when claim is too recent"""
        claim = NormalizedClaim(
            claim_id="A127",
            patient_id="P004",
            procedure_code="99401",
            denial_reason="Prior auth required",
            status="denied",
            submitted_at="2025-07-28",  # Only 2 days ago
            source_system="alpha"
        )
        
        self.assertFalse(self.pipeline.is_eligible_for_resubmission(claim))
    
    def test_eligibility_non_retryable_reason(self):
        """Test eligibility when denial reason is not retryable"""
        claim = NormalizedClaim(
            claim_id="B987",
            patient_id="P010",
            procedure_code="99213",
            denial_reason="Authorization expired",  # Non-retryable
            status="denied",
            submitted_at="2025-07-01",
            source_system="beta"
        )
        
        self.assertFalse(self.pipeline.is_eligible_for_resubmission(claim))
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictReader')
    def test_load_alpha_data(self, mock_csv_reader, mock_file):
        """Test loading Alpha EMR data from CSV"""
        mock_data = [
            {
                "claim_id": "A123",
                "patient_id": "P001",
                "procedure_code": "99213",
                "denial_reason": "Missing modifier",
                "submitted_at": "2025-07-01",
                "status": "denied"
            }
        ]
        mock_csv_reader.return_value = mock_data
        
        result = self.pipeline.load_alpha_data("test_file.csv")
        
        self.assertEqual(result, mock_data)
        mock_file.assert_called_once_with("test_file.csv", 'r', newline='', encoding='utf-8')
    
    @patch('builtins.open', new_callable=mock_open, read_data='[{"id": "B987", "status": "denied"}]')
    def test_load_beta_data(self, mock_file):
        """Test loading Beta EMR data from JSON"""
        result = self.pipeline.load_beta_data("test_file.json")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "B987")
        mock_file.assert_called_once_with("test_file.json", 'r', encoding='utf-8')
    
    def test_load_nonexistent_file(self):
        """Test loading from non-existent file"""
        result_alpha = self.pipeline.load_alpha_data("nonexistent.csv")
        result_beta = self.pipeline.load_beta_data("nonexistent.json")
        
        self.assertEqual(result_alpha, [])
        self.assertEqual(result_beta, [])

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.pipeline = ClaimsResubmissionPipeline(reference_date="2025-07-30")
        
        # Create temporary test files
        self.alpha_data = [
            ["claim_id", "patient_id", "procedure_code", "denial_reason", "submitted_at", "status"],
            ["A123", "P001", "99213", "Missing modifier", "2025-07-01", "denied"],
            ["A124", "P002", "99214", "Incorrect NPI", "2025-07-10", "denied"],
            ["A125", "", "99215", "Authorization expired", "2025-07-05", "denied"],
            ["A126", "P003", "99381", "None", "2025-07-15", "approved"],
            ["A127", "P004", "99401", "Prior auth required", "2025-07-28", "denied"]  # Too recent
        ]
        
        self.beta_data = [
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
    
    def create_temp_files(self):
        """Create temporary test files"""
        # Create temporary Alpha CSV file
        alpha_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.writer(alpha_file)
        writer.writerows(self.alpha_data)
        alpha_file.close()
        
        # Create temporary Beta JSON file
        beta_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.beta_data, beta_file, indent=2)
        beta_file.close()
        
        return alpha_file.name, beta_file.name
    
    def cleanup_temp_files(self, *file_paths):
        """Clean up temporary files"""
        for file_path in file_paths:
            try:
                os.unlink(file_path)
            except OSError:
                pass
    
    def test_end_to_end_processing(self):
        """Test complete end-to-end pipeline processing"""
        alpha_file, beta_file = self.create_temp_files()
        
        try:
            # Process claims
            candidates = self.pipeline.process_claims(
                alpha_file=alpha_file,
                beta_file=beta_file
            )
            
            # Verify results
            self.assertIsInstance(candidates, list)
            self.assertGreater(len(candidates), 0)
            
            # Check that only eligible claims are included
            candidate_ids = {c.claim_id for c in candidates}
            
            # A123 should be eligible (denied, has patient_id, >7 days, retryable reason)
            self.assertIn("A123", candidate_ids)
            
            # A124 should be eligible (denied, has patient_id, >7 days, retryable reason)
            self.assertIn("A124", candidate_ids)
            
            # A125 should NOT be eligible (no patient_id)
            self.assertNotIn("A125", candidate_ids)
            
            # A126 should NOT be eligible (approved status)
            self.assertNotIn("A126", candidate_ids)
            
            # A127 should NOT be eligible (too recent)
            self.assertNotIn("A127", candidate_ids)
            
            # B987 should NOT be eligible (non-retryable reason)
            self.assertNotIn("B987", candidate_ids)
            
            # B988 should be eligible (denied, has patient_id, >7 days, retryable reason)
            self.assertIn("B988", candidate_ids)
            
            # B989 should NOT be eligible (approved status)
            self.assertNotIn("B989", candidate_ids)
            
            # B990 should NOT be eligible (no patient_id)
            self.assertNotIn("B990", candidate_ids)
            
            # Verify metrics
            self.assertEqual(self.pipeline.metrics.total_claims_processed, 9)  # 5 + 4
            self.assertGreater(self.pipeline.metrics.flagged_for_resubmission, 0)
            self.assertIn("alpha", self.pipeline.metrics.claims_by_source)
            self.assertIn("beta", self.pipeline.metrics.claims_by_source)
            
        finally:
            self.cleanup_temp_files(alpha_file, beta_file)
    
    def test_save_results(self):
        """Test saving results to JSON file"""
        # Create sample candidates
        candidates = [
            ResubmissionCandidate(
                claim_id="A123",
                resubmission_reason="Missing modifier",
                source_system="alpha",
                recommended_changes="Add appropriate modifier and resubmit"
            )
        ]
        
        # Create temporary output file
        output_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        output_file.close()
        
        try:
            # Save results
            self.pipeline.save_results(candidates, output_file.name)
            
            # Verify file was created and contains correct data
            with open(output_file.name, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
            
            self.assertEqual(len(saved_data), 1)
            self.assertEqual(saved_data[0]["claim_id"], "A123")
            self.assertEqual(saved_data[0]["source_system"], "alpha")
            
        finally:
            self.cleanup_temp_files(output_file.name)

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.pipeline = ClaimsResubmissionPipeline()
    
    def test_empty_data_processing(self):
        """Test processing with empty data"""
        candidates = self.pipeline.process_claims()
        
        self.assertEqual(len(candidates), 0)
        self.assertEqual(self.pipeline.metrics.total_claims_processed, 0)
    
    def test_malformed_dates(self):
        """Test handling of malformed dates"""
        normalizer = ClaimsDataNormalizer()
        
        malformed_dates = [
            "2025-13-01",  # Invalid month
            "2025-02-30",  # Invalid day
            "not-a-date",  # Not a date at all
            "",            # Empty string
            None           # None value
        ]
        
        for bad_date in malformed_dates:
            with self.subTest(date=bad_date):
                result = normalizer.normalize_date(bad_date)
                self.assertIsNone(result)
    
    def test_missing_required_fields(self):
        """Test handling of missing required fields"""
        incomplete_alpha_data = [
            {
                "claim_id": "A123",
                # Missing patient_id, procedure_code, etc.
                "status": "denied"
            }
        ]
        
        normalizer = ClaimsDataNormalizer()
        
        # This should not raise an exception but should log errors
        result = normalizer.normalize_alpha_data(incomplete_alpha_data)
        
        # Should return empty list for malformed data
        self.assertEqual(len(result), 0)
    
    def test_invalid_json_handling(self):
        """Test handling of invalid JSON files"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{ invalid json }")
            invalid_json_file = f.name
        
        try:
            result = self.pipeline.load_beta_data(invalid_json_file)
            self.assertEqual(result, [])
        finally:
            os.unlink(invalid_json_file)

class TestPerformanceAndScalability(unittest.TestCase):
    """Test performance and scalability aspects"""
    
    def test_large_dataset_processing(self):
        """Test processing of large datasets"""
        # Create a large dataset simulation
        large_alpha_data = []
        for i in range(1000):
            large_alpha_data.append({
                "claim_id": f"A{i:04d}",
                "patient_id": f"P{i:03d}",
                "procedure_code": "99213",
                "denial_reason": "Missing modifier",
                "submitted_at": "2025-07-01",
                "status": "denied"
            })
        
        normalizer = ClaimsDataNormalizer()
        
        # Time the normalization process
        import time
        start_time = time.time()
        result = normalizer.normalize_alpha_data(large_alpha_data)
        end_time = time.time()
        
        # Verify results
        self.assertEqual(len(result), 1000)
        
        # Performance should be reasonable (less than 1 second for 1000 records)
        processing_time = end_time - start_time
        self.assertLess(processing_time, 1.0, f"Processing took {processing_time:.3f} seconds")
    
    def test_memory_usage_with_large_datasets(self):
        """Test memory usage doesn't grow excessively"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Process multiple batches
        normalizer = ClaimsDataNormalizer()
        for batch in range(10):
            batch_data = []
            for i in range(100):
                batch_data.append({
                    "claim_id": f"B{batch:02d}{i:03d}",
                    "patient_id": f"P{i:03d}",
                    "procedure_code": "99213",
                    "denial_reason": "Missing modifier",
                    "submitted_at": "2025-07-01",
                    "status": "denied"
                })
            
            result = normalizer.normalize_alpha_data(batch_data)
            self.assertEqual(len(result), 100)
        
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 50MB)
        self.assertLess(memory_growth, 50 * 1024 * 1024, 
                       f"Memory grew by {memory_growth / 1024 / 1024:.2f} MB")

def create_test_suite():
    """Create a comprehensive test suite"""
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestDenialReasonClassifier,
        TestClaimsDataNormalizer,
        TestClaimsResubmissionPipeline,
        TestIntegration,
        TestEdgeCases,
        TestPerformanceAndScalability
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    return suite

def run_tests():
    """Run all tests with detailed output"""
    import sys
    
    print("Healthcare Claims Pipeline Test Suite")
    print("=" * 50)
    
    # Create and run the test suite
    suite = create_test_suite()
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    # Return exit code
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    import sys
    sys.exit(run_tests())