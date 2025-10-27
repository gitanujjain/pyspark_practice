from typing import Dict, List, Tuple
from enum import Enum

class DeviceStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"

class JobStatusGenerator:
    """Generates job status statements based on device statuses and reprocess results"""
    
    def __init__(self):
        self.statements = {
            "STATEMENT_SUCCESS": "Job execution completed successfully. Status of job given below.",
            "STATEMENT_FAILED": "Job execution has failed. Status of job given below.",
            "STATEMENT_PARTIAL_SUCCESS": "Job execution has partially completed. Status of job given below.",
            "STATEMENT_SUCCESS_AND_FAILURE": "Job execution failed for {failure_device} device and completed successfully for {success_device} device. Current job status details:",
            "STATEMENT_FAILURE_AND_PARTIAL_SUCCESS": "Job execution partial success for {partial_device} device and completed successfully for {failure_device} device. Current job status details:",
            "STATEMENT_SUCCESS_AND_PARTIAL_SUCCESS": "Job execution partial success for {partial_device} device and completed successfully for {success_device} device. Current job status details:",
            "STATEMENT_REPROCESS_SUCCESS": "Reprocessing resolved previous issues - now job is successful.",
            "STATEMENT_REPROCESS_FAILED": "Critical: Reprocessing failed to resolve job issues.",
            "STATEMENT_REPROCESS_PARTIAL_SUCCESS": "Unfortunately, Reprocessing did not fully resolve issues - job partially completed.",
            "STATEMENT_REPROCESS_SUCCESS_AND_FAILURE": "Unfortunately, Reprocessing did not fully resolve issues - for {failure_device} device job issues.",
            "STATEMENT_REPROCESS_FAILURE_AND_PARTIAL_SUCCESS": "Unfortunately, Reprocessing did not fully resolve issues - for {failed_device} device job issues.",
            "STATEMENT_REPROCESS_SUCCESS_AND_PARTIAL_SUCCESS": "Unfortunately, Reprocessing did not fully resolve issues - for {success_device} device job issues."
        }
    
    def generate_statement(self, 
                         amia_status: str, 
                         claria_status: str, 
                         reprocess_amia: str = None, 
                         reprocess_claria: str = None) -> str:
        """
        Generate status statement based on device statuses and reprocess results
        
        Args:
            amia_status: Status for AMIA device (SUCCESS, FAILURE, PARTIAL_SUCCESS)
            claria_status: Status for CLARIA device (SUCCESS, FAILURE, PARTIAL_SUCCESS)
            reprocess_amia: Reprocess status for AMIA (optional)
            reprocess_claria: Reprocess status for CLARIA (optional)
        
        Returns:
            Formatted status statement
        """
        # Validate inputs
        self._validate_status(amia_status, "AMIA")
        self._validate_status(claria_status, "CLARIA")
        
        if reprocess_amia:
            self._validate_status(reprocess_amia, "AMIA Reprocess")
        if reprocess_claria:
            self._validate_status(reprocess_claria, "CLARIA Reprocess")
        
        # Check if reprocessing is involved
        has_reprocess = reprocess_amia is not None or reprocess_claria is not None
        
        if has_reprocess:
            return self._generate_reprocess_statement(amia_status, claria_status, reprocess_amia, reprocess_claria)
        else:
            return self._generate_initial_statement(amia_status, claria_status)
    
    def _validate_status(self, status: str, device_name: str):
        """Validate that status is one of the allowed values"""
        valid_statuses = [s.value for s in DeviceStatus]
        if status not in valid_statuses:
            raise ValueError(f"Invalid status for {device_name}: {status}. Must be one of {valid_statuses}")
    
    def _generate_initial_statement(self, amia_status: str, claria_status: str) -> str:
        """Generate statement for initial job execution (no reprocessing)"""
        
        # Case 1: Both devices successful
        if amia_status == DeviceStatus.SUCCESS.value and claria_status == DeviceStatus.SUCCESS.value:
            return self.statements["STATEMENT_SUCCESS"]
        
        # Case 2: Both devices failed
        elif amia_status == DeviceStatus.FAILURE.value and claria_status == DeviceStatus.FAILURE.value:
            return self.statements["STATEMENT_FAILED"]
        
        # Case 3: Both devices partial success
        elif amia_status == DeviceStatus.PARTIAL_SUCCESS.value and claria_status == DeviceStatus.PARTIAL_SUCCESS.value:
            return self.statements["STATEMENT_PARTIAL_SUCCESS"]
        
        # Case 4: AMIA success, CLARIA failure
        elif amia_status == DeviceStatus.SUCCESS.value and claria_status == DeviceStatus.FAILURE.value:
            return self.statements["STATEMENT_SUCCESS_AND_FAILURE"].format(
                failure_device="CLARIA", 
                success_device="AMIA"
            )
        
        # Case 5: AMIA failure, CLARIA partial success
        elif amia_status == DeviceStatus.FAILURE.value and claria_status == DeviceStatus.PARTIAL_SUCCESS.value:
            return self.statements["STATEMENT_FAILURE_AND_PARTIAL_SUCCESS"].format(
                partial_device="CLARIA", 
                failure_device="AMIA"
            )
        
        # Case 6: AMIA success, CLARIA partial success
        elif amia_status == DeviceStatus.SUCCESS.value and claria_status == DeviceStatus.PARTIAL_SUCCESS.value:
            return self.statements["STATEMENT_SUCCESS_AND_PARTIAL_SUCCESS"].format(
                partial_device="CLARIA", 
                success_device="AMIA"
            )
        
        # Handle reverse cases
        # AMIA failure, CLARIA success
        elif amia_status == DeviceStatus.FAILURE.value and claria_status == DeviceStatus.SUCCESS.value:
            return self.statements["STATEMENT_SUCCESS_AND_FAILURE"].format(
                failure_device="AMIA", 
                success_device="CLARIA"
            )
        
        # AMIA partial success, CLARIA failure
        elif amia_status == DeviceStatus.PARTIAL_SUCCESS.value and claria_status == DeviceStatus.FAILURE.value:
            return self.statements["STATEMENT_FAILURE_AND_PARTIAL_SUCCESS"].format(
                partial_device="AMIA", 
                failure_device="CLARIA"
            )
        
        # AMIA partial success, CLARIA success
        elif amia_status == DeviceStatus.PARTIAL_SUCCESS.value and claria_status == DeviceStatus.SUCCESS.value:
            return self.statements["STATEMENT_SUCCESS_AND_PARTIAL_SUCCESS"].format(
                partial_device="AMIA", 
                success_device="CLARIA"
            )
        
        else:
            return "Unknown status combination"
    
    def _generate_reprocess_statement(self, 
                                    amia_status: str, 
                                    claria_status: str, 
                                    reprocess_amia: str, 
                                    reprocess_claria: str) -> str:
        """Generate statement when reprocessing is involved"""
        
        # If reprocess statuses are not provided for both, use original statuses
        reprocess_amia = reprocess_amia or amia_status
        reprocess_claria = reprocess_claria or claria_status
        
        # Case 7, 13, 19, 22, 28: Both devices successful after reprocess
        if (reprocess_amia == DeviceStatus.SUCCESS.value and 
            reprocess_claria == DeviceStatus.SUCCESS.value):
            return self.statements["STATEMENT_REPROCESS_SUCCESS"]
        
        # Case 8, 14, 23: Both devices failed after reprocess
        elif (reprocess_amia == DeviceStatus.FAILURE.value and 
              reprocess_claria == DeviceStatus.FAILURE.value):
            return self.statements["STATEMENT_REPROCESS_FAILED"]
        
        # Case 9, 15, 24: Both devices partial success after reprocess
        elif (reprocess_amia == DeviceStatus.PARTIAL_SUCCESS.value and 
              reprocess_claria == DeviceStatus.PARTIAL_SUCCESS.value):
            return self.statements["STATEMENT_REPROCESS_PARTIAL_SUCCESS"]
        
        # Case 10, 16, 20, 27, 29: Mixed success and failure after reprocess
        elif ((reprocess_amia == DeviceStatus.SUCCESS.value and reprocess_claria == DeviceStatus.FAILURE.value) or
              (reprocess_amia == DeviceStatus.FAILURE.value and reprocess_claria == DeviceStatus.SUCCESS.value)):
            
            failure_device = "AMIA" if reprocess_amia == DeviceStatus.FAILURE.value else "CLARIA"
            return self.statements["STATEMENT_REPROCESS_SUCCESS_AND_FAILURE"].format(
                failure_device=failure_device
            )
        
        # Case 11, 17, 26: Mixed failure and partial success after reprocess
        elif ((reprocess_amia == DeviceStatus.FAILURE.value and reprocess_claria == DeviceStatus.PARTIAL_SUCCESS.value) or
              (reprocess_amia == DeviceStatus.PARTIAL_SUCCESS.value and reprocess_claria == DeviceStatus.FAILURE.value)):
            
            failed_device = "AMIA" if reprocess_amia == DeviceStatus.FAILURE.value else "CLARIA"
            return self.statements["STATEMENT_REPROCESS_FAILURE_AND_PARTIAL_SUCCESS"].format(
                failed_device=failed_device
            )
        
        # Case 12, 18, 21, 25, 30: Mixed success and partial success after reprocess
        elif ((reprocess_amia == DeviceStatus.SUCCESS.value and reprocess_claria == DeviceStatus.PARTIAL_SUCCESS.value) or
              (reprocess_amia == DeviceStatus.PARTIAL_SUCCESS.value and reprocess_claria == DeviceStatus.SUCCESS.value)):
            
            success_device = "AMIA" if reprocess_amia == DeviceStatus.SUCCESS.value else "CLARIA"
            return self.statements["STATEMENT_REPROCESS_SUCCESS_AND_PARTIAL_SUCCESS"].format(
                success_device=success_device
            )
        
        else:
            return "Unknown reprocess status combination"
    
    def get_detailed_status(self, 
                          amia_status: str, 
                          claria_status: str, 
                          reprocess_amia: str = None, 
                          reprocess_claria: str = None) -> Dict:
        """Get detailed status information including statement and device statuses"""
        
        statement = self.generate_statement(amia_status, claria_status, reprocess_amia, reprocess_claria)
        
        return {
            "statement": statement,
            "amia_status": amia_status,
            "claria_status": claria_status,
            "reprocess_amia": reprocess_amia,
            "reprocess_claria": reprocess_claria,
            "has_reprocess": reprocess_amia is not None or reprocess_claria is not None
        }


# Example usage and test cases
def test_job_status_generator():
    """Test the job status generator with various scenarios"""
    
    generator = JobStatusGenerator()
    
    # Test cases from the table
    test_cases = [
        # Initial execution cases (no reprocess)
        ("SUCCESS", "SUCCESS", None, None, "STATEMENT_SUCCESS"),
        ("FAILURE", "FAILURE", None, None, "STATEMENT_FAILED"),
        ("PARTIAL_SUCCESS", "PARTIAL_SUCCESS", None, None, "STATEMENT_PARTIAL_SUCCESS"),
        ("SUCCESS", "FAILURE", None, None, "STATEMENT_SUCCESS_AND_FAILURE"),
        ("FAILURE", "PARTIAL_SUCCESS", None, None, "STATEMENT_FAILURE_AND_PARTIAL_SUCCESS"),
        ("SUCCESS", "PARTIAL_SUCCESS", None, None, "STATEMENT_SUCCESS_AND_PARTIAL_SUCCESS"),
        
        # Reprocess cases
        ("FAILURE", "FAILURE", "SUCCESS", "SUCCESS", "STATEMENT_REPROCESS_SUCCESS"),
        ("FAILURE", "FAILURE", "FAILURE", "FAILURE", "STATEMENT_REPROCESS_FAILED"),
        ("FAILURE", "FAILURE", "PARTIAL_SUCCESS", "PARTIAL_SUCCESS", "STATEMENT_REPROCESS_PARTIAL_SUCCESS"),
        ("FAILURE", "FAILURE", "SUCCESS", "FAILURE", "STATEMENT_REPROCESS_SUCCESS_AND_FAILURE"),
        ("FAILURE", "FAILURE", "FAILURE", "PARTIAL_SUCCESS", "STATEMENT_REPROCESS_FAILURE_AND_PARTIAL_SUCCESS"),
        ("FAILURE", "FAILURE", "SUCCESS", "PARTIAL_SUCCESS", "STATEMENT_REPROCESS_SUCCESS_AND_PARTIAL_SUCCESS"),
    ]
    
    print("Job Status Generator Test Results")
    print("=" * 80)
    
    for i, (amia, claria, rep_amia, rep_claria, expected_key) in enumerate(test_cases, 1):
        try:
            result = generator.get_detailed_status(amia, claria, rep_amia, rep_claria)
            print(f"Test Case {i}:")
            print(f"  AMIA: {amia}, CLARIA: {claria}")
            if rep_amia or rep_claria:
                print(f"  Reprocess - AMIA: {rep_amia}, CLARIA: {rep_claria}")
            print(f"  Statement: {result['statement']}")
            print(f"  Expected Key: {expected_key}")
            print("-" * 60)
        except Exception as e:
            print(f"Test Case {i} ERROR: {e}")
            print("-" * 60)


# Real-world usage example
def generate_batch_status_report(device_results: List[Dict]) -> str:
    """Generate a comprehensive status report for multiple device jobs"""
    
    generator = JobStatusGenerator()
    report = []
    
    report.append("BATCH JOB STATUS REPORT")
    report.append("=" * 80)
    
    for i, job in enumerate(device_results, 1):
        amia_status = job.get('amia_status')
        claria_status = job.get('claria_status')
        reprocess_amia = job.get('reprocess_amia')
        reprocess_claria = job.get('reprocess_claria')
        job_id = job.get('job_id', f'Job_{i}')
        
        try:
            detailed_status = generator.get_detailed_status(
                amia_status, claria_status, reprocess_amia, reprocess_claria
            )
            
            report.append(f"\n{job_id}:")
            report.append(f"  AMIA Status: {amia_status}")
            report.append(f"  CLARIA Status: {claria_status}")
            if detailed_status['has_reprocess']:
                report.append(f"  Reprocess AMIA: {reprocess_amia}")
                report.append(f"  Reprocess CLARIA: {reprocess_claria}")
            report.append(f"  Status: {detailed_status['statement']}")
            
        except Exception as e:
            report.append(f"\n{job_id}: ERROR - {e}")
    
    return "\n".join(report)


# Interactive example
if __name__ == "__main__":
    # Run tests
    test_job_status_generator()
    
    print("\n" + "=" * 80)
    print("INTERACTIVE EXAMPLE")
    print("=" * 80)
    
    # Create generator instance
    generator = JobStatusGenerator()
    
    # Example 1: Simple success case
    print("\nExample 1 - Both devices successful:")
    result1 = generator.generate_statement("SUCCESS", "SUCCESS")
    print(f"Statement: {result1}")
    
    # Example 2: Mixed status with reprocess
    print("\nExample 2 - AMIA failed, CLARIA partial success with reprocess:")
    result2 = generator.generate_statement(
        "FAILURE", 
        "PARTIAL_SUCCESS", 
        "SUCCESS",  # AMIA reprocess success
        "PARTIAL_SUCCESS"  # CLARIA reprocess partial success
    )
    print(f"Statement: {result2}")
    
    # Example 3: Get detailed status
    print("\nExample 3 - Detailed status information:")
    detailed = generator.get_detailed_status(
        "SUCCESS", 
        "FAILURE", 
        "SUCCESS", 
        "SUCCESS"
    )
    for key, value in detailed.items():
        print(f"  {key}: {value}")
    
    # Example 4: Batch processing
    print("\nExample 4 - Batch status report:")
    batch_jobs = [
        {'job_id': 'JOB_001', 'amia_status': 'SUCCESS', 'claria_status': 'SUCCESS'},
        {'job_id': 'JOB_002', 'amia_status': 'FAILURE', 'claria_status': 'PARTIAL_SUCCESS', 'reprocess_amia': 'SUCCESS'},
        {'job_id': 'JOB_003', 'amia_status': 'SUCCESS', 'claria_status': 'FAILURE', 'reprocess_claria': 'PARTIAL_SUCCESS'},
    ]
    
    batch_report = generate_batch_status_report(batch_jobs)
    print(batch_report)


import pandas as pd
import json
import re
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from decimal import Decimal, InvalidOperation

class CSVBasedJSONValidator:
    """JSON validator that reads validation rules from CSV configuration"""
    
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.validation_rules = self._load_validation_rules()
        self.data_type_mapping = {
            'STRING': str,
            'NUMBER': (int, float, Decimal),
            'DECIMAL': (float, Decimal),
            'INTEGER': int
        }
    
    def _load_validation_rules(self) -> pd.DataFrame:
        """Load validation rules from CSV file"""
        try:
            df = pd.read_csv(self.csv_file_path)
            required_columns = [
                'CUSTOMER_ID', 'CUSTOMER_NAME', 'DEVICE', 'SOURCE', 
                'JSON_ATTRIBUTE_NAME', 'EXTRACT_QRY_COLUMN', 'DATATYPE',
                'JSON_ATTRIBUTE_NULLABILITY', 'TS_FORMAT', 'LENGTH'
            ]
            
            # Check if all required columns exist
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            return df
        except Exception as e:
            raise Exception(f"Error loading CSV file: {str(e)}")
    
    def validate_json(self, json_data: Dict[str, Any], device_type: str = "BOTH", 
                     customer_id: int = 1, source: str = "TREATMENT") -> Dict[str, Any]:
        """
        Validate JSON data based on CSV configuration
        
        Args:
            json_data: The JSON data to validate
            device_type: Filter rules by device type (AMIA, CLARIA, BOTH)
            customer_id: Filter rules by customer ID
            source: Filter rules by source (TREATMENT, CYCLE)
        """
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "validation_details": {},
            "summary": {
                "total_fields_checked": 0,
                "passed_validation": 0,
                "failed_validation": 0,
                "missing_required_fields": []
            }
        }
        
        try:
            # Filter rules based on parameters
            filtered_rules = self._filter_rules(device_type, customer_id, source)
            
            # Validate each field
            for _, rule in filtered_rules.iterrows():
                field_result = self._validate_field(json_data, rule)
                results["validation_details"][rule['JSON_ATTRIBUTE_NAME']] = field_result
                
                results["summary"]["total_fields_checked"] += 1
                if field_result["is_valid"]:
                    results["summary"]["passed_validation"] += 1
                else:
                    results["summary"]["failed_validation"] += 1
                    results["is_valid"] = False
                    results["errors"].extend(field_result["errors"])
                
                if field_result.get("is_required_missing", False):
                    results["summary"]["missing_required_fields"].append(
                        rule['JSON_ATTRIBUTE_NAME']
                    )
            
            # Generate warnings for data quality issues
            self._generate_warnings(results, json_data)
            
        except Exception as e:
            results["is_valid"] = False
            results["errors"].append(f"Validation error: {str(e)}")
        
        return results
    
    def _filter_rules(self, device_type: str, customer_id: int, source: str) -> pd.DataFrame:
        """Filter rules based on device type, customer ID, and source"""
        filtered_df = self.validation_rules[
            (self.validation_rules['CUSTOMER_ID'] == customer_id) &
            (self.validation_rules['SOURCE'] == source) &
            (
                (self.validation_rules['DEVICE'] == device_type) |
                (self.validation_rules['DEVICE'] == 'BOTH')
            )
        ]
        return filtered_df
    
    def _validate_field(self, json_data: Dict[str, Any], rule: pd.Series) -> Dict[str, Any]:
        """Validate a single field based on CSV rule"""
        field_path = rule['JSON_ATTRIBUTE_NAME']
        result = {
            "field_path": field_path,
            "extract_column": rule['EXTRACT_QRY_COLUMN'],
            "data_type": rule['DATATYPE'],
            "is_required": rule['JSON_ATTRIBUTE_NULLABILITY'] == 'N',
            "is_valid": True,
            "errors": [],
            "actual_value": None,
            "validation_rules": {}
        }
        
        try:
            # Extract value from JSON using path
            value = self._get_nested_value(json_data, field_path)
            result["actual_value"] = value
            
            # Check required field
            if result["is_required"] and (value is None or value == ""):
                result["is_valid"] = False
                result["errors"].append("Required field is missing or empty")
                result["is_required_missing"] = True
                return result
            
            # Skip validation for optional null fields
            if not result["is_required"] and (value is None or value == ""):
                result["is_valid"] = True
                return result
            
            # Data type validation
            type_validation = self._validate_data_type(value, rule['DATATYPE'])
            if not type_validation["is_valid"]:
                result["is_valid"] = False
                result["errors"].append(type_validation["error"])
            
            # Length validation
            if pd.notna(rule['LENGTH']) and isinstance(value, str):
                length_validation = self._validate_length(value, rule['LENGTH'])
                if not length_validation["is_valid"]:
                    result["is_valid"] = False
                    result["errors"].append(length_validation["error"])
            
            # Format validation for timestamps
            if pd.notna(rule['TS_FORMAT']):
                format_validation = self._validate_timestamp_format(value, rule['TS_FORMAT'])
                if not format_validation["is_valid"]:
                    result["is_valid"] = False
                    result["errors"].append(format_validation["error"])
            
            # Medical data specific validations
            medical_validation = self._validate_medical_data(value, field_path, rule['DATATYPE'])
            if not medical_validation["is_valid"]:
                result["is_valid"] = False
                result["errors"].extend(medical_validation["errors"])
        
        except Exception as e:
            result["is_valid"] = False
            result["errors"].append(f"Validation error: {str(e)}")
        
        return result
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from JSON using dot notation and array indices"""
        if not path:
            return data
        
        # Handle array indices in path (e.g., "Actual Therapy[0].Cycle Type")
        path_parts = []
        current_part = ""
        
        for char in path:
            if char == '[':
                if current_part:
                    path_parts.append(current_part)
                    current_part = ""
                path_parts.append('[')
            elif char == ']':
                if current_part:
                    path_parts.append(current_part)
                    current_part = ""
                path_parts.append(']')
            elif char == '.':
                if current_part:
                    path_parts.append(current_part)
                    current_part = ""
            else:
                current_part += char
        
        if current_part:
            path_parts.append(current_part)
        
        # Navigate through the path
        current_data = data
        i = 0
        while i < len(path_parts):
            part = path_parts[i]
            
            if part == '[':
                # Array index
                if i + 2 >= len(path_parts) or path_parts[i + 2] != ']':
                    raise ValueError(f"Invalid array syntax in path: {path}")
                
                index_str = path_parts[i + 1]
                try:
                    if index_str == '*':
                        # Wildcard - return all array elements
                        if not isinstance(current_data, list):
                            return None
                        return [self._get_nested_value(item, '.'.join(path_parts[i+3:])) 
                               for item in current_data]
                    else:
                        index = int(index_str)
                        if not isinstance(current_data, list) or index >= len(current_data):
                            return None
                        current_data = current_data[index]
                        i += 3  # Skip '[', index, ']'
                except ValueError:
                    raise ValueError(f"Invalid array index: {index_str}")
            
            else:
                # Object property
                if not isinstance(current_data, dict) or part not in current_data:
                    return None
                current_data = current_data[part]
                i += 1
        
        return current_data
    
    def _validate_data_type(self, value: Any, expected_type: str) -> Dict[str, Any]:
        """Validate data type"""
        result = {"is_valid": True, "error": ""}
        
        type_upper = expected_type.upper()
        
        if type_upper == 'STRING':
            if not isinstance(value, str):
                result["is_valid"] = False
                result["error"] = f"Expected string, got {type(value).__name__}"
        
        elif type_upper in ['NUMBER', 'DECIMAL']:
            if not isinstance(value, (int, float, Decimal)):
                # Try to convert string to number
                try:
                    float(value)
                except (ValueError, TypeError):
                    result["is_valid"] = False
                    result["error"] = f"Expected number, got {type(value).__name__}"
        
        elif type_upper == 'INTEGER':
            if not isinstance(value, int):
                # Try to convert to integer
                try:
                    int(value)
                except (ValueError, TypeError):
                    result["is_valid"] = False
                    result["error"] = f"Expected integer, got {type(value).__name__}"
        
        return result
    
    def _validate_length(self, value: str, max_length: Union[int, float]) -> Dict[str, Any]:
        """Validate string length"""
        result = {"is_valid": True, "error": ""}
        
        try:
            max_len = int(max_length)
            if len(value) > max_len:
                result["is_valid"] = False
                result["error"] = f"Length {len(value)} exceeds maximum {max_len}"
        except (ValueError, TypeError):
            pass  # Skip length validation if max_length is invalid
        
        return result
    
    def _validate_timestamp_format(self, value: str, expected_format: str) -> Dict[str, Any]:
        """Validate timestamp format"""
        result = {"is_valid": True, "error": ""}
        
        if not isinstance(value, str):
            result["is_valid"] = False
            result["error"] = "Timestamp value must be string"
            return result
        
        try:
            if expected_format == 'DD-MON-YYYY':
                datetime.strptime(value, '%d-%b-%Y')
            elif expected_format == 'HH:MM':
                datetime.strptime(value, '%H:%M')
            elif expected_format == 'HH:MM:SS':
                datetime.strptime(value, '%H:%M:%S')
            else:
                # Try to parse with common formats
                formats_to_try = ['%d-%b-%Y', '%H:%M', '%H:%M:%S', '%Y-%m-%d']
                parsed = False
                for fmt in formats_to_try:
                    try:
                        datetime.strptime(value, fmt)
                        parsed = True
                        break
                    except ValueError:
                        continue
                if not parsed:
                    result["is_valid"] = False
                    result["error"] = f"Invalid timestamp format. Expected: {expected_format}"
        except ValueError:
            result["is_valid"] = False
            result["error"] = f"Invalid timestamp format. Expected: {expected_format}"
        
        return result
    
    def _validate_medical_data(self, value: Any, field_path: str, data_type: str) -> Dict[str, Any]:
        """Medical data specific validations"""
        result = {"is_valid": True, "errors": []}
        
        # Blood pressure validation
        if 'Blood Pressure' in field_path and isinstance(value, str):
            try:
                bp_value = int(value)
                if bp_value <= 0 or bp_value > 300:
                    result["is_valid"] = False
                    result["errors"].append("Blood pressure value out of reasonable range (1-300)")
            except ValueError:
                pass  # Not a numeric string
        
        # Weight validation
        if 'Weight' in field_path:
            try:
                weight = float(value)
                if weight <= 0 or weight > 1000:  # kg, reasonable range
                    result["is_valid"] = False
                    result["errors"].append("Weight value out of reasonable range (1-1000 kg)")
            except (ValueError, TypeError):
                pass
        
        # Pulse validation
        if 'Pulse' in field_path and isinstance(value, str):
            try:
                pulse = int(value)
                if pulse <= 0 or pulse > 250:
                    result["is_valid"] = False
                    result["errors"].append("Pulse value out of reasonable range (1-250 bpm)")
            except ValueError:
                pass
        
        # Temperature validation (Fahrenheit)
        if 'Temperature' in field_path and isinstance(value, str):
            try:
                temp = float(value)
                if temp < 90 or temp > 110:  # Fahrenheit, reasonable range
                    result["is_valid"] = False
                    result["errors"].append("Temperature value out of reasonable range (90-110°F)")
            except (ValueError, TypeError):
                pass
        
        # Glucose validation
        if 'Glucose' in field_path and isinstance(value, str):
            try:
                glucose = int(value)
                if glucose <= 0 or glucose > 1000:  mg/dL, reasonable range
                    result["is_valid"] = False
                    result["errors"].append("Glucose value out of reasonable range (1-1000 mg/dL)")
            except ValueError:
                pass
        
        return result
    
    def _generate_warnings(self, results: Dict[str, Any], json_data: Dict[str, Any]):
        """Generate data quality warnings"""
        # Check for missing optional fields that might be important
        optional_medical_fields = [
            "Vitals.Pre-Treatment.Weight", "Vitals.Post-Treatment.Weight",
            "Vitals.Pre-Treatment.Blood Pressure Systolic", 
            "Vitals.Post-Treatment.Blood Pressure Systolic"
        ]
        
        for field in optional_medical_fields:
            value = self._get_nested_value(json_data, field)
            if value is None or value == "":
                results["warnings"].append(f"Optional medical field {field} is missing")
        
        # Check for extreme values in medical data
        self._check_extreme_values(results, json_data)
    
    def _check_extreme_values(self, results: Dict[str, Any], json_data: Dict[str, Any]):
        """Check for extreme values in medical data"""
        extreme_checks = [
            ("Vitals.Pre-Treatment.Weight", 30, 300),  # kg
            ("Vitals.Post-Treatment.Weight", 30, 300),
            ("Vitals.Pre-Treatment.Blood Pressure Systolic", 70, 200),  # mmHg
            ("Vitals.Post-Treatment.Blood Pressure Systolic", 70, 200),
        ]
        
        for field_path, min_val, max_val in extreme_checks:
            value = self._get_nested_value(json_data, field_path)
            if value is not None:
                try:
                    num_value = float(value)
                    if num_value < min_val or num_value > max_val:
                        results["warnings"].append(
                            f"Extreme value in {field_path}: {value} (expected {min_val}-{max_val})"
                        )
                except (ValueError, TypeError):
                    pass
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate a comprehensive validation report"""
        report = []
        report.append("=" * 80)
        report.append("MEDICAL DATA VALIDATION REPORT")
        report.append("=" * 80)
        
        # Summary
        summary = validation_results["summary"]
        report.append(f"SUMMARY:")
        report.append(f"  Total Fields Checked: {summary['total_fields_checked']}")
        report.append(f"  Passed Validation: {summary['passed_validation']}")
        report.append(f"  Failed Validation: {summary['failed_validation']}")
        report.append(f"  Overall Status: {'PASS' if validation_results['is_valid'] else 'FAIL'}")
        
        # Errors
        if validation_results["errors"]:
            report.append(f"\nERRORS ({len(validation_results['errors'])}):")
            for error in validation_results["errors"][:10]:  # Show first 10 errors
                report.append(f"  • {error}")
            if len(validation_results["errors"]) > 10:
                report.append(f"  ... and {len(validation_results['errors']) - 10} more errors")
        
        # Warnings
        if validation_results["warnings"]:
            report.append(f"\nWARNINGS ({len(validation_results['warnings'])}):")
            for warning in validation_results["warnings"][:5]:  # Show first 5 warnings
                report.append(f"  ⚠ {warning}")
        
        # Missing required fields
        if summary['missing_required_fields']:
            report.append(f"\nMISSING REQUIRED FIELDS ({len(summary['missing_required_fields'])}):")
            for field in summary['missing_required_fields'][:5]:
                report.append(f"  ✗ {field}")
        
        # Field-level details (sample)
        report.append(f"\nFIELD VALIDATION DETAILS (sample):")
        details = validation_results["validation_details"]
        for field_path, detail in list(details.items())[:5]:  # Show first 5 fields
            status = "✓" if detail["is_valid"] else "✗"
            report.append(f"  {status} {field_path}")
            if not detail["is_valid"]:
                report.append(f"    Errors: {', '.join(detail['errors'])}")
        
        return "\n".join(report)
    

# Example usage with your medical JSON data
def main():
    # Initialize validator with CSV configuration
    validator = CSVBasedJSONValidator('validation_rules.csv')
    
    # Your JSON data (from the example)
    medical_json = {
        "Patient Info": {
            "ERP Patient ID": "07501-77130212",
            "Clinic Patient ID": "123445531",
            "Treatment ID": 1102786,
            "Device Type": "AMIA",
            "First Name": "FNAME-77130212",
            "Last Name": "LNAME-77130212",
            "DOB": "26-APR-1986"
        },
        "Batch": {
            "ID": "bd31f758-58bd-11ef-8bf1-0a8910300000",
            "Date": "12-Aug-2024",
            "Time": "09:15",
            "Type": "Normal"
        },
        "Treatment Time": {
            "Date": "12-AUG-2024",
            "Time": "05:10"
        },
        "Vitals": {
            "Pre-Treatment": {
                "Weight": 130,
                "Blood Pressure Systolic": "100",
                "Blood Pressure Diastolic": "200",
                "Pulse": "100",
                "Glucose": "60",
                "Temperature": "212"
            },
            "Post-Treatment": {
                "Weight": 140,
                "Blood Pressure Systolic": "100",
                "Blood Pressure Diastolic": "210",
                "Pulse": "150",
                "Glucose": "70",
                "Temperature": "230"
            }
        },
        "Prescription": {
            "Device Program Name": "Swit_50",
            "Therapy Type": "Time-Based APD",
            "Therapy Time": 600,
            "Number of Day Cycles": 0,
            "Number of Night Cycles": 0,
            "Fill Volume": 1000,
            "Dwell Time": 0,
            "Drain Time": 60
        },
        "Actual Therapy": [
            {
                "Therapy Cycle Time": "06:30:00",
                "Cycle Type": "LASTFILL",
                "CycleAttributes": {
                    "Fill Volume": 100,
                    "Fill Time": 600
                }
            }
        ],
        "Total Therapy": {
            "Total Therapy Volume": 300,
            "Total UF": 17977
        },
        "Medication": {
            "ESA": "Y",
            "Heparin": "Y",
            "Antibiotics": "Y",
            "Vitamin D": "Y"
        }
    }
    
    # Validate for AMIA device
    results = validator.validate_json(
        medical_json, 
        device_type="AMIA", 
        customer_id=1, 
        source="TREATMENT"
    )
    
    # Generate report
    report = validator.generate_validation_report(results)
    print(report)
    
    # Save detailed results to file
    with open('validation_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    return results

class BatchMedicalValidator:
    """Batch process multiple JSON files"""
    
    def __init__(self, csv_config_path: str):
        self.validator = CSVBasedJSONValidator(csv_config_path)
    
    def validate_batch(self, json_files: List[Dict], device_type: str = "BOTH") -> Dict[str, Any]:
        """Validate multiple JSON files"""
        batch_results = {
            "total_files": len(json_files),
            "valid_files": 0,
            "invalid_files": 0,
            "file_results": [],
            "common_errors": {},
            "data_quality_issues": []
        }
        
        for i, json_data in enumerate(json_files):
            file_result = self.validator.validate_json(json_data, device_type)
            file_result["file_index"] = i
            
            batch_results["file_results"].append(file_result)
            
            if file_result["is_valid"]:
                batch_results["valid_files"] += 1
            else:
                batch_results["invalid_files"] += 1
            
            # Collect common errors
            for error in file_result.get("errors", []):
                batch_results["common_errors"][error] = batch_results["common_errors"].get(error, 0) + 1
            
            # Collect data quality issues
            for warning in file_result.get("warnings", []):
                batch_results["data_quality_issues"].append({
                    "file_index": i,
                    "warning": warning
                })
        
        return batch_results

# Example batch processing
def process_multiple_files():
    validator = BatchMedicalValidator('validation_rules.csv')
    
    # Simulate multiple JSON files
    json_files = [medical_json] * 3  # Your JSON data repeated 3 times
    
    batch_results = validator.validate_batch(json_files, device_type="AMIA")
    
    print(f"Batch Validation Results:")
    print(f"Total Files: {batch_results['total_files']}")
    print(f"Valid Files: {batch_results['valid_files']}")
    print(f"Invalid Files: {batch_results['invalid_files']}")
    
    # Show most common errors
    if batch_results['common_errors']:
        print("\nMost Common Errors:")
        for error, count in sorted(batch_results['common_errors'].items(), 
                                 key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {count}x: {error}")
    
    return batch_results

if __name__ == "__main__":
    results = main()
    process_multiple_files()
