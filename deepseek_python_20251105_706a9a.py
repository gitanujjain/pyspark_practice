from pyspark.sql.types import StructType, StructField
import logging
from typing import Dict, List, Set, Any, Optional

class SchemaValidator:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.log = logger or logging.getLogger(__name__)
        self._compatible_types = {
            'byte': {'short', 'int', 'long', 'float', 'double', 'decimal'},
            'short': {'int', 'long', 'float', 'double', 'decimal'},
            'int': {'long', 'float', 'double', 'decimal'},
            'long': {'float', 'double', 'decimal'},
            'float': {'double', 'decimal'},
            'string': {'varchar', 'char'},
        }

    def validate_schemas(
        self, 
        expected_df, 
        extracted_df, 
        source: str,
        allow_type_coercion: bool = False,
        ignore_extra_columns: bool = False
    ) -> Dict[str, Any]:
        """
        Optimized schema validation between expected and extracted DataFrames.
        """
        result = self._initialize_result(source)
        
        try:
            expected_fields = {field.name: field for field in expected_df.schema.fields}
            extracted_fields = {field.name: field for field in extracted_df.schema.fields}
            
            self._log_schemas(source, expected_fields, extracted_fields)
            self._validate_column_counts(result, expected_fields, extracted_fields, ignore_extra_columns)
            self._validate_columns(result, expected_fields, extracted_fields, ignore_extra_columns)
            self._validate_data_types(result, expected_fields, extracted_fields, allow_type_coercion)
            
            result['is_valid'] = not result['errors']
            self._log_validation_result(result, source)
            
        except Exception as err:
            self._handle_validation_error(result, source, err)
            
        return result

    def _initialize_result(self, source: str) -> Dict[str, Any]:
        """Initialize the validation result structure."""
        return {
            'is_valid': False,
            'source': source,
            'errors': [],
            'warnings': [],
            'details': {
                'expected_columns': 0,
                'extracted_columns': 0,
                'missing_columns': [],
                'extra_columns': [],
                'type_mismatches': [],
                'nullable_mismatches': []
            }
        }

    def _log_schemas(self, source: str, expected_fields: Dict, extracted_fields: Dict) -> None:
        """Log schema information for debugging."""
        self.log.info(f'Expected schema for "{source}": {list(expected_fields.keys())}')
        self.log.info(f'Extracted schema for "{source}": {list(extracted_fields.keys())}')

    def _validate_column_counts(self, result: Dict, expected_fields: Dict, extracted_fields: Dict, ignore_extra: bool) -> None:
        """Validate column count compatibility."""
        expected_count = len(expected_fields)
        extracted_count = len(extracted_fields)
        
        result['details']['expected_columns'] = expected_count
        result['details']['extracted_columns'] = extracted_count
        
        if expected_count != extracted_count and not ignore_extra:
            error_msg = (
                f'Column count mismatch: Expected {expected_count}, '
                f'Found {extracted_count} columns'
            )
            result['errors'].append(error_msg)
            self.log.error(error_msg)

    def _validate_columns(self, result: Dict, expected_fields: Dict, extracted_fields: Dict, ignore_extra: bool) -> None:
        """Validate column presence and extra columns."""
        expected_names = set(expected_fields.keys())
        extracted_names = set(extracted_fields.keys())
        
        # Find missing columns
        missing_columns = expected_names - extracted_names
        if missing_columns:
            error_msg = f'Missing columns: {sorted(missing_columns)}'
            result['errors'].append(error_msg)
            result['details']['missing_columns'] = sorted(missing_columns)
            self.log.error(error_msg)
        
        # Find extra columns
        extra_columns = extracted_names - expected_names
        if extra_columns and not ignore_extra:
            warning_msg = f'Extra columns: {sorted(extra_columns)}'
            result['warnings'].append(warning_msg)
            result['details']['extra_columns'] = sorted(extra_columns)
            self.log.warning(warning_msg)

    def _validate_data_types(self, result: Dict, expected_fields: Dict, extracted_fields: Dict, allow_coercion: bool) -> None:
        """Validate data types and nullable constraints for common columns."""
        common_columns = set(expected_fields.keys()) & set(extracted_fields.keys())
        
        for column in common_columns:
            expected_field = expected_fields[column]
            extracted_field = extracted_fields[column]
            
            self._check_type_compatibility(result, column, expected_field, extracted_field, allow_coercion)
            self._check_nullable_constraint(result, column, expected_field, extracted_field)

    def _check_type_compatibility(self, result: Dict, column: str, expected_field: StructField, 
                                extracted_field: StructField, allow_coercion: bool) -> None:
        """Check if data types are compatible."""
        if expected_field.dataType == extracted_field.dataType:
            return
        
        expected_type = str(expected_field.dataType).lower()
        actual_type = str(extracted_field.dataType).lower()
        
        is_compatible = allow_coercion and self._is_type_coercible(expected_type, actual_type)
        
        mismatch_info = {
            'column': column,
            'expected_type': str(expected_field.dataType),
            'actual_type': str(extracted_field.dataType)
        }
        
        if is_compatible:
            warning_msg = f'Type mismatch (coercible) for "{column}": {expected_type} -> {actual_type}'
            result['warnings'].append(warning_msg)
            self.log.warning(warning_msg)
        else:
            error_msg = f'Type mismatch for "{column}": Expected {expected_type}, Found {actual_type}'
            result['errors'].append(error_msg)
            result['details']['type_mismatches'].append(mismatch_info)
            self.log.error(error_msg)

    def _check_nullable_constraint(self, result: Dict, column: str, expected_field: StructField, 
                                 extracted_field: StructField) -> None:
        """Check nullable constraint compatibility."""
        if expected_field.nullable != extracted_field.nullable:
            mismatch_info = {
                'column': column,
                'expected_nullable': expected_field.nullable,
                'actual_nullable': extracted_field.nullable
            }
            result['details']['nullable_mismatches'].append(mismatch_info)
            
            warning_msg = (
                f'Nullable mismatch for "{column}": '
                f'Expected {expected_field.nullable}, Found {extracted_field.nullable}'
            )
            result['warnings'].append(warning_msg)
            self.log.warning(warning_msg)

    def _is_type_coercible(self, expected_type: str, actual_type: str) -> bool:
        """Check if actual type can be coerced to expected type."""
        return (
            actual_type in self._compatible_types.get(expected_type, set()) or 
            expected_type in self._compatible_types.get(actual_type, set())
        )

    def _log_validation_result(self, result: Dict, source: str) -> None:
        """Log the final validation result."""
        if result['is_valid']:
            self.log.info(f'✅ Schema validation passed for "{source}"')
        else:
            self.log.error(f'❌ Schema validation failed for "{source}": {len(result["errors"])} errors')

    def _handle_validation_error(self, result: Dict, source: str, error: Exception) -> None:
        """Handle unexpected validation errors."""
        error_msg = f'Validation error for "{source}": {str(error)}'
        result['errors'].append(error_msg)
        self.log.error(error_msg, exc_info=True)

    def generate_diff_report(self, validation_result: Dict) -> str:
        """Generate optimized schema difference report."""
        source = validation_result['source']
        
        if validation_result['is_valid']:
            return f"✅ Schema validation passed for '{source}'"
        
        report = [f"📊 Schema Validation Report for '{source}':", "=" * 50]
        
        # Add errors section
        if validation_result['errors']:
            report.extend(["❌ ERRORS:", *[f"  • {error}" for error in validation_result['errors']]])
        
        # Add warnings section  
        if validation_result['warnings']:
            report.extend(["⚠️  WARNINGS:", *[f"  • {warning}" for warning in validation_result['warnings']]])
        
        # Add details section
        details = validation_result['details']
        if details.get('missing_columns'):
            report.extend(["\n🔍 Missing Columns:", *[f"  • {col}" for col in details['missing_columns']]])
        
        if details.get('type_mismatches'):
            report.extend(["\n🔄 Type Mismatches:"])
            for mismatch in details['type_mismatches']:
                report.append(f"  • {mismatch['column']}: {mismatch['expected_type']} → {mismatch['actual_type']}")
        
        return "\n".join(report)


# Usage example with performance optimizations
def validate_multiple_sources(validator: SchemaValidator, configs: List[Dict]) -> Dict[str, Dict]:
    """Batch validate multiple data sources."""
    return {
        config['source']: validator.validate_schemas(**config) 
        for config in configs
    }