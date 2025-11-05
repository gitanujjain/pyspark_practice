import csv
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

class DynamicHL7Generator:
    """Dynamic HL7 ORU_R01 message generator using configuration mapping."""
    
    def __init__(self, config_file_path: str):
        self.version = '2.5'
        self.config_data = self._load_config(config_file_path)
        self.segment_mappings = self._build_dynamic_mappings()
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_file_path: str) -> List[Dict]:
        """Load configuration from CSV file."""
        config_data = []
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                config_data = [row for row in reader]
            self.logger.info(f"Loaded {len(config_data)} configuration mappings")
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise
        return config_data

    def _build_dynamic_mappings(self) -> Dict[str, Any]:
        """Build dynamic mappings from configuration data."""
        mappings = {
            'segment_order': [],
            'segments': {},
            'obr_sections': {},
            'field_sequence': {}
        }
        
        for row in self.config_data:
            segment = row['SEGMENT']
            segment_id = row.get('SEGMENT_ID', '')
            child_obr = row.get('CHILD_OBR', '')
            json_attr = row['JSON_ATTRIBUTE'].replace(' ', '_').upper()
            hl7_key = row.get('HL7_KEY', '')
            
            field_config = {
                'identifier': row['IDENTIFIER'],
                'display_name': row['DISPLAY_NAME'],
                'data_type': row['DATA_TYPE'],
                'unit': row.get('UNIT', ''),
                'sequence': int(row['SEQUENCE']) if row['SEQUENCE'] else 999,
                'json_attribute': json_attr,
                'child_obr': child_obr,
                'segment_id': segment_id,
                'hl7_key': hl7_key
            }
            
            # Build segment order based on sequence
            if segment not in mappings['segment_order']:
                mappings['segment_order'].append(segment)
            
            # Organize by segment type
            if segment not in mappings['segments']:
                mappings['segments'][segment] = {}
            
            if segment == 'OBX' and child_obr:
                # OBX fields grouped by their parent OBR
                if child_obr not in mappings['obr_sections']:
                    mappings['obr_sections'][child_obr] = []
                mappings['obr_sections'][child_obr].append(field_config)
            else:
                # Direct segment fields (PID, PV1, OBR, BHS)
                mappings['segments'][segment][json_attr] = field_config
            
            # Store field sequence for ordering
            mappings['field_sequence'][f"{segment}_{json_attr}"] = field_config['sequence']
        
        # Sort segments and fields by sequence
        for segment in mappings['segments']:
            mappings['segments'][segment] = dict(
                sorted(mappings['segments'][segment].items(), 
                      key=lambda x: x[1]['sequence'])
            )
        
        for obr_section in mappings['obr_sections']:
            mappings['obr_sections'][obr_section] = sorted(
                mappings['obr_sections'][obr_section], 
                key=lambda x: x['sequence']
            )
        
        return mappings

    def _normalize_json_data(self, data: Any) -> Any:
        """Recursively normalize JSON data keys to match configuration."""
        if isinstance(data, dict):
            normalized = {}
            for key, value in data.items():
                normalized_key = key.replace(' ', '_').replace('-', '_').upper()
                normalized[normalized_key] = self._normalize_json_data(value)
            return normalized
        elif isinstance(data, list):
            return [self._normalize_json_data(item) for item in data]
        return data

    def _get_timestamp(self) -> str:
        """Get current timestamp in HL7 format."""
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def _get_field_value(self, data: Dict, field_path: str) -> Any:
        """Extract field value from nested data using path."""
        try:
            keys = field_path.split('.')
            current = data
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                elif isinstance(current, list) and len(current) > 0:
                    # Handle list of objects
                    if isinstance(current[0], dict) and key in current[0]:
                        current = [item.get(key) for item in current]
                    else:
                        return None
                else:
                    return None
            return current
        except (KeyError, IndexError, TypeError):
            return None

    def _build_segment(self, segment_type: str, data: Dict, set_id: int = 1) -> List[str]:
        """Dynamically build HL7 segments based on configuration."""
        segments = []
        
        if segment_type == 'MSH':
            segments.append(
                f"MSH|^~\\&|SENDING_APP|SENDING_FACILITY|RECEIVING_APP|RECEIVING_FACILITY|"
                f"{self._get_timestamp()}||ORU^R01|{self._get_timestamp()}|P|{self.version}"
            )
        
        elif segment_type == 'BHS':
            batch_data = data.get('BATCH', {})
            segments.append(
                f"BHS|^~\\&|SENDING_APP|SENDING_FACILITY|RECEIVING_APP|RECEIVING_FACILITY|"
                f"{self._get_timestamp()}||BatchType-{batch_data.get('TYPE', 'Normal')}||{batch_data.get('ID', '')}"
            )
        
        elif segment_type == 'PID':
            pid_fields = ['PID', str(set_id)]
            
            # Add patient identifiers
            id_fields = []
            for field_config in self.segment_mappings['segments']['PID'].values():
                field_value = self._get_field_value(data, f"PATIENT_INFO.{field_config['json_attribute']}")
                if field_value:
                    if field_config['json_attribute'] in ['ERP_PATIENT_ID', 'BAXTER_PATIENT_ID']:
                        id_fields.append(f"{field_value}^^^Baxter_Patient_ID")
                    elif field_config['json_attribute'] == 'CLINIC_PATIENT_ID':
                        id_fields.append(f"{field_value}^^^Clinic_Patient_ID")
            
            # Add empty fields for PID structure
            pid_fields.extend(id_fields + [''] * (4 - len(id_fields)))  # PID-2 to PID-5
            
            # Add patient name (PID-5)
            first_name = self._get_field_value(data, "PATIENT_INFO.FIRST_NAME")
            last_name = self._get_field_value(data, "PATIENT_INFO.LAST_NAME")
            middle_name = self._get_field_value(data, "PATIENT_INFO.MIDDLE_NAME")
            if first_name or last_name:
                pid_fields.append(f"{last_name or ''}^{first_name or ''}^{middle_name or ''}")
            else:
                pid_fields.append("")
            
            segments.append('|'.join(pid_fields))
        
        elif segment_type == 'PV1':
            clinic_name = self._get_field_value(data, "PATIENT_INFO.CLINIC_NAME") or "XYZClinic"
            segments.append(f"PV1|{set_id}|o|^^^{clinic_name}")
        
        elif segment_type == 'ORC':
            treatment_id = self._get_field_value(data, "PATIENT_INFO.TREATMENT_ID") or ""
            segments.append(f"ORC|RE|{treatment_id}|FILLER_ID|||||||{self._get_timestamp()}")
        
        return segments

    def _build_obr_section(self, obr_section: str, data: Dict, obr_counter: int) -> List[str]:
        """Dynamically build OBR section with OBX fields."""
        segments = []
        
        # Get OBR configuration
        obr_config = self.segment_mappings['segments']['OBR'].get(obr_section)
        if not obr_config:
            return segments
        
        # Build OBR segment
        obr_identifier = obr_config['identifier']
        obr_display_name = obr_config['display_name']
        timestamp_suffix = 23 + obr_counter  # Dynamic timestamp as in example
        
        segments.append(f"OBR|{obr_counter}|||{obr_identifier}^{obr_display_name}|||202510151234{timestamp_suffix}")
        
        # Build OBX fields for this OBR section
        obx_fields = self.segment_mappings['obr_sections'].get(obr_section, [])
        obx_counter = 1
        
        for obx_config in obx_fields:
            json_attr = obx_config['json_attribute']
            
            # Extract value from data
            field_value = self._get_field_value(data, json_attr)
            if field_value is None:
                # Try nested lookup for common structures
                field_value = self._get_field_value(data, f"{obr_section}.{json_attr}")
            
            if field_value is not None:
                # Handle list values (like ACTUAL_THERAPY)
                if isinstance(field_value, list):
                    for item in field_value:
                        if isinstance(item, dict):
                            # For nested structures like ACTUAL_THERAPY
                            self._process_nested_obx(segments, item, obr_counter, obx_config, obx_counter)
                            obr_counter += 1
                            obx_counter = 1
                        else:
                            self._add_obx_segment(segments, obx_config, item, obr_counter, obx_counter)
                            obx_counter += 1
                else:
                    self._add_obx_segment(segments, obx_config, field_value, obr_counter, obx_counter)
                    obx_counter += 1
        
        return segments

    def _process_nested_obx(self, segments: List[str], nested_data: Dict, obr_counter: int, 
                           obx_config: Dict, obx_counter: int) -> None:
        """Process nested OBX data (like ACTUAL_THERAPY cycles)."""
        # This would handle complex nested structures
        # For now, add simple OBX segments
        for key, value in nested_data.items():
            if key == 'CYCLEATTRIBUTES':
                for attr_key, attr_value in value.items():
                    # Find matching OBX config for this attribute
                    matching_config = None
                    for config in self.segment_mappings['obr_sections'].get(obx_config['child_obr'], []):
                        if config['json_attribute'] == attr_key:
                            matching_config = config
                            break
                    
                    if matching_config:
                        self._add_obx_segment(segments, matching_config, attr_value, obr_counter, obx_counter)
                        obx_counter += 1
            else:
                # Find matching OBX config
                matching_config = None
                for config in self.segment_mappings['obr_sections'].get(obx_config['child_obr'], []):
                    if config['json_attribute'] == key:
                        matching_config = config
                        break
                
                if matching_config:
                    self._add_obx_segment(segments, matching_config, value, obr_counter, obx_counter)
                    obx_counter += 1

    def _add_obx_segment(self, segments: List[str], obx_config: Dict, value: Any, 
                        obr_set_id: int, obx_set_id: int) -> None:
        """Add individual OBX segment."""
        identifier = obx_config['identifier']
        display_name = obx_config['display_name']
        data_type = obx_config['data_type']
        unit = obx_config['unit']
        
        unit_str = f"|{unit}" if unit else ""
        segments.append(
            f"OBX|{obx_set_id}|{data_type}|{identifier}^{display_name}|{obr_set_id}|{value}{unit_str}|||||F"
        )

    def generate_hl7(self, json_data: Dict) -> str:
        """Generate complete HL7 message from JSON data."""
        try:
            # Normalize input data
            normalized_data = self._normalize_json_data(json_data)
            hl7_segments = []
            
            # Build segments in order
            for segment_type in ['BHS', 'MSH', 'PID', 'PV1', 'ORC']:
                segments = self._build_segment(segment_type, normalized_data)
                hl7_segments.extend(segments)
            
            # Add initial OBR for device info
            hl7_segments.append("OBR|1|||VITAL_SIGNS^Vitals Panel^CLINIC_APP|||20251015123423")
            
            treatment_id = self._get_field_value(normalized_data, "PATIENT_INFO.TREATMENT_ID") or ""
            hl7_segments.append(f"NTE|1||Treatment ID: {treatment_id}|")
            hl7_segments.append("NTE|2||Clinic Timezone: PST")
            
            # Add device type OBX
            device_type = self._get_field_value(normalized_data, "PATIENT_INFO.DEVICE_TYPE")
            if device_type:
                hl7_segments.append(f"OBX|1|ST|DEVICE_TYPE^Device Type|1|{device_type}||||||F")
            
            # Process all OBR sections dynamically
            obr_counter = 2
            for obr_section in self.segment_mappings['obr_sections'].keys():
                obr_segments = self._build_obr_section(obr_section, normalized_data, obr_counter)
                hl7_segments.extend(obr_segments)
                
                # Increment counter based on segments added
                if obr_segments:
                    obr_counter += len([s for s in obr_segments if s.startswith('OBR')])
            
            # Add BTS segment
            hl7_segments.append("BTS|1|final segment|100")
            
            return '\n'.join(hl7_segments)
            
        except Exception as e:
            self.logger.error(f"Error generating HL7 message: {e}")
            raise

class DynamicHL7BatchProcessor:
    """Batch processor for multiple HL7 messages."""
    
    def __init__(self, config_file_path: str, batch_size: int = 10):
        self.batch_size = batch_size
        self.hl7_generator = DynamicHL7Generator(config_file_path)
        self.logger = logging.getLogger(__name__)
    
    def process_batch(self, json_data_list: List[Dict]) -> List[Dict]:
        """Process batch of JSON data into HL7 messages."""
        results = []
        
        for i, json_data in enumerate(json_data_list, 1):
            try:
                hl7_message = self.hl7_generator.generate_hl7(json_data)
                results.append({
                    'sequence': i,
                    'status': 'success',
                    'hl7_message': hl7_message,
                    'timestamp': datetime.now().isoformat()
                })
                self.logger.info(f"Successfully generated HL7 message {i}")
            except Exception as e:
                results.append({
                    'sequence': i,
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                self.logger.error(f"Error generating HL7 message {i}: {e}")
                
        return results

    def process_to_files(self, json_data_list: List[Dict], output_prefix: str = 'hl7_output') -> Dict[str, Any]:
        """Process data and save to files."""
        results = self.process_batch(json_data_list)
        
        success_count = 0
        for result in results:
            if result['status'] == 'success':
                filename = f"{output_prefix}_{result['sequence']:03d}.hl7"
                with open(filename, 'w') as f:
                    f.write(result['hl7_message'])
                success_count += 1
                self.logger.info(f"Saved HL7 message to {filename}")
        
        return {
            'total_processed': len(results),
            'successful': success_count,
            'failed': len(results) - success_count,
            'results': results
        }


# Usage Example
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Your input data
    input_data = {
        "Patient Info": {
            "Baxter Patient ID": "07501-39994726",
            "Clinic Patient ID": "9820609",
            "Treatment ID": 1264740,
            "Device Type": "CLARIA",
            "First Name": "John",
            "Last Name": "Doe",
            "Clinic Name": "XYZClinic"
        },
        "Vitals": {
            "Pre-Treatment": {
                "Weight": 70.0,
                "Blood Pressure Systolic": "120",
                "Blood Pressure Diastolic": "80"
            }
        },
        "Prescription": {
            "Device Program Name": "Harry's",
            "Therapy Type": "Hi-Dose",
            "Therapy Time": 18000,
            "Number of Day Cycles": 1,
            "Number of Night Cycles": 4,
            "Fill Volume": 600
        },
        "Actual Therapy": [
            {
                "Therapy Cycle Time": "22:50:38",
                "Cycle Type": "Night 5",
                "CycleAttributes": {
                    "Fill Volume": 100,
                    "Fill Time": 600,
                    "Dwell Time": 600,
                    "Drain Volume": 100,
                    "UF/Cycle": 0
                }
            }
        ],
        "Total Therapy": {
            "Total Therapy Volume": 3000
        },
        "Solution Claria": {
            "SurveyNightConcentration1": "Extraneal",
            "SurveyNightConcentration2": "Extraneal",
            "SurveyLastBagConcentration": "Extraneal"
        },
        "Flag": {
            "Lost dwell time": 179
        },
        "Batch": {
            "ID": "9daa7664-889b-4a51-a555-476642caf035",
            "Date": "13-Aug-2025",
            "Time": "14:28",
            "Type": "Normal"
        }
    }
    
    try:
        # Initialize with your configuration file
        processor = DynamicHL7BatchProcessor('customer_hl7_attributes_mapping.csv')
        
        # Generate HL7 message
        results = processor.process_batch([input_data])
        
        if results[0]['status'] == 'success':
            print("✅ Successfully generated HL7 message:")
            print(results[0]['hl7_message'])
            
            # Save to file
            with open('dynamic_output.hl7', 'w') as f:
                f.write(results[0]['hl7_message'])
            print("\n💾 Saved to 'dynamic_output.hl7'")
        else:
            print(f"❌ Error: {results[0]['error']}")
            
    except Exception as e:
        print(f"❌ Initialization error: {e}")