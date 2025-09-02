import logging
import requests
from google.cloud import bigquery
from typing import Optional
from datetime import datetime
import pandas as pd

from config.settings import BIGQUERY_CONFIG, SERVICE_ACCOUNT_CONFIG, TABLE_MAPPING, Platform
from config.schemas import get_schema
from utils.transformers import convert_batch_datetime_for_json

logger = logging.getLogger(__name__)

class BigQueryService:
    def __init__(self):
        self.client: Optional[bigquery.Client] = None
        self.project_id = BIGQUERY_CONFIG["project_id"]
        self.dataset_id = BIGQUERY_CONFIG["dataset_id"]
        self.dataset_ref = None  # Will be set after client initialization
    
    def initialize(self) -> bool:
        """Initialize BigQuery client using service account"""
        try:
            # Verify service account on Compute Engine
            email_url = f"{SERVICE_ACCOUNT_CONFIG['metadata_url']}instance/service-accounts/{SERVICE_ACCOUNT_CONFIG['service_account']}/email"
            response = requests.get(
                email_url, 
                headers=SERVICE_ACCOUNT_CONFIG['metadata_headers'], 
                timeout=10
            )
            response.raise_for_status()
            
            current_sa = response.text.strip()
            logger.info(f"🔍 Using service account: {current_sa}")
            
            # Verify service account format
            if not current_sa.endswith('.gserviceaccount.com'):
                logger.warning(f"⚠️ Service account format seems unusual: {current_sa}")
            else:
                logger.info(f"Service account format looks valid")
            
            # Initialize BigQuery client
            self.client = bigquery.Client(project=self.project_id)
            self.dataset_ref = self.client.dataset(self.dataset_id)
            logger.info(f"🔑 Using VM's default service account credentials")
            
            # Test connection
            test_query = "SELECT 1 as test_connection"
            test_job = self.client.query(test_query)
            list(test_job.result())
            
            logger.info(f"BigQuery client initialized successfully")
            logger.info(f"Project: {self.project_id}")
            logger.info(f"🔑 Service Account: {current_sa}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            return False
    
    def create_tables_for_all_platforms(self) -> bool:
        """Create all tables for all platforms with proper timing handling"""
        try:
            if not self.client:
                logger.error("BigQuery client not initialized")
                return False
            
            created_tables = []
            existing_tables = []
            
            for platform in Platform:
                for table_type in ["profiles", "urls", "keywords", "url_followers"]:
                    if platform == Platform.FACEBOOK and table_type == "keywords":
                        continue
                    if platform == Platform.LINKEDIN and table_type == "url_followers":
                        continue
                    table_name = TABLE_MAPPING[platform][table_type]
                    table_ref = self.dataset_ref.table(table_name)

                    # Check if table exists
                    try:
                        self.client.get_table(table_ref)
                        logger.info(f"Table {self.project_id}.{self.dataset_id}.{table_name} already exists")
                        existing_tables.append(table_name)
                    except:
                        # Create table
                        schema = get_schema(platform, table_type)
                        table = bigquery.Table(table_ref, schema=schema)
                        table = self.client.create_table(table)
                        logger.info(f"Created table {self.project_id}.{self.dataset_id}.{table_name}")
                        created_tables.append(table_name)
            
            # If we created new tables, wait for them to be ready
            if created_tables:
                import time
                wait_time = 3
                logger.info(f"⏳ Waiting {wait_time} seconds for {len(created_tables)} new tables to be ready...")
                time.sleep(wait_time)
                
                # Verify tables are accessible (optional - comment out if you want faster startup)
                self._verify_tables_accessibility(self.dataset_ref, created_tables)
            
            logger.info(f"Tables status - Created: {len(created_tables)}, Existing: {len(existing_tables)}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False

    def _verify_tables_accessibility(self, dataset_ref, table_names: list, max_retries: int = 3):
        """Verify that newly created tables are accessible"""
        for table_name in table_names:
            table_ref = dataset_ref.table(table_name)
            
            for attempt in range(max_retries):
                try:
                    table = self.client.get_table(table_ref)
                    logger.info(f"Verified table accessibility: {table.full_table_id}")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        logger.warning(f"⏳ Table {table_name} not ready yet (attempt {attempt + 1}), retrying in 1s...")
                        time.sleep(1)
                    else:
                        logger.error(f"Table {table_name} still not accessible after {max_retries} attempts: {e}")
                        raise e
    
    def get_table_ref(self, platform: Platform, table_type: str):
        """Get table reference for a platform and table type with explicit project"""
        table_name = TABLE_MAPPING[platform][table_type]
        # Use explicit project ID to avoid confusion
        return bigquery.Table(f"{self.project_id}.{self.dataset_id}.{table_name}")
    
    def insert_rows(self, platform: Platform, table_type: str, rows_data: list) -> tuple[bool, list]:
        """Insert rows into specified table with explicit project ID"""
        try:
            if not self.client:
                return False, ["BigQuery client not initialized"]
            
            table_name = TABLE_MAPPING[platform][table_type]
            
            # Use explicit full table ID to avoid project mismatch
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            logger.info(f"[DEBUG] Inserting to table: {full_table_id}")
            logger.info(f"[DEBUG] Client project: {self.client.project}")
            
            # Get table using full table ID
            table = self.client.get_table(full_table_id)
            logger.info(f"[DEBUG] Table retrieved: {table.full_table_id}")
            
            # Convert datetime objects to ISO strings for insert_rows_json compatibility
            json_serializable_data = convert_batch_datetime_for_json(rows_data)
            
            errors = self.client.insert_rows_json(table, json_serializable_data)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False, errors
            
            logger.info(f"Successfully inserted {len(rows_data)} rows into {full_table_id}")
            return True, []
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error inserting rows: {error_msg}")
            return False, [error_msg]
    
    def merge_rows(self, platform: Platform, table_type: str, rows_data: list, unique_field: str) -> tuple[bool, list]:
        """
        Safe version using parameterized queries - handles timestamps properly
        """
        try:
            if not self.client:
                return False, ["BigQuery client not initialized"]

            if not rows_data:
                return True, [] 

            table_name = TABLE_MAPPING[platform][table_type]
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            logger.info(f"[DEBUG] Safe merging to table: {full_table_id}")
            logger.info(f"[DEBUG] Unique field: {unique_field}")
            logger.info(f"[DEBUG] Rows count: {len(rows_data)}")

            # Get table schema to understand field types
            table = self.client.get_table(full_table_id)
            schema_dict = {field.name: field.field_type for field in table.schema}
            
            # For small datasets, use individual INSERT IGNORE (simpler)
            if len(rows_data) <= 10:
                return self._merge_rows_individual(full_table_id, rows_data, unique_field, schema_dict)
            
            # For larger datasets, use the dynamic SQL approach with proper typing
            return self._merge_rows_bulk(full_table_id, rows_data, unique_field, schema_dict)
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error in safe merge: {error_msg}")
            return False, [error_msg]

    def _merge_rows_individual(self, full_table_id: str, rows_data: list, unique_field: str, schema_dict: dict) -> tuple[bool, list]:
        """Handle small datasets with individual queries"""
        try:
            for i, row in enumerate(rows_data):
                # Build parameterized INSERT with conflict handling
                columns = list(row.keys())
                placeholders = [f"@param_{col}" for col in columns]
                
                # Use INSERT statement with WHERE NOT EXISTS to avoid duplicates
                query = f"""
                INSERT INTO `{full_table_id}` ({', '.join(columns)})
                SELECT {', '.join(placeholders)}
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{full_table_id}` WHERE {unique_field} = @param_{unique_field}
                )
                """
                
                # Build parameters with proper types
                query_parameters = []
                for col, value in row.items():
                    param_type = "STRING"  # default
                    if col in schema_dict:
                        if schema_dict[col] == "TIMESTAMP":
                            param_type = "TIMESTAMP"
                        elif schema_dict[col] == "INTEGER":
                            param_type = "INT64"
                        elif schema_dict[col] == "BOOLEAN":
                            param_type = "BOOL"
                        # Add more type mappings as needed
                    
                    # Handle timestamp string conversion
                    param_value = value
                    if param_type == "TIMESTAMP" and isinstance(value, str):
                        # BigQuery will parse ISO format timestamps
                        param_value = value
                    
                    query_parameters.append(
                        bigquery.ScalarQueryParameter(f"param_{col}", param_type, param_value)
                    )
                
                job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
                job = self.client.query(query, job_config=job_config)
                job.result()
            
            logger.info(f"Successfully processed {len(rows_data)} rows individually")
            return True, []
            
        except Exception as e:
            logger.error(f"Error in individual merge: {e}")
            return False, [str(e)]

    def _merge_rows_bulk(self, full_table_id: str, rows_data: list, unique_field: str, schema_dict: dict) -> tuple[bool, list]:
        """Handle larger datasets with bulk MERGE - improved timestamp handling"""
        def format_value_with_schema(key, value):
            if value is None:
                return "NULL"
            
            # Check schema for proper type handling
            field_type = schema_dict.get(key, "STRING")
            
            if field_type == "TIMESTAMP":
                if isinstance(value, str):
                    return f'TIMESTAMP("{value}")'
                else:
                    return f'TIMESTAMP("{str(value)}")'
            elif field_type == "STRING":
                safe_v = str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                return f'"{safe_v}"'
            elif field_type == "BOOLEAN":
                return "TRUE" if value else "FALSE"
            elif field_type in ["INTEGER", "INT64", "FLOAT", "NUMERIC"]:
                return str(value)
            else:
                # Default to string handling
                safe_v = str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                return f'"{safe_v}"'
        
        try:
            struct_rows = []
            for row in rows_data:
                fields = []
                for k, v in row.items():
                    formatted_value = format_value_with_schema(k, v)
                    fields.append(f"{formatted_value} AS {k}")
                struct_rows.append(f"STRUCT({', '.join(fields)})")
            values_clause = ",\n".join(struct_rows)

            columns = ", ".join(rows_data[0].keys())
            
            query = f"""
            MERGE `{full_table_id}` T
            USING (
                SELECT * FROM UNNEST([
                    {values_clause}
                ])
            ) S
            ON T.{unique_field} = S.{unique_field}
            WHEN NOT MATCHED THEN
                INSERT ({columns}) VALUES ({columns})
            """
            
            job = self.client.query(query)
            job.result()
            
            logger.info(f"Successfully bulk merged {len(rows_data)} rows")
            return True, []
            
        except Exception as e:
            logger.error(f"Error in bulk merge: {e}")
            # Log partial query for debugging
            if 'query' in locals():
                logger.error(f"Failed query sample: {query[:300]}...")
            return False, [str(e)]

    def query_table(self, query: str, job_config: bigquery.QueryJobConfig = None):
        """Execute a query and return results"""
        try:
            if not self.client:
                raise Exception("BigQuery client not initialized")

            job = self.client.query(query, job_config=job_config)
            return list(job.result())
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def upsert_data(self, platform: Platform, table_type: str, data: pd.DataFrame | list | dict, merge_key: str):
        """
        🎯 MAIN METHOD: Insert new records + Update existing records
        
        Args:
            platform (Platform): The platform for the data
            table_type (str): Target table type
            data: Can be:
                - pandas.DataFrame
                - List of dictionaries  
                - Single dictionary
            merge_key (str): Column name to match for updates
            
        Examples:
            # Single record (will be inserted or updated)
            bq.upsert_data("users", {"account_id": "ACC001", "name": "John"}, "account_id")
            
            # Multiple records (mixed insert/update)
            data = [
                {"account_id": "ACC001", "name": "John Updated"},  # Will UPDATE if exists
                {"account_id": "ACC002", "name": "Jane New"}       # Will INSERT if not exists
            ]
            bq.upsert_data("users", data, "account_id")
            
            # DataFrame
            df = pd.DataFrame(data)
            bq.upsert_data("users", df, "account_id")
        """
        # Ensure table exists
        if not self.client:
            raise Exception("BigQuery client not initialized")

        # Convert input to DataFrame
        df = self._convert_to_dataframe(data)
        
        if df is None or len(df) == 0:
            logger.warning("No data to upsert")
            return
        
        # Validate merge_key exists in data
        if merge_key not in df.columns:
            raise ValueError(f"merge_key '{merge_key}' not found in data columns: {df.columns.tolist()}")
        
        # Remove rows with null merge_key
        original_count = len(df)
        df = df.dropna(subset=[merge_key])
        if len(df) < original_count:
            print(f"Removed {original_count - len(df)} rows with null {merge_key}")
        
        if len(df) == 0:
            print(f"No valid data to upsert (all {merge_key} values are null)")
            return
        
        print(f"Upserting {len(df)} records based on {merge_key}...")
        
        table_id = TABLE_MAPPING[platform][table_type]

        # Create temporary table for merge operation
        temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"
        
        try:
            # Step 1: Validate target table exists
            self._validate_target_table(table_id)

            # Determine schema and valid columns
            target_schema = get_schema(platform, table_type)
            schema_names = {f.name for f in target_schema}
            merge_columns = [c for c in df.columns.tolist() if c in schema_names]
            if merge_key not in merge_columns and merge_key in df.columns:
                merge_columns.append(merge_key)
            
            # Prepare records (as dicts)
            records = df.to_dict(orient="records")
            
            # Detect nested/repeated structures
            def _has_nested(row: dict) -> bool:
                for v in row.values():
                    if isinstance(v, (list, dict)):
                        return True
                return False
            has_nested = any(_has_nested(r) for r in records)
            
            # Step 2: Load data to temporary table
            if has_nested or table_type == "profiles":
                # Use JSON load with explicit schema for nested fields
                self._load_temp_table_json(temp_table_id, records, platform, table_type)
            else:
                # Flat schema, can use DataFrame load
                self._load_temp_table(temp_table_id, df[merge_columns])
            
            # Step 3: Execute MERGE statement
            affected_rows = self._execute_merge(table_id, temp_table_id, merge_columns, merge_key)
            
            print(f"Successfully upserted {len(df)} records in {table_id}")
            print(f"Affected rows: {affected_rows}")
            
            return affected_rows
            
        except Exception as e:
            print(f"Upsert failed: {e}")
            raise
        finally:
            # Step 4: Clean up temporary table
            self._cleanup_temp_table(temp_table_id)

    def _validate_target_table(self, table_id):
        """Validate that target table exists"""
        try:
            table_ref = self.dataset_ref.table(table_id)
            self.client.get_table(table_ref)  # This will raise if table doesn't exist
        except Exception as e:
            raise ValueError(f"Target table '{table_id}' does not exist: {e}")

    def _convert_to_dataframe(self, data):
        """Convert various input types to DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, list):
            return pd.DataFrame(data) if data else None
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            raise ValueError(f"Unsupported data type: {type(data)}. Use DataFrame, list of dicts, or dict")

    def _load_temp_table(self, temp_table_id, df):
        """Load DataFrame to temporary table"""
        table_ref = self.dataset_ref.table(temp_table_id)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Overwrite temp table
            autodetect=True  # Auto-detect schema from DataFrame
        )
        if 'parent_account_id' in df.columns and df['parent_account_id'] is not None:
            df['parent_account_id'] = df['parent_account_id'].astype(str)
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        print(f"Loaded {len(df)} rows to temporary table")

    def _load_temp_table_json(self, temp_table_id: str, rows: list[dict], platform: Platform, table_type: str):
        """Load JSON rows to temporary table with explicit schema (for nested RECORD fields)."""
        table_ref = self.dataset_ref.table(temp_table_id)
        schema = get_schema(platform, table_type)
        # Ensure datetimes are JSON-serializable (RFC3339 strings)
        serializable_rows = convert_batch_datetime_for_json(rows)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema,
            ignore_unknown_values=True,
        )
        job = self.client.load_table_from_json(serializable_rows, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {len(rows)} rows to temporary table (JSON load)")

    def _execute_merge(self, target_table_id, temp_table_id, columns, merge_key):
        """Execute MERGE statement with improved error handling"""
        
        # Generate update clause (exclude merge_key from updates)
        update_columns = [col for col in columns if col != merge_key]
        
        if not update_columns:
            # If only merge_key exists, we can only do INSERT
            print("Only merge_key column found, performing INSERT only (no updates)")
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{target_table_id}` AS target
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN NOT MATCHED THEN
                INSERT ({merge_key})
                VALUES (source.{merge_key})
            """
        else:
            # Full UPSERT with UPDATE and INSERT
            update_clause = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])
            
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{target_table_id}` AS target
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
        
        print(f"Executing MERGE on {merge_key}...")
        query_job = self.client.query(merge_query)
        result = query_job.result()  # Wait for completion
        
        # Return number of affected rows
        return result.num_dml_affected_rows if hasattr(result, 'num_dml_affected_rows') else 0

    def _cleanup_temp_table(self, temp_table_id):
        """Clean up temporary table"""
        try:
            table_ref = self.dataset_ref.table(temp_table_id)
            self.client.delete_table(table_ref)
            print(f"Cleaned up temporary table: {temp_table_id}")
        except Exception as e:
            print(f"Could not clean up temp table {temp_table_id}: {e}")

    def insert_if_not_exists(self, platform: Platform, table_type: str, data: pd.DataFrame | list[dict] | dict, unique_key: str):
        """
        🎯 INSERT ONLY: Insert new records, skip existing ones
        
        Args:
            table_id (str): Target table name  
            data: DataFrame, list of dicts, or single dict
            unique_key (str): Column name to check for duplicates (like account_id)
        
        Example:
            # Chỉ insert record mới, bỏ qua nếu account_id đã tồn tại
            bq.insert_if_not_exists("users", data, "account_id")
        """
        
        df = self._convert_to_dataframe(data)
        
        if df is None or len(df) == 0:
            print("No data to insert")
            return
        
        if unique_key not in df.columns:
            raise ValueError(f"unique_key '{unique_key}' not found in data columns")
        
        # Remove null values
        df = df.dropna(subset=[unique_key])
        if len(df) == 0:
            print(f"No valid data (all {unique_key} values are null)")
            return
        
        print(f"Inserting {len(df)} records (skip if {unique_key} exists)...")

        table_id = TABLE_MAPPING[platform][table_type]

        temp_table_id = f"{table_id}_insert_temp_{int(datetime.now().timestamp())}"
        
        try:
            # Determine schema and valid columns
            target_schema = get_schema(platform, table_type)
            schema_names = {f.name for f in target_schema}
            insert_columns_list = [c for c in df.columns.tolist() if c in schema_names]
            if unique_key not in insert_columns_list and unique_key in df.columns:
                insert_columns_list.append(unique_key)

            # Load to temp table (use JSON for nested)
            records = df.to_dict(orient="records")
            def _has_nested(row: dict) -> bool:
                for v in row.values():
                    if isinstance(v, (list, dict)):
                        return True
                return False
            has_nested = any(_has_nested(r) for r in records)
            
            if has_nested or table_type == "profiles":
                self._load_temp_table_json(temp_table_id, records, platform, table_type)
            else:
                self._load_temp_table(temp_table_id, df[insert_columns_list])
            
            # INSERT only new records
            insert_columns = ", ".join(insert_columns_list)
            insert_values = ", ".join([f"source.{col}" for col in insert_columns_list])
            
            insert_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{table_id}` AS target
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
            ON target.{unique_key} = source.{unique_key}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
            
            query_job = self.client.query(insert_query)
            result = query_job.result()
            
            inserted_rows = result.num_dml_affected_rows if hasattr(result, 'num_dml_affected_rows') else 0
            skipped_rows = len(df) - inserted_rows
            
            print(f"Inserted: {inserted_rows} new records")
            print(f"Skipped: {skipped_rows} existing {unique_key}s")
            
            return inserted_rows
            
        except Exception as e:
            print(f"Insert failed: {e}")
            raise
        finally:
            self._cleanup_temp_table(temp_table_id)

    def get_pending_keywords(self, platform: Platform, limit: int = 100, extension_id: str = None):
        """Get keywords available for processing by this extension"""
        try:
            table_name = TABLE_MAPPING[platform]["keywords"]
            
            if extension_id:
                # If extension_id provided, get:
                # 1. Keywords already processing by this extension (to continue)
                # 2. Keywords that are pending (available for pickup)
                query = f"""
                SELECT id, keyword, status, start, extension_id
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE (
                    (status = 'processing' AND extension_id = '{extension_id}')
                    OR 
                    (status = 'pending' AND (extension_id IS NULL OR extension_id = ''))
                )
                ORDER BY 
                    CASE WHEN status = 'processing' THEN 1 ELSE 2 END,
                    created_at ASC
                LIMIT {limit}
                """
            else:
                # If no extension_id, get all pending keywords (available for pickup)
                query = f"""
                SELECT id, keyword, status, start, extension_id
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE status = 'pending' AND (extension_id IS NULL OR extension_id = '')
                ORDER BY created_at ASC
                LIMIT {limit}
                """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            keywords = []
            for row in results:
                keywords.append({
                    "id": row.id,
                    "keyword": row.keyword,
                    "status": row.status,
                    "start": row.start or 0,
                    "extension_id": row.extension_id
                })
            
            return keywords
            
        except Exception as e:
            logger.error(f"Error getting keywords for {platform.value}: {e}")
            return []

# Global instance
bigquery_service = BigQueryService()
