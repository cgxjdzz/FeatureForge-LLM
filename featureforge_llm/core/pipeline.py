"""
Main Coordination Class, Streamlined LLMFeaturePipeline
"""
import time
import os
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

from ..core.utils import create_provider_instance, save_suggestions_to_file, load_suggestions_from_file, generate_report
from ..llm.base import LLMProvider
from ..parsers.json_parser import JsonParser
from ..parsers.code_parser import CodeParser
from ..data.data_analyzer import DataAnalyzer
from ..executors.code_executor import CodeExecutor
from ..data.feature_implementer import FeatureImplementer

class LLMFeaturePipeline:
    """
    LLM-driven Feature Engineering Pipeline, Implementing Full Workflow of Asking Suggestions - Obtaining Suggestions - Implementing Code - Obtaining New Features
    """
    
    def __init__(self, llm_api_key: str, model: str = "gpt-4", verbose: bool = True, provider: str = "openai"):
        """
        Initialize LLM Feature Engineering Pipeline
        
        Parameters:
            llm_api_key: LLM API key
            model: LLM model to use
            verbose: Whether to print detailed information
            provider: LLM provider, supports "openai" or "gemini"
        """
        self.verbose = verbose
        
        # Create LLM provider
        try:
            self.llm_provider = create_provider_instance(provider, llm_api_key, model, verbose)
        except Exception as e:
            if self.verbose:
                print(f"⚠️ Failed to initialize LLM provider: {e}")
            self.llm_provider = None
        
        # Create core components
        self.json_parser = JsonParser(verbose=verbose)
        self.code_parser = CodeParser(verbose=verbose)
        self.data_analyzer = DataAnalyzer(verbose=verbose)
        self.code_executor = CodeExecutor(verbose=verbose)
        self.feature_implementer = FeatureImplementer(self.llm_provider, self.code_executor, verbose=verbose)
        
        # Initialize state
        self.feature_suggestions = []
        self.implemented_features = {}
        self.execution_history = []
        self.start_time = time.time()
        self.feature_transformations = [] 
    
    def ask_for_feature_suggestions(self, df: pd.DataFrame, 
                                  task_description: str, 
                                  target_column: Optional[str] = None,
                                  dataset_background: Optional[str] = None,
                                  custom_prompt: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Ask LLM for Feature Engineering Suggestions
        
        Parameters:
            df: Input dataframe
            task_description: Task description
            target_column: Target column name
            dataset_background: Dataset background information to help the model understand the data
            custom_prompt: Custom prompt (if needed)
            
        Returns:
            List of feature engineering suggestions
        """
        if not self.llm_provider:
            if self.verbose:
                print("❌ LLM provider not initialized, cannot request suggestions")
            return []
            
        # Prepare dataframe information
        df_info = self.data_analyzer.get_dataframe_info(df)
        data_sample = df.head(3).to_dict() if df.shape[0] > 0 else {}
        
        system_message = """You are a professional feature engineering expert, skilled at discovering patterns in data and creating valuable features.
Please provide specific, executable feature engineering suggestions. Reply in JSON format."""
        
        if custom_prompt:
            prompt = custom_prompt
        else:
            background_section = ""
            if dataset_background:
                background_section = f"""
Dataset Background:
{dataset_background}
"""

            prompt = f"""
I have a machine learning project and need your help with feature engineering.
            
Task Description: {task_description}

{"Target Column: " + target_column if target_column else ""}
{background_section}
Dataset Information:
- Shape: {df_info['shape']}
- Columns: {df_info['columns']}
- Data Types: {df_info['dtypes']}
- Missing Values: {df_info['missing_values']}
- Unique Value Counts: {df_info['unique_values']}

Categorical Feature Distributions:
{df_info.get('categorical_distributions', {})}

Numerical Feature Statistics:
{df_info.get('numerical_statistics', {})}

Data Sample:
{data_sample}

Please provide 5-10 valuable feature engineering suggestions, including:
1. Feature Transformations (such as binarization, standardization, one-hot encoding, etc.)
2. Feature Interactions (such as feature combinations, ratio features, etc.)
3. Domain Knowledge-based Features (such as time features, text features, etc.)

For each suggestion, please provide the following information in a JSON array format:
[
  {{
    "suggestion_id": "Unique identifier",
    "suggestion_type": "Transformation|Interaction|Domain Knowledge|Other",
    "description": "Detailed suggestion description",
    "rationale": "Why this feature might be valuable",
    "affected_columns": ["Affected columns"],
    "new_features": ["New generated feature names"]
  }},
  ...
]
"""
        if self.verbose:
            print("🔍 Asking LLM for feature engineering suggestions...")
            
        response = self.llm_provider.call(prompt, system_message)
        
        try:
            suggestions = self.json_parser.parse_json_from_response(response)
            if isinstance(suggestions, list):
                self.feature_suggestions = suggestions
                if self.verbose:
                    print(f"✅ Received {len(suggestions)} feature engineering suggestions")
                return suggestions
            else:
                if self.verbose:
                    print("⚠️ LLM returned incorrect format, attempting to extract suggestions")
                extracted_suggestions = self.json_parser._extract_suggestions_from_text(response)
                self.feature_suggestions = extracted_suggestions
                return extracted_suggestions
        except Exception as e:
            if self.verbose:
                print(f"❌ Failed to parse suggestions: {e}")
            return []

    def implement_feature_suggestion(self, df: pd.DataFrame, suggestion_id: str, 
                                    keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Implement a specific feature engineering suggestion
        
        Parameters:
            df: Input dataframe
            suggestion_id: Suggestion ID
            keep_original: Whether to keep original features
            
        Returns:
            (Updated dataframe, Implementation result information)
        """
        # Find corresponding suggestion
        suggestion = None
        for s in self.feature_suggestions:
            if s.get("suggestion_id") == suggestion_id:
                suggestion = s
                break
                
        if not suggestion:
            if self.verbose:
                print(f"❌ Cannot find suggestion with ID {suggestion_id}")
            return df, {"status": "error", "message": f"Cannot find suggestion with ID {suggestion_id}"}
        
        # Implement suggestion
        result_df, impl_result = self.feature_implementer.implement_suggestion(df, suggestion, keep_original)
        
        # Record results
        self.implemented_features[suggestion_id] = impl_result
        self.execution_history.append(impl_result)
        
        return result_df, impl_result
    
    def implement_all_suggestions(self, df: pd.DataFrame, keep_original: bool = True) -> pd.DataFrame:
        """
        Implement all feature engineering suggestions
        
        Parameters:
            df: Input dataframe
            keep_original: Whether to keep original features
            
        Returns:
            Dataframe containing all new features
        """
        df, self.feature_transformations = self.feature_implementer.implement_all_suggestions(df, self.feature_suggestions, keep_original)
        return df
    
    def custom_feature_request(self, df: pd.DataFrame, feature_description: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Create features based on custom description
        
        Parameters:
            df: Input dataframe
            feature_description: Feature description
            
        Returns:
            (Updated dataframe, Implementation result information)
        """
        result_df, impl_result = self.feature_implementer.custom_feature_request(df, feature_description)
        
        # Add custom feature to suggestion list
        if impl_result["status"] == "success":
            suggestion_id = impl_result["suggestion_id"]
            suggestion = {
                "suggestion_id": suggestion_id,
                "suggestion_type": "Custom",
                "description": feature_description,
                "rationale": "User-defined feature",
                "implementation": impl_result["code"],
                "affected_columns": [],
                "new_features": impl_result["new_features"]
            }
            
            self.feature_suggestions.append(suggestion)
            self.implemented_features[suggestion_id] = impl_result
            self.execution_history.append(impl_result)
        
        return result_df, impl_result
    
    def save_suggestions(self, file_path: str) -> bool:
        """
        Save feature suggestions to file
        
        Parameters:
            file_path: File path
            
        Returns:
            Whether saving was successful
        """
        return save_suggestions_to_file(self.feature_suggestions, file_path)
    
    def load_suggestions(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load feature suggestions from file
        
        Parameters:
            file_path: File path
            
        Returns:
            List of loaded suggestions
        """
        suggestions = load_suggestions_from_file(file_path)
        if suggestions:
            self.feature_suggestions = suggestions
        return suggestions
    
    def generate_report(self, original_df: pd.DataFrame, result_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate feature engineering report
        
        Parameters:
            original_df: Original dataframe
            result_df: Result dataframe
            
        Returns:
            Report data
        """
        return generate_report(
            self.implemented_features, 
            self.execution_history,
            original_df,
            result_df
        )
    
    def get_execution_time(self) -> float:
        """
        Get execution time (seconds)
        
        Returns:
            Execution time
        """
        return time.time() - self.start_time
    
    def analyze_correlations(self, df: pd.DataFrame, target_column: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze correlations between numerical features
        
        Parameters:
            df: Input dataframe
            target_column: Target column name
            
        Returns:
            Correlation analysis results
        """
        return self.data_analyzer.analyze_correlations(df, target_column)
    
    def detect_skewed_features(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Detect highly skewed numerical features
        
        Parameters:
            df: Input dataframe
            
        Returns:
            Feature skewness dictionary
        """
        return self.data_analyzer.detect_skewed_features(df)
    
    def suggest_feature_transformations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Suggest feature transformations based on data analysis
        
        Parameters:
            df: Input dataframe
            
        Returns:
            List of feature transformation suggestions
        """
        return self.data_analyzer.suggest_feature_transformations(df)
    
    def benchmark_feature_implementation(self, df: pd.DataFrame, 
                                       suggestion_id: str, 
                                       iterations: int = 3) -> Dict[str, Any]:
        """
        Perform performance benchmark for feature implementation
        
        Parameters:
            df: Input dataframe
            suggestion_id: Suggestion ID
            iterations: Number of executions
            
        Returns:
            Benchmark test results
        """
        # Find corresponding suggestion
        suggestion = None
        for s in self.feature_suggestions:
            if s.get("suggestion_id") == suggestion_id:
                suggestion = s
                break
                
        if not suggestion:
            if self.verbose:
                print(f"❌ Cannot find suggestion with ID {suggestion_id}")
            return {"status": "error", "message": f"Cannot find suggestion with ID {suggestion_id}"}
        
        # Extract implementation code
        implementation_code = suggestion.get("implementation", "")
        implementation_code = self.code_parser.clean_implementation_code(implementation_code)
        
        if not implementation_code or implementation_code == "# Needs manual implementation":
            if self.verbose:
                print("❌ No implementation code in suggestion, cannot perform benchmark test")
            return {"status": "error", "message": "No implementation code in suggestion"}
        
        # Ensure code is in function structure
        implementation_code = self.code_parser.ensure_function_structure(
            implementation_code, 
            f"feature_{suggestion_id.replace('-', '_').replace('.', '_')}"
        )
        
        # Execute benchmark test
        return self.code_executor.benchmark_execution(df, implementation_code, iterations)
    

    def apply_saved_transformations(self, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the saved feature transformations on a new dataset (e.g., test dataset).
        
        Parameters:
            new_df (pd.DataFrame): The dataset to apply transformations to.
        
        Returns:
            pd.DataFrame: The transformed dataset.
        """
        if not self.feature_transformations:
            if self.verbose:
                print("⚠️ No saved feature transformations found. Returning original dataset.")
            return new_df
        
        transformed_df = new_df.copy()
        
        for transformation in self.feature_transformations:
            if transformation["status"] != "success" or not transformation["used_implementation_code"]:
                if self.verbose:
                    print(f"⚠️ Skipping transformation {transformation['suggestion_id']} due to error.")
                continue
            
            # Extract the implementation code
            implementation_code = transformation["used_implementation_code"]
            
            # Execute the saved code using CodeExecutor
            transformed_df, exec_result = self.code_executor.execute(transformed_df, implementation_code)
            
            # Handle execution errors
            if exec_result["status"] != "success":
                if self.verbose:
                    print(f"❌ Error applying transformation {transformation['suggestion_id']}: {exec_result['error']}")
        
        return transformed_df



    def get_status_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current pipeline status, including more detailed statistics.
        
        Returns:
            A dictionary containing various statistics about the pipeline execution.
        """
        # 1. Basic statistics
        total_suggestions = len(self.feature_suggestions)  # Total number of feature suggestions
        implemented_count = len(self.implemented_features)  # Total number of implemented suggestions

        # Separate successful and failed implementations
        successful_records = [r for r in self.implemented_features.values() if r.get("status") == "success"]
        failed_records = [r for r in self.implemented_features.values() if r.get("status") != "success"]

        successful_count = len(successful_records)  # Number of successful implementations
        failed_count = len(failed_records)  # Number of failed implementations
        unimplemented_count = total_suggestions - implemented_count  # Number of unimplemented suggestions

        # 2. Count the number of new features created from successful implementations
        total_new_features = 0
        for record in successful_records:
            new_feats = record.get("new_features", [])
            if isinstance(new_feats, list):
                total_new_features += len(new_feats)

        # 3. Collect error details from failed implementations
        # Each failed record might have "error_message" and "traceback" information
        error_details = []
        for record in failed_records:
            error_info = {
                "suggestion_id": record.get("suggestion_id"),
                "status": record.get("status"),
                "error_message": record.get("error_message", ""),
                "traceback": record.get("traceback", "")
            }
            error_details.append(error_info)

        # 4. Calculate the total execution time since the pipeline was initialized
        total_execution_time = self.get_execution_time()

        # 5. Compute the average implementation time per feature implementation
        history_execution_times = [
            h.get("execution_time", 0.0) for h in self.execution_history if isinstance(h, dict)
        ]
        avg_implementation_time = 0.0
        if len(history_execution_times) > 0:
            avg_implementation_time = sum(history_execution_times) / len(history_execution_times)

        # 6. Return a comprehensive status summary
        summary = {
            "total_suggestions": total_suggestions,         # Total number of feature suggestions
            "unimplemented_count": unimplemented_count,     # Number of unimplemented suggestions
            "implemented_count": implemented_count,         # Number of implemented suggestions
            "successful_count": successful_count,           # Number of successfully implemented features
            "failed_count": failed_count,                   # Number of failed implementations
            "total_new_features_created": total_new_features,  # Total number of new features generated
            "execution_time": total_execution_time,         # Total execution time since pipeline initialization
            "avg_implementation_time": avg_implementation_time,  # Average time spent per feature implementation
            "provider": self.llm_provider.get_provider_info() if self.llm_provider else None,
            "failed_records": error_details,                # List of failed implementation records
            "execution_history": self.execution_history     # Full history of feature implementations
        }
        
        return summary
