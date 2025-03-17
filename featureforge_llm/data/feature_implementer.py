"""
Feature Implementer
"""
import pandas as pd
import time
from typing import Dict, Any, List, Tuple, Optional

from ..llm.base import LLMProvider
from ..executors.code_executor import CodeExecutor
from ..data.data_analyzer import DataAnalyzer
from ..parsers.code_parser import CodeParser

class FeatureImplementer:
    """
    Implement Feature Engineering Suggestions
    """
    
    def __init__(self, llm_provider: LLMProvider, code_executor: CodeExecutor, verbose: bool = True):
        """
        Initialize Feature Implementer
        
        Parameters:
            llm_provider: LLM provider
            code_executor: Code executor
            verbose: Whether to print detailed information
        """
        self.llm_provider = llm_provider
        self.code_executor = code_executor
        self.verbose = verbose
        self.data_analyzer = DataAnalyzer(verbose=verbose)
        self.code_parser = CodeParser(verbose=verbose)
        self.implemented_features = {}
    
    def implement_suggestion(self, df: pd.DataFrame, suggestion: Dict[str, Any], 
                                keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Implement a specific feature engineering suggestion
        
        Parameters:
            df: Input dataframe
            suggestion: Feature suggestion dictionary
            keep_original: Whether to keep original features
            
        Returns:
            (Updated dataframe, Implementation result information)
        """
        suggestion_id = suggestion.get("suggestion_id")
        if not suggestion_id:
            if self.verbose:
                print("❌ Suggestion missing ID")
            return df, {"status": "error", "message": "Suggestion missing ID"}
            
        if self.verbose:
            print(f"🔧 Implementing suggestion: {suggestion.get('description', suggestion_id)}")
        
        # If no implementation code, use LLM to generate code
        implementation_code = suggestion.get("implementation")
        if not implementation_code or implementation_code == "# Needs manual implementation":
            
            # Call method to generate code
            implementation_code = self.generate_implementation_code(df, suggestion)
            
            # Update implementation code in suggestion
            suggestion["implementation"] = implementation_code
            used_implementation_code = implementation_code

        # Clean implementation code
        implementation_code = self.code_parser.clean_implementation_code(implementation_code)
        
        # Ensure code is in function structure
        implementation_code = self.code_parser.ensure_function_structure(
            implementation_code, 
            f"feature_{suggestion_id.replace('-', '_').replace('.', '_')}"
        )
        
        # Implement suggestion
        result_df, impl_result = self.code_executor.execute(df, implementation_code, suggestion, keep_original)
        used_implementation_code = implementation_code
        # If execution fails, try to fix code
        if impl_result["status"] == "error" and self.llm_provider:
            if self.verbose:
                print("🔄 Execution failed, attempting to fix code...")
                
            # Get dataframe info for code fixing
            df_info = self.data_analyzer.get_dataframe_info(df)
            
            # Fix code
            fixed_code = self.code_executor.fix_code(
                implementation_code, 
                impl_result["error"], 
                df_info, 
                self.llm_provider
            )
            
            if fixed_code != implementation_code:
                if self.verbose:
                    print("🔧 Retrying with fixed code...")
                    
                # Retry with fixed code
                result_df, impl_result = self.code_executor.execute(df, fixed_code, suggestion, keep_original)
                
                # Update implementation code in suggestion
                if impl_result["status"] == "success":
                    suggestion["implementation"] = fixed_code
                    used_implementation_code = fixed_code
        # Record implementation result
        self.implemented_features[suggestion_id] = impl_result
        impl_result["used_implementation_code"] = used_implementation_code

        return result_df, impl_result
    
    def generate_implementation_code(self, df: pd.DataFrame, suggestion: Dict[str, Any]) -> str:
        """
        Generate implementation code for a suggestion
        
        Parameters:
            df: Input dataframe
            suggestion: Suggestion details
            
        Returns:
            Implementation code
        """
        if not self.llm_provider:
            if self.verbose:
                print("⚠️ Missing LLM provider, cannot generate code")
            return "# Missing LLM provider, cannot generate code\ndef implement_feature(df):\n    return df"
        
        # Get dataframe information
        df_info = self.data_analyzer.get_dataframe_info(df)
        
        system_message = """You are a feature engineering expert capable of writing high-quality Python code to implement feature engineering.
Please provide a complete, executable Python function to implement the required feature engineering for the input DataFrame.
The code should be robust, able to handle edge cases such as missing values and outliers."""
        
        prompt = f"""
Please write Python implementation code based on the following feature engineering suggestion:

Suggestion description: {suggestion.get('description', '')}
Suggestion rationale: {suggestion.get('rationale', '')}
Suggestion type: {suggestion.get('suggestion_type', 'Unknown')}
Affected columns: {suggestion.get('affected_columns', [])}
Expected new features: {suggestion.get('new_features', [])}

Dataset information:
- Shape: {df_info['shape']}
- Columns: {df_info['columns']}
- Data types: {df_info['dtypes']}
- Missing values: {df_info['missing_values']}
- Unique value counts: {df_info['unique_values']}

Please write a Python function named `implement_feature` that:
1. Accepts a pandas DataFrame as input
2. Implements the above feature engineering suggestion
3. Returns a DataFrame with new features

The code should:
- Handle potential missing values
- Include appropriate comments
- Follow Python best practices
- Not use external data sources

Key suggestions:
- For feature transformations, consider using built-in pandas and numpy methods
- For feature interactions, use column combinations or mathematical operations
- For domain knowledge features, extract meaningful information

Please return only Python code, no explanation needed.
"""
        
        if self.verbose:
            print("🔬 Generating feature implementation code...")
        
        response = self.llm_provider.call(prompt, system_message)
        code = self.code_parser.parse_code_from_response(response)
        
        if not code:
            # If no code was extracted, use a simple template
            code = f"""def implement_feature(df):
        \"\"\"
        Implementation: {suggestion.get('description', '')}
        
        Parameters:
            df: Input dataframe
            
        Returns:
            Dataframe with new features
        \"\"\"
        # Create a copy of the dataframe to avoid modifying original data
        df_result = df.copy()
        
        # TODO: Implement feature engineering logic
        # Possible steps:
        # 1. Handle missing values
        # 2. Create new features
        # 3. Perform necessary transformations
        
        return df_result
    """
        
        return code
    
    def implement_all_suggestions(self, df: pd.DataFrame, 
                                suggestions: List[Dict[str, Any]],
                                keep_original: bool = True) -> pd.DataFrame:
        """
        Implement all feature engineering suggestions
        
        Parameters:
            df: Input dataframe
            suggestions: List of suggestions
            keep_original: Whether to keep original features
            
        Returns:
            Dataframe containing all new features
        """
        if not suggestions:
            if self.verbose:
                print("⚠️ No feature engineering suggestions available")
            return df
            
        result_df = df.copy()
        successful_count = 0
        execution_details = []

        for i, suggestion in enumerate(suggestions):
            suggestion_id = suggestion.get("suggestion_id")
            
            if not suggestion_id:
                continue
                
            if self.verbose:
                print(f"🔍 Implementing suggestion {i+1}/{len(suggestions)}: {suggestion.get('description', '')}")
                
            try:
                result_df, impl_result = self.implement_suggestion(result_df, suggestion, keep_original)

                execution_details.append({
                    "suggestion_id": suggestion_id,
                    "description": suggestion.get('description', ''),
                    "status": impl_result.get("status"),
                    "message": impl_result.get("message", ""),
                    "used_implementation_code": impl_result.get("used_implementation_code", ""),
                })

                if impl_result["status"] == "success":
                    successful_count += 1
            except Exception as e:
                if self.verbose:
                    print(f"❌ Unhandled error implementing suggestion {suggestion_id}: {e}")
                execution_details.append({
                    "suggestion_id": suggestion_id,
                    "description": suggestion.get('description', ''),
                    "status": "error",
                    "message": str(e),
                    "used_implementation_code": "",
                })


        if self.verbose:
            print(f"✅ Successfully implemented {successful_count}/{len(suggestions)} suggestions")
            print(f"🆕 Total new features: {len(result_df.columns) - len(df.columns)}")
            
        return result_df, execution_details
    
    def custom_feature_request(self, df: pd.DataFrame, feature_description: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Create features based on custom description
        
        Parameters:
            df: Input dataframe
            feature_description: Feature description
            
        Returns:
            (Updated dataframe, Implementation result information)
        """
        if not self.llm_provider:
            if self.verbose:
                print("⚠️ Missing LLM provider, cannot process custom feature request")
            return df, {"status": "error", "message": "Missing LLM provider"}
            
        if self.verbose:
            print(f"🔍 Processing custom feature request: {feature_description}")
            
        df_info = self.data_analyzer.get_dataframe_info(df)
        
        system_message = """You are a feature engineering expert capable of creating valuable features based on descriptions.
Please provide a complete, executable Python function to implement the required feature engineering."""

        prompt = f"""
Please create new features based on the following description:

Feature description: {feature_description}

Dataset information:
- Shape: {df_info['shape']}
- Columns: {df_info['columns']}
- Data types: {df_info['dtypes']}

Please write a Python function named `create_custom_feature` that:
1. Accepts a pandas DataFrame as input
2. Creates new features based on the above description
3. Returns a DataFrame with new features

The code should:
- Handle potential missing values
- Include appropriate comments
- Follow Python best practices

Please return only Python code, no explanation needed.
"""
        
        response = self.llm_provider.call(prompt, system_message)
        implementation_code = self.code_parser.parse_code_from_response(response)
        
        # Generate unique ID
        suggestion_id = f"custom_{int(time.time())}"
        
        # Create suggestion object
        suggestion = {
            "suggestion_id": suggestion_id,
            "suggestion_type": "Custom",
            "description": feature_description,
            "rationale": "User-defined feature",
            "implementation": implementation_code,
            "affected_columns": [],
            "new_features": []
        }
        
        # Implement suggestion
        return self.implement_suggestion(df, suggestion)