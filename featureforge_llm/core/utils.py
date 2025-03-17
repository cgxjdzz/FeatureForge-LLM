"""
Generic Utility Functions
"""
import json
import time
import os
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

def create_provider_instance(provider_name: str, api_key: str, model: str, verbose: bool = True):
    """
    Create LLM provider instance
    
    Parameters:
        provider_name: Provider name
        api_key: API key
        model: Model name
        verbose: Whether to print detailed information
        
    Returns:
        LLM provider instance
    """
    provider_name = provider_name.lower()
    
    if provider_name == "openai":
        from ..llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(verbose=verbose)
        provider.setup(api_key, model=model)
        return provider
    elif provider_name == "gemini":
        from ..llm.gemini_provider import GeminiProvider
        provider = GeminiProvider(verbose=verbose)
        provider.setup(api_key, model=model)
        return provider
    else:
        raise ValueError(f"Unsupported provider: {provider_name}, currently supporting 'openai' or 'gemini'")

def save_suggestions_to_file(suggestions: List[Dict[str, Any]], file_path: str) -> bool:
    """
    Save feature suggestions to file
    
    Parameters:
        suggestions: List of suggestions
        file_path: File path
        
    Returns:
        Whether saving was successful
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(suggestions, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ Failed to save suggestions to file: {e}")
        return False

def load_suggestions_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load feature suggestions from file
    
    Parameters:
        file_path: File path
        
    Returns:
        List of suggestions
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to load suggestions from file: {e}")
        return []

def save_implementation_results(results: Dict[str, Any], file_path: str) -> bool:
    """
    Save implementation results
    
    Parameters:
        results: Implementation results
        file_path: File path
        
    Returns:
        Whether saving was successful
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ Failed to save implementation results: {e}")
        return False

def generate_report(implemented_features: Dict[str, Any], 
                   execution_history: List[Dict[str, Any]],
                   original_df: pd.DataFrame,
                   result_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate feature engineering report
    
    Parameters:
        implemented_features: Implemented features
        execution_history: Execution history
        original_df: Original dataframe
        result_df: Result dataframe
        
    Returns:
        Report data
    """
    # Collect basic information
    successful_features = [f for f in implemented_features.values() if f.get("status") == "success"]
    failed_features = [f for f in implemented_features.values() if f.get("status") != "success"]
    
    # Calculate statistics
    added_columns = list(set(result_df.columns) - set(original_df.columns))
    removed_columns = list(set(original_df.columns) - set(result_df.columns))
    
    # Generate report
    report = {
        "timestamp": time.time(),
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "summary": {
            "total_suggestions": len(implemented_features),
            "successful_implementations": len(successful_features),
            "failed_implementations": len(failed_features),
            "original_columns": len(original_df.columns),
            "final_columns": len(result_df.columns),
            "added_columns": len(added_columns),
            "removed_columns": len(removed_columns)
        },
        "added_features": added_columns,
        "removed_features": removed_columns,
        "successful_features": [
            {
                "id": f.get("suggestion_id"),
                "description": f.get("description"),
                "new_features": f.get("new_features")
            } for f in successful_features
        ],
        "failed_features": [
            {
                "id": f.get("suggestion_id"),
                "description": f.get("description"),
                "error": f.get("error")
            } for f in failed_features
        ],
        "execution_history": execution_history
    }
    
    return report

def format_timedelta(seconds: float) -> str:
    """
    Format time difference
    
    Parameters:
        seconds: Number of seconds
        
    Returns:
        Formatted time string
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{int(hours)} hours {int(minutes)} minutes {seconds:.2f} seconds"
    elif minutes > 0:
        return f"{int(minutes)} minutes {seconds:.2f} seconds"
    else:
        return f"{seconds:.2f} seconds"