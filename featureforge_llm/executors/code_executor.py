"""
Code Execution Engine
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, List, Optional, Callable
import time

from ..executors.safety_utils import SafetyUtils
from ..llm.base import LLMProvider

class CodeExecutor:
    """
    Safely Execute Feature Engineering Code
    """
    
    def __init__(self, verbose: bool = True):
        """
       Initialize Code Executor
       
       Parameters:
           verbose: Whether to print detailed information
        """
        self.verbose = verbose
        self.safety_utils = SafetyUtils(verbose=verbose)
        self.execution_history = []
    
    def execute(self, df: pd.DataFrame, code: str, 
                suggestion: Optional[Dict[str, Any]] = None, 
                keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
       Execute code and return results
       
       Parameters:
           df: Input dataframe
           code: Code to execute
           suggestion: Suggestion information (optional)
           keep_original: Whether to keep original features
           
       Returns:
           (Updated dataframe, Execution result information)
        """
        suggestion_id = suggestion.get("suggestion_id", f"code_{int(time.time())}") if suggestion else f"code_{int(time.time())}"
        affected_columns = suggestion.get("affected_columns", []) if suggestion else []
        
# Safety check
        safety_result = self.safety_utils.check_code_safety(code)
        if not safety_result["is_safe"]:
            if self.verbose:
                print(f"⚠️ Code safety check failed: {safety_result['warnings']}")
            code = self.safety_utils.sanitize_code(code)
            if self.verbose:
                print("🔒 Code sanitized, continuing execution...")
        
        # Add safety checks
        code = self.safety_utils.add_safety_checks(code, affected_columns)
        
        try:
            # Create local namespace
            local_namespace = {"pd": pd, "np": np}
            
            # Execute code
            exec(code, globals(), local_namespace)
            
            # Get function name
            function_name = None
            for name, obj in local_namespace.items():
                if callable(obj) and name not in ["pd", "np"]:
                    function_name = name
                    break
            
            if not function_name:
                raise ValueError("Unable to find implementation function")
            
            # Call function
            start_time = time.time()
            result_df = local_namespace[function_name](df)
            execution_time = time.time() - start_time
            
            # Validate result
            if not isinstance(result_df, pd.DataFrame):
                raise TypeError(f"Implementation function returned {type(result_df).__name__}, not a DataFrame")
            
            # If specified not to keep original features, remove them
            if not keep_original and affected_columns:
                # Ensure all affected columns are transformed before removal
                safe_to_remove = all(col in df.columns for col in affected_columns)
                if safe_to_remove:
                    new_features = suggestion.get("new_features", []) if suggestion else []
                    for col in affected_columns:
                        if col in result_df.columns and col not in new_features:
                            if self.verbose:
                                print(f"🗑️ Removing original feature based on suggestion: {col}")
                            result_df = result_df.drop(col, axis=1)
            
            # Determine newly added features
            new_features = list(set(result_df.columns) - set(df.columns))
            # Record implementation results
            execution_result = {
                "suggestion_id": suggestion_id,
                "status": "success",
                "description": suggestion.get("description", "Code Execution") if suggestion else "Code Execution",
                "code": code,
                "function_name": function_name,
                "execution_time": execution_time,
                "new_features": new_features,
                "removed_features": [col for col in df.columns if col not in result_df.columns],
                "keep_original": keep_original,
                "error": None
            }
            
            self.execution_history.append(execution_result)
            
            if self.verbose:
                print(f"✅ Successfully executed code, time taken {execution_time:.4f} seconds")
                print(f"🆕 Added {len(new_features)} new features: {new_features}")
                if execution_result["removed_features"]:
                    print(f"🗑️ Removed {len(execution_result['removed_features'])} original features: {execution_result['removed_features']}")
            
            return result_df, execution_result
            
        except Exception as e:
            error_message = str(e)
            
            if self.verbose:
                print(f"❌ Error executing code: {error_message}")
            
            # Record failure
            execution_result = {
                "suggestion_id": suggestion_id,
                "status": "error",
                "description": suggestion.get("description", "Code Execution") if suggestion else "Code Execution",
                "code": code,
                "new_features": [],
                "removed_features": [],
                "keep_original": keep_original,
                "error": error_message
            }
            
            self.execution_history.append(execution_result)
            
            return df, execution_result
    
    def fix_code(self, code: str, error_message: str, df_info: Dict[str, Any], 
                llm_provider: Optional[LLMProvider] = None) -> str:
        """
        Fix errors in the code
        
        Parameters:
            code: Original code
            error_message: Error message
            df_info: DataFrame information
            llm_provider: LLM provider (if any)
            
        Returns:
            Fixed code
        """
        # 如果没有LLM提供者，尝试简单修复
        if not llm_provider:
            return self._simple_fix_code(code, error_message)
            
        system_message = """You are a Python expert capable of fixing code errors.
Analyze the error message and provide the corrected code. Return only the complete, fixed code without explanations."""
        
        prompt = f"""
The following code encountered an error during execution:

```python
{code}
```

Error message:
{error_message}

Dataset information:
- Shape: {df_info.get('shape', 'Unknown')}
- Columns: {df_info.get('columns', 'Unknown')}
- Data types: {df_info.get('dtypes', 'Unknown')}

Please fix the errors in the code. Return only the complete, fixed code without any explanations.
"""
        

        try:
            response = llm_provider.call(prompt, system_message)
            
            # Extract code
            from ..parsers.code_parser import CodeParser
            code_parser = CodeParser(verbose=self.verbose)
            fixed_code = code_parser.parse_code_from_response(response)
            
            if not fixed_code:
                # If no code was extracted, return original code
                if self.verbose:
                    print("⚠️ LLM did not return valid repair code")
                return code
                
            if self.verbose:
                print("✅ LLM has provided repair code")
                
            # Safety check
            safety_result = self.safety_utils.check_code_safety(fixed_code)
            if not safety_result["is_safe"]:
                if self.verbose:
                    print(f"⚠️ Fixed code safety check failed: {safety_result['warnings']}")
                fixed_code = self.safety_utils.sanitize_code(fixed_code)
                
            return fixed_code
            
        except Exception as e:
            if self.verbose:
                print(f"❌ Failed to request LLM to fix code: {e}")
            return self._simple_fix_code(code, error_message)
        
    def _simple_fix_code(self, code: str, error_message: str) -> str:
        """
        Simple code repair attempt without relying on LLM
        
        Parameters:
            code: Original code
            error_message: Error message
            
        Returns:
            Attempted fixed code
        """
        # Common error fixes
        fixed_code = code
        
        # 1. Fix undefined variable
        name_error_match = re.search(r"name '(\w+)' is not defined", error_message)
        if name_error_match:
            var_name = name_error_match.group(1)
            # Check if it's a common missing import
            if var_name == 'np':
                fixed_code = "import numpy as np\n" + fixed_code
            elif var_name == 'pd':
                fixed_code = "import pandas as pd\n" + fixed_code
                
        # 2. Fix non-existent column error
        key_error_match = re.search(r"KeyError: ['\"](.*)['\"]", error_message)
        if key_error_match:
            col_name = key_error_match.group(1)
            # Add column existence check
            function_def_end = fixed_code.find(":", fixed_code.find("def ")) + 1
            check_code = f"\n    # Check if column exists\n    if '{col_name}' not in df.columns:\n        print(f\"Warning: Column '{col_name}' does not exist\")\n        return df\n"
            fixed_code = fixed_code[:function_def_end] + check_code + fixed_code[function_def_end:]
            
        # 3. Fix type error
        type_error_match = re.search(r"TypeError: (.*)", error_message)
        if type_error_match:
            type_error = type_error_match.group(1)
            if "cannot convert" in type_error or "must be" in type_error:
                # Add type conversion
                fixed_code = fixed_code.replace("df[", "df[df.columns].astype('object')[")
                
        if self.verbose and fixed_code != code:
            print("🔧 Attempted simple code fix")
            
        return fixed_code

    def benchmark_execution(self, df: pd.DataFrame, code: str, 
                        iterations: int = 3) -> Dict[str, Any]:
        """
        Performance benchmarking for code execution
        
        Parameters:
            df: Input DataFrame
            code: Code to execute
            iterations: Number of executions
            
        Returns:
            Benchmarking results
        """
        times = []
        memory_usage_before = df.memory_usage(deep=True).sum()
        result_df = None
        execution_result = None
        
        for i in range(iterations):
            if self.verbose:
                print(f"🔍 Running benchmark iteration {i+1}/{iterations}...")
                
            start_time = time.time()
            result_df, execution_result = self.execute(df, code)
            end_time = time.time()
            
            if execution_result["status"] == "error":
                return {
                    "status": "error",
                    "error": execution_result["error"],
                    "message": "Benchmark aborted due to error"
                }
                
            times.append(end_time - start_time)
            
        # Calculate memory usage change
        memory_usage_after = result_df.memory_usage(deep=True).sum() if result_df is not None else 0
        memory_change = memory_usage_after - memory_usage_before
        
        benchmark_result = {
            "status": "success",
            "avg_execution_time": sum(times) / len(times),
            "min_execution_time": min(times),
            "max_execution_time": max(times),
            "memory_before_bytes": int(memory_usage_before),
            "memory_after_bytes": int(memory_usage_after),
            "memory_change_bytes": int(memory_change),
            "memory_change_percent": float(memory_change / memory_usage_before * 100) if memory_usage_before > 0 else 0,
            "new_features_count": len(execution_result["new_features"]) if execution_result else 0,
            "iterations": iterations
        }
        
        if self.verbose:
            print(f"📊 Benchmark completed:")
            print(f"   Average execution time: {benchmark_result['avg_execution_time']:.4f} seconds")
            print(f"   Memory change: {benchmark_result['memory_change_bytes'] / (1024*1024):.2f} MB ({benchmark_result['memory_change_percent']:.2f}%)")
            
        return benchmark_result

# To use the re module, it needs to be imported
import re