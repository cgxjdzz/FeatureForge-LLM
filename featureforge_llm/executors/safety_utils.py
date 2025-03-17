"""
Code Safety Check Tool
"""
import re
from typing import List, Dict, Any

class SafetyUtils:
    """
    Provides tools for code safety checking
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize safety tools
        
        Parameters:
            verbose: Whether to print detailed information
        """
        self.verbose = verbose
        # Define patterns for dangerous operations
        self.dangerous_patterns = [
            r'import\s+os', 
            r'import\s+sys',
            r'import\s+subprocess',
            r'__import__',
            r'eval\s*\(',
            r'exec\s*\(',
            r'os\.(system|popen|execv|spawn)',
            r'subprocess\.(Popen|call|run)',
            r'open\s*\(.+,\s*[\'"]w',  
            r'shutil\.(rmtree|remove)',
            r'glob\.',
            r'(rm|del|remove)\s+',
            r'request\.(get|post)'
        ]
    
    def add_safety_checks(self, code: str, affected_columns: List[str]) -> str:
        """
        Add safety checks to ensure correct code execution
        
        Parameters:
            code: Original code
            affected_columns: Columns to be affected
            
        Returns:
            Code with added safety checks
        """
        # Get function name
        func_match = re.search(r'def\s+(\w+)', code)
        if not func_match:
            if self.verbose:
                print("⚠️ Unable to extract function name from code")
            return code
            
        # Add column existence checks
        column_checks = []
        for col in affected_columns:
            if col:
                column_checks.append(
                    f'    # Check if column "{col}" exists\n'
                    f'    if "{col}" not in df.columns:\n'
                    f'        print(f"Warning: Column \\"{col}\\" does not exist, skipping column processing")\n'
                    f'        return df'
                )
        
        # If there are columns to check, insert check code
        if column_checks:
            # Find the end of function definition
            func_def_end = code.find(":", code.find("def ")) + 1
            
            # Insert safety check code
            safety_code = "\n" + "\n".join(column_checks) + "\n    \n    # Create a copy to avoid modifying original data\n    df = df.copy()\n"
            code = code[:func_def_end] + safety_code + code[func_def_end:]
        
        return code
    
    def check_code_safety(self, code: str) -> Dict[str, Any]:
        """
        Check if the code has security risks
        
        Parameters:
            code: Code to be checked
            
        Returns:
            Safety check results
        """
        # Initialize check results
        result = {
            "is_safe": True,
            "warnings": [],
            "details": {}
        }
        
        # Check dangerous operations
        for pattern in self.dangerous_patterns:
            matches = re.findall(pattern, code)
            if matches:
                result["is_safe"] = False
                warning = f"Code contains potentially dangerous operations: {pattern}"
                result["warnings"].append(warning)
                result["details"][pattern] = matches
                
                if self.verbose:
                    print(f"⚠️ {warning}")
        
        # Check other potential issues
        # 1. Recursive call check
        func_name_match = re.search(r'def\s+(\w+)', code)
        if func_name_match:
            func_name = func_name_match.group(1)
            if re.search(fr'{func_name}\s*\(', code[code.find("def ")+len(f"def {func_name}"):]): 
                result["warnings"].append(f"Code may contain recursive call: {func_name}")
                
        # 2. Check infinite loop risks
        for loop_keyword in ["while", "for"]:
            loop_matches = re.findall(fr'{loop_keyword}\s+.*:', code)
            for loop_match in loop_matches:
                loop_body_start = code.find(loop_match) + len(loop_match)
                # Check if there's a break statement in the loop body
                next_break = code.find("break", loop_body_start)
                if next_break == -1 or (code.find("def ", loop_body_start) != -1 and code.find("def ", loop_body_start) < next_break):
                    if loop_keyword == "while" and "True" in loop_match:
                        result["warnings"].append("Code may contain an infinite loop without a break condition")
                        
        return result
    
def sanitize_code(self, code: str) -> str:
        """
        Clean potential dangerous parts from the code
        
        Parameters:
            code: Original code
            
        Returns:
            Sanitized code
        """
        # Remove dangerous imports
        for pattern in [r'import\s+os.*\n', r'import\s+sys.*\n', r'import\s+subprocess.*\n']:
            code = re.sub(pattern, '# Import removed for security reasons\n', code)
        
        # Replace dangerous function calls
        code = re.sub(r'eval\s*\(', '# eval(', code)
        code = re.sub(r'exec\s*\(', '# exec(', code)
        code = re.sub(r'os\.(system|popen|execv|spawn)', '# os.\\1', code)
        code = re.sub(r'subprocess\.(Popen|call|run)', '# subprocess.\\1', code)
        
        # Add safety comment
        safe_code = "# Note: This code has been safety checked and sanitized\n" + code
        
        return safe_code