"""
Code Extraction and Cleaning
"""
import re
from typing import Optional

class CodeParser:
    """
    Parses code content from LLM responses
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize code parser
        
        Parameters:
            verbose: Whether to print detailed information
        """
        self.verbose = verbose
    
    def parse_code_from_response(self, response: str) -> str:
        """
        Extract Python code from LLM reply, supporting nested code blocks
        
        Parameters:
            response: Content of the LLM reply
            
        Returns:
            Extracted Python code
        """
        # Try to match the outermost Python code block
        code_pattern = r"```python(.*?)```"
        matches = re.findall(code_pattern, response, re.DOTALL)
        
        if matches:
            # Clean the extracted code
            extracted_code = matches[0].strip()
            
            # Check for and remove inner code block markers
            extracted_code = re.sub(r'```\w*\n', '', extracted_code)
            extracted_code = extracted_code.replace('\n```', '')
            
            return extracted_code
        
        # If no Markdown format, try to find possible Python code section
        if "def " in response and "return" in response:
            code_start = response.find("def ")
            
            # Find the end position of the code block
            code_lines = response[code_start:].split('\n')
            end_line = 0
            indent_level = 0
            in_function = False
            
            for i, line in enumerate(code_lines):
                if line.strip().startswith("def ") and line.strip().endswith(":"):
                    in_function = True
                    indent_level = len(line) - len(line.lstrip())
                    continue
                    
                if in_function:
                    if line.strip() and not line.startswith(" " * (indent_level + 4)):
                        # Reduced indentation, possibly end of function
                        if i > 2:  # At least include function definition and one line of function body
                            end_line = i
                            break
            
            if end_line > 0:
                extracted_code = "\n".join(code_lines[:end_line])
                return extracted_code
            else:
                return "\n".join(code_lines)
                
        return ""
    
    def clean_implementation_code(self, code: str) -> str:
        """
        Clean implementation code of Markdown markers and special characters
        
        Parameters:
            code: Original code
            
        Returns:
            Cleaned code
        """
        # Remove Markdown code block markers
        code = re.sub(r'```python\s*', '', code)
        code = re.sub(r'\s*```', '', code)
        
        # Remove possible quote escaping
        code = code.replace('\\"', '"')
        
        # Remove leading and trailing whitespace
        return code.strip()
    
    def extract_function_name(self, code: str) -> Optional[str]:
        """
        Extract function name from code
        
        Parameters:
            code: Code string
            
        Returns:
            Function name, or None if not found
        """
        match = re.search(r'def\s+(\w+)', code)
        if match:
            return match.group(1)
        return None
    
    def ensure_function_structure(self, code: str, function_name: Optional[str] = None) -> str:
        """
        Ensure code is in a function structure, wrapping it if necessary
        
        Parameters:
            code: Original code
            function_name: Specified function name, auto-generated if None
            
        Returns:
            Code ensured to be in function structure
        """
        if not code.strip():
            return ""
            
        # If already a function definition, return directly
        if code.strip().startswith("def "):
            return code
            
        # Generate function name
        if not function_name:
            function_name = f"process_feature_{hash(code) % 10000}"
            
        # Check if code already contains function call
        if "df = " in code or "return df" in code:
            # Already contains processing logic, just wrap into function
            wrapped_code = f"def {function_name}(df):\n" + "\n".join(
                f"    {line}" for line in code.split("\n")
            )
        else:
            # Might be just some operation steps, need to add DataFrame processing logic
            wrapped_code = f"""def {function_name}(df):
    df_result = df.copy()
    
    # Implement feature engineering logic
    {code.strip()}
    
    return df_result"""
        
        # Ensure there's a return statement
        if "return" not in wrapped_code:
            wrapped_code = wrapped_code.rstrip() + "\n    return df_result"
            
        return wrapped_code