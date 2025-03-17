"""
JSON Parsing Related Functionality
"""
import re
import json
from typing import Dict, List, Any, Union

class JsonParser:
    """
    Parses JSON content from LLM responses
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize JSON parser
        
        Parameters:
            verbose: Whether to print detailed information
        """
        self.verbose = verbose
    
    def parse_json_from_response(self, response: str) -> Union[Dict, List]:
        """
        Extract JSON content from LLM reply
        
        Parameters:
            response: Content of the LLM reply
            
        Returns:
            Extracted JSON content (dictionary or list)
        """
        if self.verbose:
            print("\n==== LLM Original Response ====")
            print(response)
            print("=====================\n")
        
        # First try to directly parse JSON part in the full response
        try:
            # Find the outermost JSON structure
            json_pattern = r"```json(.*?)```"
            matches = re.findall(json_pattern, response, re.DOTALL)
            
            if matches:
                # Extract and clean JSON string
                json_str = matches[0].strip()
                
                # Replace embedded code blocks
                code_pattern = r"```python(.*?)```"
                json_str = re.sub(code_pattern, lambda m: json.dumps(m.group(1)), json_str)
                
                # Standardize line breaks and spaces
                json_str = re.sub(r'[\r\n\t]+', ' ', json_str)
                json_str = re.sub(r'\s{2,}', ' ', json_str)
                
                # Try to parse
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    # Try a more strict parsing method
                    return self._extract_json_array_or_object(json_str)
                    
            # Try to extract JSON array or object from entire text
            return self._extract_json_array_or_object(response)
        
        except Exception as e:
            if self.verbose:
                print(f"⚠️ JSON parsing failed: {e}")
            return self._fallback_parse_suggestions(response)
    
    def _extract_json_array_or_object(self, text: str) -> Union[Dict, List]:
        """
        Extract JSON array or object from text
        
        Parameters:
            text: Input text
            
        Returns:
            Extracted JSON content
        """
        # Find JSON array pattern: [...]
        array_match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
        if array_match:
            try:
                return json.loads(array_match.group(0))
            except json.JSONDecodeError:
                pass
        
        # Find JSON object pattern: {...}
        object_match = re.search(r'\{\s*".*"\s*:.*\}', text, re.DOTALL)
        if object_match:
            try:
                return json.loads(object_match.group(0))
            except json.JSONDecodeError:
                pass
        
        # If all fail, return empty result
        return {}
    
    def _fallback_parse_suggestions(self, text: str) -> List[Dict]:
        """
        As a last resort, extract suggestions from text
        
        Parameters:
            text: Input text
            
        Returns:
            List of extracted suggestions
        """
        suggestions = []
        
        # Use regex to extract individual suggestions
        suggestion_pattern = r'"suggestion_id":\s*"([^"]+)".*?"description":\s*"([^"]+)".*?"rationale":\s*"([^"]+)"'
        matches = re.findall(suggestion_pattern, text, re.DOTALL)
        
        for i, match in enumerate(matches):
            suggestion_id, description, rationale = match
            
            # Extract implementation for each match
            implementation_pattern = r'"implementation":\s*"(.*?)"'
            impl_match = re.search(implementation_pattern, text[text.find(suggestion_id):], re.DOTALL)
            implementation = impl_match.group(1) if impl_match else ""
            
            # Extract affected columns
            affected_cols_pattern = r'"affected_columns":\s*\[(.*?)\]'
            cols_match = re.search(affected_cols_pattern, text[text.find(suggestion_id):], re.DOTALL)
            affected_columns = self._parse_string_array(cols_match.group(1)) if cols_match else []
            
            # Extract new features
            new_features_pattern = r'"new_features":\s*\[(.*?)\]'
            features_match = re.search(new_features_pattern, text[text.find(suggestion_id):], re.DOTALL)
            new_features = self._parse_string_array(features_match.group(1)) if features_match else []
            
            suggestion = {
                "suggestion_id": suggestion_id,
                "suggestion_type": self._guess_suggestion_type(description),
                "description": description,
                "rationale": rationale,
                "implementation": implementation,
                "affected_columns": affected_columns,
                "new_features": new_features
            }
            
            suggestions.append(suggestion)
        
        if not suggestions:
            # If the above method fails, fallback to the original extraction method
            suggestions = self._extract_suggestions_from_text(text)
        
        return suggestions
    
    def _parse_string_array(self, array_str: str) -> List[str]:
        """
        Parse string array
        
        Parameters:
            array_str: Array string
            
        Returns:
            Parsed list of strings
        """
        values = []
        for item in array_str.split(','):
            item = item.strip().strip('"\'')
            if item:
                values.append(item)
        return values
    
    def _extract_suggestions_from_text(self, text: str) -> List[Dict]:
        """
        Extract suggestions from text reply
        
        Parameters:
            text: LLM reply text
            
        Returns:
            List of extracted suggestions
        """
        if self.verbose:
            print("\n==== Attempting to extract suggestions from text ====")
            print(f"Text length: {len(text)} characters")
            print("Preview of first 500 characters:")
            print(text[:500] + "..." if len(text) > 500 else text)
            print("============================\n")
            
        suggestions = []
        
        # Find possible suggestion sections
        suggestion_blocks = re.split(r'\n\d+[\.\)]\s+', text)
        
        if self.verbose:
            print(f"Found {len(suggestion_blocks) - 1} potential suggestion blocks")
        
        for i, block in enumerate(suggestion_blocks[1:], 1):  # Skip first block which might be an introduction
            if self.verbose and i <= 3:  # Only show first 3 blocks as example
                print(f"\n== Suggestion Block #{i} Preview ==")
                preview = block[:200] + "..." if len(block) > 200 else block
                print(preview)
                print("===================")
                
            lines = block.strip().split('\n')
            
            if not lines:
                continue
                
            # Extract suggestion information
            title = lines[0].strip()
            description = "\n".join(lines[1:])
            
            # Extract code part (assumes CodeParser is available, actual implementation needs import)
            from ..parsers.code_parser import CodeParser
            code_parser = CodeParser(verbose=self.verbose)
            code = code_parser.parse_code_from_response(block)
            
            if self.verbose and code:
                print(f"Extracted code from suggestion #{i}:")
                print(code[:200] + "..." if len(code) > 200 else code)
            
            suggestion = {
                "suggestion_id": f"auto_extracted_{i}",
                "suggestion_type": self._guess_suggestion_type(title),
                "description": title,
                "rationale": description,
                "implementation": code if code else "# Needs manual implementation",
                "affected_columns": [],
                "new_features": []
            }
            
            suggestions.append(suggestion)
        
        if self.verbose:
            print(f"📝 Extracted {len(suggestions)} suggestions from text")
            
        return suggestions
    
    def _guess_suggestion_type(self, text: str) -> str:
        """
        Guess suggestion type based on text
        
        Parameters:
            text: Suggestion text
            
        Returns:
            Guessed suggestion type (Transformation|Interaction|Domain Knowledge|Other)
        """
        text = text.lower()
        
        if any(word in text for word in ["交互", "组合", "乘积", "比率", "interaction", "multiply", "product", "combination"]):
            return "Interaction"
        elif any(word in text for word in ["标准化", "归一化", "编码", "二值化", "transform", "encoding", "normalize", "standardize", "binarize"]):
            return "Transformation"
        elif any(word in text for word in ["领域", "知识", "domain", "knowledge", "expert", "context", "specific"]):
            return "Domain Knowledge"
        else:
            return "Other"