"""
Gemini LLM Provider Implementation
"""
import time
from typing import Optional, Dict, Any

from ..llm.base import LLMProvider

class GeminiProvider(LLMProvider):
    """
    LLM Provider implementation for Google Gemini API
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize Gemini provider
        
        Parameters:
            verbose: Whether to print detailed information
        """
        self.api_key = None
        self.client = None
        self._model = None
        self.verbose = verbose
    
    def setup(self, api_key: str, **kwargs) -> None:
        """
        Set up Gemini API client
        
        Parameters:
            api_key: Gemini API key
            **kwargs: Additional parameters, such as model
        """
        try:
            from google import genai
            self.api_key = api_key
            self.client = genai.Client(api_key=api_key)
            self._model = kwargs.get('model', 'gemini-pro')
            
            if self.verbose:
                print("✅ Gemini API client setup successful")
        except ImportError:
            raise ImportError("Please install google-generativeai library: pip install google-generativeai")
    
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        Call Gemini API to get a response
        
        Parameters:
            prompt: User prompt
            system_message: System prompt
            
        Returns:
            Content of the model's reply
        """
        if not self.client:
            raise ValueError("Please call the setup method to set up the API client first")
        
        try:
            # Construct prompt content
            contents = prompt
            
            if system_message:
                from google.genai import types
                response = self.client.models.generate_content(
                    model=self._model,
                    contents=contents,    
                    config=types.GenerateContentConfig(
                        system_instruction=system_message)
                )
            else:
                response = self.client.models.generate_content(
                    model=self._model,
                    contents=contents 
                )
            
            return response.text
            
        except Exception as e:
            if self.verbose:
                print(f"❌ Gemini API call failed: {e}")
            time.sleep(2)  # Wait a bit before retrying
            
            try:
                # Simplify request and try again
                response = self.client.models.generate_content(
                    model=self._model, 
                    contents=prompt
                )
                return response.text
            except Exception as e2:
                print(f"❌ Gemini API call failed again: {e2}")
                return "Gemini API call failed. Please check network connection and API key."
    
    @property
    def model_name(self) -> str:
        """
        Get the name of the currently used model
        
        Returns:
            Model name
        """
        return self._model
    
    def get_provider_info(self) -> Dict[str, Any]:
        """
        Get provider information
        
        Returns:
            Dictionary containing provider information
        """
        return {
            "provider": "gemini",
            "model": self._model,
            "api_version": "unknown"
        }