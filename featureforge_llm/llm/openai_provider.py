"""
OpenAI LLM Provider Implementation
"""
import time
from typing import Optional, Dict, Any

from ..llm.base import LLMProvider

class OpenAIProvider(LLMProvider):
    """
    LLM Provider implementation for OpenAI API
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize OpenAI provider
        
        Parameters:
            verbose: Whether to print detailed information
        """
        self.api_key = None
        self.client = None
        self._model = None
        self.verbose = verbose
    
    def setup(self, api_key: str, **kwargs) -> None:
        """
        Set up OpenAI API client
        
        Parameters:
            api_key: OpenAI API key
            **kwargs: Additional parameters, such as model
        """
        try:
            import openai
            self.api_key = api_key
            openai.api_key = api_key
            self.client = openai
            self._model = kwargs.get('model', 'gpt-4')
            
            if self.verbose:
                print("✅ OpenAI API client setup successful")
        except ImportError:
            raise ImportError("Please install openai library: pip install openai")
    
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        Call OpenAI API to get a response
        
        Parameters:
            prompt: User prompt
            system_message: System prompt
            
        Returns:
            Content of the model's reply
        """
        if not self.client:
            raise ValueError("Please call the setup method to set up the API client first")
        
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        
        messages.append({"role": "user", "content": prompt})
        
        try:
            response = self.client.ChatCompletion.create(
                model=self._model,
                messages=messages
            )
            return response.choices[0].message.content
        except Exception as e:
            if self.verbose:
                print(f"❌ OpenAI API call failed: {e}")
            time.sleep(2)  # Wait a bit before retrying
            try:
                response = self.client.ChatCompletion.create(
                    model=self._model,
                    messages=messages
                )
                return response.choices[0].message.content
            except Exception as e2:
                print(f"❌ OpenAI API call failed again: {e2}")
                return "API call failed. Please check network connection and API key."
    
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
            "provider": "openai",
            "model": self._model,
            "api_version": self.client.__version__ if hasattr(self.client, '__version__') else "unknown"
        }