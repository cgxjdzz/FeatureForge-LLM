"""
Abstract base class for LLM providers
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class LLMProvider(ABC):
    """
    Abstract base class for Large Language Model service providers, 
    defining a common interface for LLM services
    """
    
    @abstractmethod
    def setup(self, api_key: str, **kwargs) -> None:
        """
        Set up the API client
        
        Parameters:
            api_key: API key
            **kwargs: Additional configuration parameters
        """
        pass
        
    @abstractmethod
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        Call LLM API to get a response
        
        Parameters:
            prompt: User prompt
            system_message: System prompt
            
        Returns:
            Content of the LLM response
        """
        pass
    
    @property
    @abstractmethod
    def model_name(self) -> str:
        """
        Get the name of the currently used model
        
        Returns:
            Model name
        """
        pass
    
    @abstractmethod
    def get_provider_info(self) -> Dict[str, Any]:
        """
        Get provider information
        
        Returns:
            Dictionary containing provider information
        """
        pass