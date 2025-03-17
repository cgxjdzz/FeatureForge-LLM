"""
LLM提供者的抽象基类
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class LLMProvider(ABC):
    """
    大语言模型服务提供者的抽象基类，定义了LLM服务的通用接口
    """
    
    @abstractmethod
    def setup(self, api_key: str, **kwargs) -> None:
        """
        设置API客户端
        
        参数:
            api_key: API密钥
            **kwargs: 额外的配置参数
        """
        pass
        
    @abstractmethod
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        调用LLM API获取回复
        
        参数:
            prompt: 用户提示
            system_message: 系统提示
            
        返回:
            LLM回复的内容
        """
        pass
    
    @property
    @abstractmethod
    def model_name(self) -> str:
        """
        获取当前使用的模型名称
        
        返回:
            模型名称
        """
        pass
    
    @abstractmethod
    def get_provider_info(self) -> Dict[str, Any]:
        """
        获取提供者信息
        
        返回:
            包含提供者信息的字典
        """
        pass