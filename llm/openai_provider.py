"""
OpenAI LLM提供者实现
"""
import time
from typing import Optional, Dict, Any

from ..llm.base import LLMProvider

class OpenAIProvider(LLMProvider):
    """
    OpenAI API的LLM提供者实现
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化OpenAI提供者
        
        参数:
            verbose: 是否打印详细信息
        """
        self.api_key = None
        self.client = None
        self._model = None
        self.verbose = verbose
    
    def setup(self, api_key: str, **kwargs) -> None:
        """
        设置OpenAI API客户端
        
        参数:
            api_key: OpenAI API密钥
            **kwargs: 额外参数，如model等
        """
        try:
            import openai
            self.api_key = api_key
            openai.api_key = api_key
            self.client = openai
            self._model = kwargs.get('model', 'gpt-4')
            
            if self.verbose:
                print("✅ OpenAI API客户端设置成功")
        except ImportError:
            raise ImportError("请安装openai库: pip install openai")
    
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        调用OpenAI API获取回复
        
        参数:
            prompt: 用户提示
            system_message: 系统提示
            
        返回:
            模型回复的内容
        """
        if not self.client:
            raise ValueError("请先调用setup方法设置API客户端")
        
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
                print(f"❌ OpenAI API调用失败: {e}")
            time.sleep(2)  # 等待一下再重试
            try:
                response = self.client.ChatCompletion.create(
                    model=self._model,
                    messages=messages
                )
                return response.choices[0].message.content
            except Exception as e2:
                print(f"❌ OpenAI API再次调用失败: {e2}")
                return "API调用失败，请检查网络连接和API密钥。"
    
    @property
    def model_name(self) -> str:
        """
        获取当前使用的模型名称
        
        返回:
            模型名称
        """
        return self._model
    
    def get_provider_info(self) -> Dict[str, Any]:
        """
        获取提供者信息
        
        返回:
            包含提供者信息的字典
        """
        return {
            "provider": "openai",
            "model": self._model,
            "api_version": self.client.__version__ if hasattr(self.client, '__version__') else "unknown"
        }