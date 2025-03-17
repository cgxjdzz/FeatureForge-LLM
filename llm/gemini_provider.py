"""
Gemini LLM提供者实现
"""
import time
from typing import Optional, Dict, Any

from ..llm.base import LLMProvider

class GeminiProvider(LLMProvider):
    """
    Google Gemini API的LLM提供者实现
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化Gemini提供者
        
        参数:
            verbose: 是否打印详细信息
        """
        self.api_key = None
        self.client = None
        self._model = None
        self.verbose = verbose
    
    def setup(self, api_key: str, **kwargs) -> None:
        """
        设置Gemini API客户端
        
        参数:
            api_key: Gemini API密钥
            **kwargs: 额外参数，如model等
        """
        try:
            from google import genai
            self.api_key = api_key
            self.client = genai.Client(api_key=api_key)
            self._model = kwargs.get('model', 'gemini-pro')
            
            if self.verbose:
                print("✅ Gemini API客户端设置成功")
        except ImportError:
            raise ImportError("请安装google-generativeai库: pip install google-generativeai")
    
    def call(self, prompt: str, system_message: Optional[str] = None) -> str:
        """
        调用Gemini API获取回复
        
        参数:
            prompt: 用户提示
            system_message: 系统提示
            
        返回:
            模型回复的内容
        """
        if not self.client:
            raise ValueError("请先调用setup方法设置API客户端")
        
        try:
            # 构建提示内容
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
                print(f"❌ Gemini API调用失败: {e}")
            time.sleep(2)  # 等待一下再重试
            
            try:
                # 简化请求再尝试
                response = self.client.models.generate_content(
                    model=self._model, 
                    contents=prompt
                )
                return response.text
            except Exception as e2:
                print(f"❌ Gemini API再次调用失败: {e2}")
                return "Gemini API调用失败，请检查网络连接和API密钥。"
    
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
            "provider": "gemini",
            "model": self._model,
            "api_version": "unknown"
        }