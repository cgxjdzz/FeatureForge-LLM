"""
LLM特征工程工具包 - 基于大语言模型的自动化特征工程

此包提供了通过LLM实现自动化特征工程的工具，包括特征建议、代码生成和执行。
"""

# 主要导出
from .core.pipeline import LLMFeaturePipeline

# 版本信息
__version__ = "0.1.0"
__author__ = "Your Name"
__license__ = "MIT"
__description__ = "基于大语言模型的自动化特征工程工具包"

# 导出子模块API
from .llm.base import LLMProvider
from .llm.openai_provider import OpenAIProvider
from .llm.gemini_provider import GeminiProvider
from .data.data_analyzer import DataAnalyzer
from .data.feature_implementer import FeatureImplementer
from .executors.code_executor import CodeExecutor
from .parsers.code_parser import CodeParser
from .parsers.json_parser import JsonParser

# 方便用户导入的别名
from .core.utils import (
    create_provider_instance,
    save_suggestions_to_file,
    load_suggestions_from_file,
    generate_report
)

__all__ = [
    'LLMFeaturePipeline',
    'LLMProvider',
    'OpenAIProvider',
    'GeminiProvider',
    'DataAnalyzer',
    'FeatureImplementer',
    'CodeExecutor',
    'CodeParser',
    'JsonParser',
    'create_provider_instance',
    'save_suggestions_to_file',
    'load_suggestions_from_file',
    'generate_report'
]