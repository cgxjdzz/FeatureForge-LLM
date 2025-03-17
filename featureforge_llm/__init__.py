"""
LLM Feature Engineering Toolkit - Automated Feature Engineering Based on Large Language Models
This package provides tools for automated feature engineering using LLMs, including feature suggestion, code generation, and execution.
"""

# 主要导出
from .core.pipeline import LLMFeaturePipeline

# 版本信息
__version__ = "1.0.0"
__author__ = "Feifan Zhang"
__license__ = "MIT"
__description__ = "Automated Feature Engineering Toolkit Based on Large Language Models"

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