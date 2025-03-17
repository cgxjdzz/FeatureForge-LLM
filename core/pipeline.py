"""
主要协调类，精简的LLMFeaturePipeline
"""
import time
import os
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

from ..core.utils import create_provider_instance, save_suggestions_to_file, load_suggestions_from_file, generate_report
from ..llm.base import LLMProvider
from ..parsers.json_parser import JsonParser
from ..parsers.code_parser import CodeParser
from ..data.data_analyzer import DataAnalyzer
from ..executors.code_executor import CodeExecutor
from ..data.feature_implementer import FeatureImplementer

class LLMFeaturePipeline:
    """
    LLM驱动的特征工程管道，实现询问建议-获得建议-实施代码-获得新特征的全流程
    """
    
    def __init__(self, llm_api_key: str, model: str = "gpt-4", verbose: bool = True, provider: str = "openai"):
        """
        初始化LLM特征工程管道
        
        参数:
            llm_api_key: LLM API密钥
            model: 使用的LLM模型
            verbose: 是否打印详细信息
            provider: LLM提供商，支持"openai"或"gemini"
        """
        self.verbose = verbose
        
        # 创建LLM提供者
        try:
            self.llm_provider = create_provider_instance(provider, llm_api_key, model, verbose)
        except Exception as e:
            if self.verbose:
                print(f"⚠️ 初始化LLM提供者失败: {e}")
            self.llm_provider = None
        
        # 创建核心组件
        self.json_parser = JsonParser(verbose=verbose)
        self.code_parser = CodeParser(verbose=verbose)
        self.data_analyzer = DataAnalyzer(verbose=verbose)
        self.code_executor = CodeExecutor(verbose=verbose)
        self.feature_implementer = FeatureImplementer(self.llm_provider, self.code_executor, verbose=verbose)
        
        # 初始化状态
        self.feature_suggestions = []
        self.implemented_features = {}
        self.execution_history = []
        self.start_time = time.time()
    
    def ask_for_feature_suggestions(self, df: pd.DataFrame, 
                                  task_description: str, 
                                  target_column: Optional[str] = None,
                                  dataset_background: Optional[str] = None,
                                  custom_prompt: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        询问LLM提供特征工程建议
        
        参数:
            df: 输入数据帧
            task_description: 任务描述
            target_column: 目标列名称
            dataset_background: 数据集背景信息，帮助模型理解数据
            custom_prompt: 自定义提示（如果需要）
            
        返回:
            特征工程建议列表
        """
        if not self.llm_provider:
            if self.verbose:
                print("❌ LLM提供者未初始化，无法请求建议")
            return []
            
        # 准备数据帧信息
        df_info = self.data_analyzer.get_dataframe_info(df)
        data_sample = df.head(3).to_dict() if df.shape[0] > 0 else {}
        
        system_message = """你是一位专业的特征工程专家，擅长发现数据中的模式和创建有价值的特征。
请提供具体、可执行的特征工程建议，每个建议都应包含详细的实现方式。以JSON格式回复。"""
        
        if custom_prompt:
            prompt = custom_prompt
        else:
            background_section = ""
            if dataset_background:
                background_section = f"""
数据集背景：
{dataset_background}
"""

            prompt = f"""
我有一个机器学习项目，需要你帮我进行特征工程。
            
任务描述：{task_description}

{"目标列：" + target_column if target_column else ""}
{background_section}
数据集信息：
- 形状：{df_info['shape']}
- 列：{df_info['columns']}
- 数据类型：{df_info['dtypes']}
- 缺失值：{df_info['missing_values']}
- 唯一值数量：{df_info['unique_values']}

分类特征分布：
{df_info.get('categorical_distributions', {})}

数值特征统计：
{df_info.get('numerical_statistics', {})}

数据样例：
{data_sample}

请提供5-10个有价值的特征工程建议，包括：
1. 特征转换（如二值化、标准化、独热编码等）
2. 特征交互（如特征组合、比率特征等）
3. 基于领域知识的特征（如时间特征、文本特征等）

对每个建议，请提供以下信息，以JSON数组格式返回：
[
  {{
    "suggestion_id": "唯一标识符",
    "suggestion_type": "转换|交互|领域知识|其他",
    "description": "详细的建议描述",
    "rationale": "为什么这个特征可能有价值",
    "implementation": "Python代码实现（可作为一个函数）",
    "affected_columns": ["受影响的列"],
    "new_features": ["新生成的特征名称"]
  }},
  ...
]
"""
        if self.verbose:
            print("🔍 正在询问LLM提供特征工程建议...")
            
        response = self.llm_provider.call(prompt, system_message)
        
        try:
            suggestions = self.json_parser.parse_json_from_response(response)
            if isinstance(suggestions, list):
                self.feature_suggestions = suggestions
                if self.verbose:
                    print(f"✅ 收到{len(suggestions)}个特征工程建议")
                return suggestions
            else:
                if self.verbose:
                    print("⚠️ LLM返回格式不正确，尝试提取建议")
                extracted_suggestions = self.json_parser._extract_suggestions_from_text(response)
                self.feature_suggestions = extracted_suggestions
                return extracted_suggestions
        except Exception as e:
            if self.verbose:
                print(f"❌ 解析建议失败: {e}")
            return []
    
    def implement_feature_suggestion(self, df: pd.DataFrame, suggestion_id: str, 
                                    keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        实现特定的特征工程建议
        
        参数:
            df: 输入数据帧
            suggestion_id: 建议ID
            keep_original: 是否保留原始特征
            
        返回:
            (更新的数据帧, 实现结果信息)
        """
        # 查找对应的建议
        suggestion = None
        for s in self.feature_suggestions:
            if s.get("suggestion_id") == suggestion_id:
                suggestion = s
                break
                
        if not suggestion:
            if self.verbose:
                print(f"❌ 找不到ID为{suggestion_id}的建议")
            return df, {"status": "error", "message": f"找不到ID为{suggestion_id}的建议"}
        
        # 实现建议
        result_df, impl_result = self.feature_implementer.implement_suggestion(df, suggestion, keep_original)
        
        # 记录结果
        self.implemented_features[suggestion_id] = impl_result
        self.execution_history.append(impl_result)
        
        return result_df, impl_result
    
    def implement_all_suggestions(self, df: pd.DataFrame, keep_original: bool = True) -> pd.DataFrame:
        """
        实现所有的特征工程建议
        
        参数:
            df: 输入数据帧
            keep_original: 是否保留原始特征
            
        返回:
            包含所有新特征的数据帧
        """
        return self.feature_implementer.implement_all_suggestions(df, self.feature_suggestions, keep_original)
    
    def custom_feature_request(self, df: pd.DataFrame, feature_description: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        根据自定义描述创建特征
        
        参数:
            df: 输入数据帧
            feature_description: 特征描述
            
        返回:
            (更新的数据帧, 实现结果信息)
        """
        result_df, impl_result = self.feature_implementer.custom_feature_request(df, feature_description)
        
        # 将自定义特征添加到建议列表中
        if impl_result["status"] == "success":
            suggestion_id = impl_result["suggestion_id"]
            suggestion = {
                "suggestion_id": suggestion_id,
                "suggestion_type": "自定义",
                "description": feature_description,
                "rationale": "用户自定义特征",
                "implementation": impl_result["code"],
                "affected_columns": [],
                "new_features": impl_result["new_features"]
            }
            
            self.feature_suggestions.append(suggestion)
            self.implemented_features[suggestion_id] = impl_result
            self.execution_history.append(impl_result)
        
        return result_df, impl_result
    
    def save_suggestions(self, file_path: str) -> bool:
        """
        保存特征建议到文件
        
        参数:
            file_path: 文件路径
            
        返回:
            是否保存成功
        """
        return save_suggestions_to_file(self.feature_suggestions, file_path)
    
    def load_suggestions(self, file_path: str) -> List[Dict[str, Any]]:
        """
        从文件加载特征建议
        
        参数:
            file_path: 文件路径
            
        返回:
            加载的建议列表
        """
        suggestions = load_suggestions_from_file(file_path)
        if suggestions:
            self.feature_suggestions = suggestions
        return suggestions
    
    def generate_report(self, original_df: pd.DataFrame, result_df: pd.DataFrame) -> Dict[str, Any]:
        """
        生成特征工程报告
        
        参数:
            original_df: 原始数据帧
            result_df: 结果数据帧
            
        返回:
            报告数据
        """
        return generate_report(
            self.implemented_features, 
            self.execution_history,
            original_df,
            result_df
        )
    
    def get_execution_time(self) -> float:
        """
        获取执行时间（秒）
        
        返回:
            执行时间
        """
        return time.time() - self.start_time
    
    def analyze_correlations(self, df: pd.DataFrame, target_column: Optional[str] = None) -> Dict[str, Any]:
        """
        分析数值特征之间的相关性
        
        参数:
            df: 输入数据帧
            target_column: 目标列名称
            
        返回:
            相关性分析结果
        """
        return self.data_analyzer.analyze_correlations(df, target_column)
    
    def detect_skewed_features(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        检测高度偏斜的数值特征
        
        参数:
            df: 输入数据帧
            
        返回:
            特征偏度字典
        """
        return self.data_analyzer.detect_skewed_features(df)
    
    def suggest_feature_transformations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        基于数据分析建议特征转换
        
        参数:
            df: 输入数据帧
            
        返回:
            特征转换建议列表
        """
        return self.data_analyzer.suggest_feature_transformations(df)
    
    def benchmark_feature_implementation(self, df: pd.DataFrame, 
                                       suggestion_id: str, 
                                       iterations: int = 3) -> Dict[str, Any]:
        """
        对特征实现进行性能基准测试
        
        参数:
            df: 输入数据帧
            suggestion_id: 建议ID
            iterations: 执行次数
            
        返回:
            基准测试结果
        """
        # 查找对应的建议
        suggestion = None
        for s in self.feature_suggestions:
            if s.get("suggestion_id") == suggestion_id:
                suggestion = s
                break
                
        if not suggestion:
            if self.verbose:
                print(f"❌ 找不到ID为{suggestion_id}的建议")
            return {"status": "error", "message": f"找不到ID为{suggestion_id}的建议"}
        
        # 提取实现代码
        implementation_code = suggestion.get("implementation", "")
        implementation_code = self.code_parser.clean_implementation_code(implementation_code)
        
        if not implementation_code or implementation_code == "# 需要手动实现":
            if self.verbose:
                print("❌ 建议中没有实现代码，无法进行基准测试")
            return {"status": "error", "message": "建议中没有实现代码"}
        
        # 确保代码是函数结构
        implementation_code = self.code_parser.ensure_function_structure(
            implementation_code, 
            f"feature_{suggestion_id.replace('-', '_').replace('.', '_')}"
        )
        
        # 执行基准测试
        return self.code_executor.benchmark_execution(df, implementation_code, iterations)
    
    def get_status_summary(self) -> Dict[str, Any]:
        """
        获取当前状态摘要
        
        返回:
            状态摘要字典
        """
        successful_features = [f for f in self.implemented_features.values() if f.get("status") == "success"]
        failed_features = [f for f in self.implemented_features.values() if f.get("status") != "success"]
        
        return {
            "total_suggestions": len(self.feature_suggestions),
            "implemented_count": len(self.implemented_features),
            "successful_count": len(successful_features),
            "failed_count": len(failed_features),
            "execution_time": self.get_execution_time(),
            "provider": self.llm_provider.get_provider_info() if self.llm_provider else None
        }