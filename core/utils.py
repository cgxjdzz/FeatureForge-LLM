"""
通用工具函数
"""
import json
import time
import os
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd

def create_provider_instance(provider_name: str, api_key: str, model: str, verbose: bool = True):
    """
    创建LLM提供者实例
    
    参数:
        provider_name: 提供者名称
        api_key: API密钥
        model: 模型名称
        verbose: 是否打印详细信息
        
    返回:
        LLM提供者实例
    """
    provider_name = provider_name.lower()
    
    if provider_name == "openai":
        from ..llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(verbose=verbose)
        provider.setup(api_key, model=model)
        return provider
    elif provider_name == "gemini":
        from ..llm.gemini_provider import GeminiProvider
        provider = GeminiProvider(verbose=verbose)
        provider.setup(api_key, model=model)
        return provider
    else:
        raise ValueError(f"不支持的提供商: {provider_name}，目前支持 'openai' 或 'gemini'")

def save_suggestions_to_file(suggestions: List[Dict[str, Any]], file_path: str) -> bool:
    """
    将特征建议保存到文件
    
    参数:
        suggestions: 建议列表
        file_path: 文件路径
        
    返回:
        是否保存成功
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(suggestions, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ 保存建议到文件失败: {e}")
        return False

def load_suggestions_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    从文件加载特征建议
    
    参数:
        file_path: 文件路径
        
    返回:
        建议列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ 从文件加载建议失败: {e}")
        return []

def save_implementation_results(results: Dict[str, Any], file_path: str) -> bool:
    """
    保存实现结果
    
    参数:
        results: 实现结果
        file_path: 文件路径
        
    返回:
        是否保存成功
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ 保存实现结果失败: {e}")
        return False

def generate_report(implemented_features: Dict[str, Any], 
                   execution_history: List[Dict[str, Any]],
                   original_df: pd.DataFrame,
                   result_df: pd.DataFrame) -> Dict[str, Any]:
    """
    生成特征工程报告
    
    参数:
        implemented_features: 已实现的特征
        execution_history: 执行历史
        original_df: 原始数据帧
        result_df: 结果数据帧
        
    返回:
        报告数据
    """
    # 收集基本信息
    successful_features = [f for f in implemented_features.values() if f.get("status") == "success"]
    failed_features = [f for f in implemented_features.values() if f.get("status") != "success"]
    
    # 计算统计信息
    added_columns = list(set(result_df.columns) - set(original_df.columns))
    removed_columns = list(set(original_df.columns) - set(result_df.columns))
    
    # 生成报告
    report = {
        "timestamp": time.time(),
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "summary": {
            "total_suggestions": len(implemented_features),
            "successful_implementations": len(successful_features),
            "failed_implementations": len(failed_features),
            "original_columns": len(original_df.columns),
            "final_columns": len(result_df.columns),
            "added_columns": len(added_columns),
            "removed_columns": len(removed_columns)
        },
        "added_features": added_columns,
        "removed_features": removed_columns,
        "successful_features": [
            {
                "id": f.get("suggestion_id"),
                "description": f.get("description"),
                "new_features": f.get("new_features")
            } for f in successful_features
        ],
        "failed_features": [
            {
                "id": f.get("suggestion_id"),
                "description": f.get("description"),
                "error": f.get("error")
            } for f in failed_features
        ],
        "execution_history": execution_history
    }
    
    return report

def format_timedelta(seconds: float) -> str:
    """
    格式化时间差
    
    参数:
        seconds: 秒数
        
    返回:
        格式化的时间字符串
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{int(hours)}小时 {int(minutes)}分钟 {seconds:.2f}秒"
    elif minutes > 0:
        return f"{int(minutes)}分钟 {seconds:.2f}秒"
    else:
        return f"{seconds:.2f}秒"