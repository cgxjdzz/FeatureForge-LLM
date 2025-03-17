"""
特征实现器
"""
import pandas as pd
import time
from typing import Dict, Any, List, Tuple, Optional

from ..llm.base import LLMProvider
from ..executors.code_executor import CodeExecutor
from ..data.data_analyzer import DataAnalyzer
from ..parsers.code_parser import CodeParser

class FeatureImplementer:
    """
    实现特征工程建议
    """
    
    def __init__(self, llm_provider: LLMProvider, code_executor: CodeExecutor, verbose: bool = True):
        """
        初始化特征实现器
        
        参数:
            llm_provider: LLM提供者
            code_executor: 代码执行器
            verbose: 是否打印详细信息
        """
        self.llm_provider = llm_provider
        self.code_executor = code_executor
        self.verbose = verbose
        self.data_analyzer = DataAnalyzer(verbose=verbose)
        self.code_parser = CodeParser(verbose=verbose)
        self.implemented_features = {}
    
    def implement_suggestion(self, df: pd.DataFrame, suggestion: Dict[str, Any], 
                                keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        实现特定的特征工程建议
        
        参数:
            df: 输入数据帧
            suggestion: 特征建议字典
            keep_original: 是否保留原始特征
            
        返回:
            (更新的数据帧, 实现结果信息)
        """
        suggestion_id = suggestion.get("suggestion_id")
        if not suggestion_id:
            if self.verbose:
                print("❌ 建议缺少ID")
            return df, {"status": "error", "message": "建议缺少ID"}
            
        if self.verbose:
            print(f"🔧 正在实现建议: {suggestion.get('description', suggestion_id)}")
        
        # 如果没有实现代码，使用LLM生成代码
        implementation_code = suggestion.get("implementation")
        if not implementation_code or implementation_code == "# 需要手动实现":
            
            # 调用生成代码的方法
            implementation_code = self.generate_implementation_code(df, suggestion)
            
            # 更新建议中的实现代码
            suggestion["implementation"] = implementation_code
        
        # 清理实现代码
        implementation_code = self.code_parser.clean_implementation_code(implementation_code)
        
        # 确保代码是函数结构
        implementation_code = self.code_parser.ensure_function_structure(
            implementation_code, 
            f"feature_{suggestion_id.replace('-', '_').replace('.', '_')}"
        )
        
        # 实现建议
        result_df, impl_result = self.code_executor.execute(df, implementation_code, suggestion, keep_original)
        
        # 如果执行失败，尝试修复代码
        if impl_result["status"] == "error" and self.llm_provider:
            if self.verbose:
                print("🔄 执行失败，尝试修复代码...")
                
            # 获取数据帧信息用于修复代码
            df_info = self.data_analyzer.get_dataframe_info(df)
            
            # 修复代码
            fixed_code = self.code_executor.fix_code(
                implementation_code, 
                impl_result["error"], 
                df_info, 
                self.llm_provider
            )
            
            if fixed_code != implementation_code:
                if self.verbose:
                    print("🔧 使用修复后的代码重新尝试...")
                    
                # 使用修复后的代码重新尝试
                result_df, impl_result = self.code_executor.execute(df, fixed_code, suggestion, keep_original)
                
                # 更新建议中的实现代码
                if impl_result["status"] == "success":
                    suggestion["implementation"] = fixed_code
        
        # 记录实现结果
        self.implemented_features[suggestion_id] = impl_result
        
        return result_df, impl_result
    
    def generate_implementation_code(self, df: pd.DataFrame, suggestion: Dict[str, Any]) -> str:
        """
        为建议生成实现代码
        
        参数:
            df: 输入数据帧
            suggestion: 建议详情
            
        返回:
            实现代码
        """
        if not self.llm_provider:
            if self.verbose:
                print("⚠️ 缺少LLM提供者，无法生成代码")
            return "# 缺少LLM提供者，无法生成代码\ndef implement_feature(df):\n    return df"
        
        # 获取数据帧信息
        df_info = self.data_analyzer.get_dataframe_info(df)
        
        system_message = """你是一位特征工程专家，能够编写高质量的Python代码来实现特征工程。
    请提供完整可执行的Python函数，针对输入的DataFrame实现所需的特征工程。
    代码应该是健壮的，能够处理边缘情况，如缺失值和异常值。"""
        
        prompt = f"""
    请根据以下特征工程建议编写Python实现代码:

    建议描述: {suggestion.get('description', '')}
    建议理由: {suggestion.get('rationale', '')}
    建议类型: {suggestion.get('suggestion_type', '未知')}
    受影响的列: {suggestion.get('affected_columns', [])}
    预期新特征: {suggestion.get('new_features', [])}

    数据集信息:
    - 形状: {df_info['shape']}
    - 列: {df_info['columns']}
    - 数据类型: {df_info['dtypes']}
    - 缺失值: {df_info['missing_values']}
    - 唯一值数量: {df_info['unique_values']}

    请编写一个名为`implement_feature`的Python函数，该函数:
    1. 接受一个pandas DataFrame作为输入
    2. 实现上述特征工程建议
    3. 返回包含新特征的DataFrame

    代码应该:
    - 处理可能的缺失值
    - 包含适当的注释
    - 遵循Python最佳实践
    - 不使用外部数据源

    要点建议：
    - 对于特征转换，考虑使用pandas和numpy的内置方法
    - 对于特征交互，使用列组合或数学运算
    - 对于领域知识特征，提取有意义的信息

    请仅返回Python代码，不需要解释。
    """
        
        if self.verbose:
            print("🔬 正在生成特征实现代码...")
        
        response = self.llm_provider.call(prompt, system_message)
        code = self.code_parser.parse_code_from_response(response)
        
        if not code:
            # 如果没有提取到代码，使用简单的模板
            code = f"""def implement_feature(df):
        \"\"\"
        实现: {suggestion.get('description', '')}
        
        参数:
            df: 输入数据帧
            
        返回:
            包含新特征的数据帧
        \"\"\"
        # 创建数据帧副本以避免修改原始数据
        df_result = df.copy()
        
        # TODO: 实现特征工程逻辑
        # 可能的步骤：
        # 1. 处理缺失值
        # 2. 创建新特征
        # 3. 执行必要的转换
        
        return df_result
    """
        
        return code
    
    def implement_all_suggestions(self, df: pd.DataFrame, 
                                suggestions: List[Dict[str, Any]],
                                keep_original: bool = True) -> pd.DataFrame:
        """
        实现所有的特征工程建议
        
        参数:
            df: 输入数据帧
            suggestions: 建议列表
            keep_original: 是否保留原始特征
            
        返回:
            包含所有新特征的数据帧
        """
        if not suggestions:
            if self.verbose:
                print("⚠️ 没有可用的特征工程建议")
            return df
            
        result_df = df.copy()
        successful_count = 0
        
        for i, suggestion in enumerate(suggestions):
            suggestion_id = suggestion.get("suggestion_id")
            
            if not suggestion_id:
                continue
                
            if self.verbose:
                print(f"🔍 实现建议 {i+1}/{len(suggestions)}: {suggestion.get('description', '')}")
                
            try:
                result_df, impl_result = self.implement_suggestion(result_df, suggestion, keep_original)
                
                if impl_result["status"] == "success":
                    successful_count += 1
            except Exception as e:
                if self.verbose:
                    print(f"❌ 实现建议 {suggestion_id} 时出现未处理的错误: {e}")
        
        if self.verbose:
            print(f"✅ 成功实现 {successful_count}/{len(suggestions)} 个建议")
            print(f"🆕 新特征总数: {len(result_df.columns) - len(df.columns)}")
            
        return result_df
    
    def custom_feature_request(self, df: pd.DataFrame, feature_description: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        根据自定义描述创建特征
        
        参数:
            df: 输入数据帧
            feature_description: 特征描述
            
        返回:
            (更新的数据帧, 实现结果信息)
        """
        if not self.llm_provider:
            if self.verbose:
                print("⚠️ 缺少LLM提供者，无法处理自定义特征请求")
            return df, {"status": "error", "message": "缺少LLM提供者"}
            
        if self.verbose:
            print(f"🔍 正在处理自定义特征请求: {feature_description}")
            
        df_info = self.data_analyzer.get_dataframe_info(df)
        
        system_message = """你是一位特征工程专家，能够根据描述创建有价值的特征。
请提供完整可执行的Python函数，实现所需的特征工程。"""

        prompt = f"""
请根据以下描述创建新特征:

特征描述: {feature_description}

数据集信息:
- 形状: {df_info['shape']}
- 列: {df_info['columns']}
- 数据类型: {df_info['dtypes']}

请编写一个名为`create_custom_feature`的Python函数，该函数:
1. 接受一个pandas DataFrame作为输入
2. 根据上述描述创建新特征
3. 返回包含新特征的DataFrame

代码应该:
- 处理可能的缺失值
- 包含适当的注释
- 遵循Python最佳实践

请仅返回Python代码，不需要解释。
"""
        
        response = self.llm_provider.call(prompt, system_message)
        implementation_code = self.code_parser.parse_code_from_response(response)
        
        # 生成唯一ID
        suggestion_id = f"custom_{int(time.time())}"
        
        # 创建建议对象
        suggestion = {
            "suggestion_id": suggestion_id,
            "suggestion_type": "自定义",
            "description": feature_description,
            "rationale": "用户自定义特征",
            "implementation": implementation_code,
            "affected_columns": [],
            "new_features": []
        }
        
        # 实现建议
        return self.implement_suggestion(df, suggestion)