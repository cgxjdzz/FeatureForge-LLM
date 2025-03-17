"""
代码执行引擎
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, List, Optional, Callable
import time

from ..executors.safety_utils import SafetyUtils
from ..llm.base import LLMProvider

class CodeExecutor:
    """
    安全执行特征工程代码
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化代码执行器
        
        参数:
            verbose: 是否打印详细信息
        """
        self.verbose = verbose
        self.safety_utils = SafetyUtils(verbose=verbose)
        self.execution_history = []
    
    def execute(self, df: pd.DataFrame, code: str, 
                suggestion: Optional[Dict[str, Any]] = None, 
                keep_original: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        执行代码并返回结果
        
        参数:
            df: 输入数据帧
            code: 要执行的代码
            suggestion: 建议信息（可选）
            keep_original: 是否保留原始特征
            
        返回:
            (更新的数据帧, 执行结果信息)
        """
        suggestion_id = suggestion.get("suggestion_id", f"code_{int(time.time())}") if suggestion else f"code_{int(time.time())}"
        affected_columns = suggestion.get("affected_columns", []) if suggestion else []
        
        # 安全检查
        safety_result = self.safety_utils.check_code_safety(code)
        if not safety_result["is_safe"]:
            if self.verbose:
                print(f"⚠️ 代码安全检查未通过: {safety_result['warnings']}")
            code = self.safety_utils.sanitize_code(code)
            if self.verbose:
                print("🔒 代码已清理，继续执行...")
        
        # 添加安全检查
        code = self.safety_utils.add_safety_checks(code, affected_columns)
        
        try:
            # 创建本地命名空间
            local_namespace = {"pd": pd, "np": np}
            
            # 执行代码
            exec(code, globals(), local_namespace)
            
            # 获取函数名
            function_name = None
            for name, obj in local_namespace.items():
                if callable(obj) and name not in ["pd", "np"]:
                    function_name = name
                    break
            
            if not function_name:
                raise ValueError("无法找到实现函数")
            
            # 调用函数
            start_time = time.time()
            result_df = local_namespace[function_name](df)
            execution_time = time.time() - start_time
            
            # 验证结果
            if not isinstance(result_df, pd.DataFrame):
                raise TypeError(f"实现函数返回了 {type(result_df).__name__}，而不是 DataFrame")
            
            # 如果指定不保留原始特征，则移除
            if not keep_original and affected_columns:
                # 确保所有受影响的列都被转换后才移除
                safe_to_remove = all(col in df.columns for col in affected_columns)
                if safe_to_remove:
                    new_features = suggestion.get("new_features", []) if suggestion else []
                    for col in affected_columns:
                        if col in result_df.columns and col not in new_features:
                            if self.verbose:
                                print(f"🗑️ 根据建议移除原始特征: {col}")
                            result_df = result_df.drop(col, axis=1)
            
            # 确定新增的特征
            new_features = list(set(result_df.columns) - set(df.columns))
            
            # 记录实现结果
            execution_result = {
                "suggestion_id": suggestion_id,
                "status": "success",
                "description": suggestion.get("description", "代码执行") if suggestion else "代码执行",
                "code": code,
                "function_name": function_name,
                "execution_time": execution_time,
                "new_features": new_features,
                "removed_features": [col for col in df.columns if col not in result_df.columns],
                "keep_original": keep_original,
                "error": None
            }
            
            self.execution_history.append(execution_result)
            
            if self.verbose:
                print(f"✅ 成功执行代码，耗时 {execution_time:.4f} 秒")
                print(f"🆕 新增 {len(new_features)} 个特征: {new_features}")
                if execution_result["removed_features"]:
                    print(f"🗑️ 移除了 {len(execution_result['removed_features'])} 个原始特征: {execution_result['removed_features']}")
            
            return result_df, execution_result
            
        except Exception as e:
            error_message = str(e)
            
            if self.verbose:
                print(f"❌ 执行代码时出错: {error_message}")
            
            # 记录失败
            execution_result = {
                "suggestion_id": suggestion_id,
                "status": "error",
                "description": suggestion.get("description", "代码执行") if suggestion else "代码执行",
                "code": code,
                "new_features": [],
                "removed_features": [],
                "keep_original": keep_original,
                "error": error_message
            }
            
            self.execution_history.append(execution_result)
            
            return df, execution_result
    
    def fix_code(self, code: str, error_message: str, df_info: Dict[str, Any], 
                llm_provider: Optional[LLMProvider] = None) -> str:
        """
        修复代码中的错误
        
        参数:
            code: 原始代码
            error_message: 错误信息
            df_info: 数据帧信息
            llm_provider: LLM提供者（如果有）
            
        返回:
            修复后的代码
        """
        # 如果没有LLM提供者，尝试简单修复
        if not llm_provider:
            return self._simple_fix_code(code, error_message)
            
        system_message = """你是一位Python专家，能够修复代码中的错误。
请分析错误信息，并提供修复后的代码。只返回完整的、修复后的代码，不需要解释。"""
        
        prompt = f"""
以下代码在执行时出现错误:

```python
{code}
```

错误信息:
{error_message}

数据集信息:
- 形状: {df_info.get('shape', '未知')}
- 列: {df_info.get('columns', '未知')}
- 数据类型: {df_info.get('dtypes', '未知')}

请修复代码中的错误。只返回完整的、修复后的代码，不要有任何解释。
"""
        
        try:
            response = llm_provider.call(prompt, system_message)
            
            # 提取代码
            from ..parsers.code_parser import CodeParser
            code_parser = CodeParser(verbose=self.verbose)
            fixed_code = code_parser.parse_code_from_response(response)
            
            if not fixed_code:
                # 如果没有提取到代码，返回原始代码
                if self.verbose:
                    print("⚠️ LLM未返回有效的修复代码")
                return code
                
            if self.verbose:
                print("✅ LLM已提供修复代码")
                
            # 安全检查
            safety_result = self.safety_utils.check_code_safety(fixed_code)
            if not safety_result["is_safe"]:
                if self.verbose:
                    print(f"⚠️ 修复代码安全检查未通过: {safety_result['warnings']}")
                fixed_code = self.safety_utils.sanitize_code(fixed_code)
                
            return fixed_code
            
        except Exception as e:
            if self.verbose:
                print(f"❌ 请求LLM修复代码失败: {e}")
            return self._simple_fix_code(code, error_message)
    
    def _simple_fix_code(self, code: str, error_message: str) -> str:
        """
        简单的代码修复尝试，不依赖LLM
        
        参数:
            code: 原始代码
            error_message: 错误信息
            
        返回:
            尝试修复后的代码
        """
        # 常见错误修复
        fixed_code = code
        
        # 1. 修复未定义变量
        name_error_match = re.search(r"name '(\w+)' is not defined", error_message)
        if name_error_match:
            var_name = name_error_match.group(1)
            # 检查是否是常见的导入缺失
            if var_name == 'np':
                fixed_code = "import numpy as np\n" + fixed_code
            elif var_name == 'pd':
                fixed_code = "import pandas as pd\n" + fixed_code
                
        # 2. 修复列不存在错误
        key_error_match = re.search(r"KeyError: ['\"](.*)['\"]", error_message)
        if key_error_match:
            col_name = key_error_match.group(1)
            # 添加列存在检查
            function_def_end = fixed_code.find(":", fixed_code.find("def ")) + 1
            check_code = f"\n    # 检查列是否存在\n    if '{col_name}' not in df.columns:\n        print(f\"警告: 列 '{col_name}' 不存在\")\n        return df\n"
            fixed_code = fixed_code[:function_def_end] + check_code + fixed_code[function_def_end:]
            
        # 3. 修复类型错误
        type_error_match = re.search(r"TypeError: (.*)", error_message)
        if type_error_match:
            type_error = type_error_match.group(1)
            if "cannot convert" in type_error or "must be" in type_error:
                # 添加类型转换
                fixed_code = fixed_code.replace("df[", "df[df.columns].astype('object')[")
                
        if self.verbose and fixed_code != code:
            print("🔧 已尝试简单修复代码")
            
        return fixed_code
    
    def benchmark_execution(self, df: pd.DataFrame, code: str, 
                          iterations: int = 3) -> Dict[str, Any]:
        """
        对代码执行进行性能基准测试
        
        参数:
            df: 输入数据帧
            code: 要执行的代码
            iterations: 执行次数
            
        返回:
            基准测试结果
        """
        times = []
        memory_usage_before = df.memory_usage(deep=True).sum()
        result_df = None
        execution_result = None
        
        for i in range(iterations):
            if self.verbose:
                print(f"🔍 运行基准测试迭代 {i+1}/{iterations}...")
                
            start_time = time.time()
            result_df, execution_result = self.execute(df, code)
            end_time = time.time()
            
            if execution_result["status"] == "error":
                return {
                    "status": "error",
                    "error": execution_result["error"],
                    "message": "基准测试因错误中止"
                }
                
            times.append(end_time - start_time)
            
        # 计算内存使用变化
        memory_usage_after = result_df.memory_usage(deep=True).sum() if result_df is not None else 0
        memory_change = memory_usage_after - memory_usage_before
        
        benchmark_result = {
            "status": "success",
            "avg_execution_time": sum(times) / len(times),
            "min_execution_time": min(times),
            "max_execution_time": max(times),
            "memory_before_bytes": int(memory_usage_before),
            "memory_after_bytes": int(memory_usage_after),
            "memory_change_bytes": int(memory_change),
            "memory_change_percent": float(memory_change / memory_usage_before * 100) if memory_usage_before > 0 else 0,
            "new_features_count": len(execution_result["new_features"]) if execution_result else 0,
            "iterations": iterations
        }
        
        if self.verbose:
            print(f"📊 基准测试完成:")
            print(f"   平均执行时间: {benchmark_result['avg_execution_time']:.4f} 秒")
            print(f"   内存变化: {benchmark_result['memory_change_bytes'] / (1024*1024):.2f} MB ({benchmark_result['memory_change_percent']:.2f}%)")
            
        return benchmark_result

# 要使用re模块，需要导入
import re