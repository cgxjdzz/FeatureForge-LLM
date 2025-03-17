"""
代码提取与清理
"""
import re
from typing import Optional

class CodeParser:
    """
    解析LLM响应中的代码内容
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化代码解析器
        
        参数:
            verbose: 是否打印详细信息
        """
        self.verbose = verbose
    
    def parse_code_from_response(self, response: str) -> str:
        """
        从LLM回复中提取Python代码，支持嵌套代码块
        
        参数:
            response: LLM回复的内容
            
        返回:
            提取的Python代码
        """
        # 尝试匹配最外层的Python代码块
        code_pattern = r"```python(.*?)```"
        matches = re.findall(code_pattern, response, re.DOTALL)
        
        if matches:
            # 清理提取的代码
            extracted_code = matches[0].strip()
            
            # 检查是否有内部代码块标记，并移除它们
            extracted_code = re.sub(r'```\w*\n', '', extracted_code)
            extracted_code = extracted_code.replace('\n```', '')
            
            return extracted_code
        
        # 如果没有Markdown格式，尝试查找可能的Python代码部分
        if "def " in response and "return" in response:
            code_start = response.find("def ")
            
            # 找到代码块的结束位置
            code_lines = response[code_start:].split('\n')
            end_line = 0
            indent_level = 0
            in_function = False
            
            for i, line in enumerate(code_lines):
                if line.strip().startswith("def ") and line.strip().endswith(":"):
                    in_function = True
                    indent_level = len(line) - len(line.lstrip())
                    continue
                    
                if in_function:
                    if line.strip() and not line.startswith(" " * (indent_level + 4)):
                        # 缩进减少，可能是函数结束
                        if i > 2:  # 至少包含函数定义和一行函数体
                            end_line = i
                            break
            
            if end_line > 0:
                extracted_code = "\n".join(code_lines[:end_line])
                return extracted_code
            else:
                return "\n".join(code_lines)
                
        return ""
    
    def clean_implementation_code(self, code: str) -> str:
        """
        清理实现代码中的Markdown标记和特殊字符
        
        参数:
            code: 原始代码
            
        返回:
            清理后的代码
        """
        # 移除Markdown代码块标记
        code = re.sub(r'```python\s*', '', code)
        code = re.sub(r'\s*```', '', code)
        
        # 移除可能的引号转义
        code = code.replace('\\"', '"')
        
        # 移除开头和结尾的空白
        return code.strip()
    
    def extract_function_name(self, code: str) -> Optional[str]:
        """
        从代码中提取函数名
        
        参数:
            code: 代码字符串
            
        返回:
            函数名，如果找不到则返回None
        """
        match = re.search(r'def\s+(\w+)', code)
        if match:
            return match.group(1)
        return None
    
    def ensure_function_structure(self, code: str, function_name: Optional[str] = None) -> str:
        """
        确保代码是一个函数结构，如果不是则包装它
        
        参数:
            code: 原始代码
            function_name: 指定的函数名，如果为None则自动生成
            
        返回:
            确保为函数结构的代码
        """
        if not code.strip():
            return ""
            
        # 如果已经是函数定义，直接返回
        if code.strip().startswith("def "):
            return code
            
        # 生成函数名
        if not function_name:
            function_name = f"process_feature_{hash(code) % 10000}"
            
        # 检查代码是否已经包含函数调用
        if "df = " in code or "return df" in code:
            # 已经包含处理逻辑，只需要包装成函数
            wrapped_code = f"def {function_name}(df):\n" + "\n".join(
                f"    {line}" for line in code.split("\n")
            )
        else:
            # 可能只是一些操作步骤，需要添加DataFrame处理逻辑
            wrapped_code = f"""def {function_name}(df):
    df_result = df.copy()
    
    # 实现特征工程逻辑
    {code.strip()}
    
    return df_result"""
        
        # 确保有返回语句
        if "return" not in wrapped_code:
            wrapped_code = wrapped_code.rstrip() + "\n    return df_result"
            
        return wrapped_code