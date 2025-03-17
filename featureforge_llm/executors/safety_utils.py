"""
代码安全检查工具
"""
import re
from typing import List, Dict, Any

class SafetyUtils:
    """
    提供代码安全检查的工具
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化安全工具
        
        参数:
            verbose: 是否打印详细信息
        """
        self.verbose = verbose
        # 定义危险操作的模式
        self.dangerous_patterns = [
            r'import\s+os', 
            r'import\s+sys',
            r'import\s+subprocess',
            r'__import__',
            r'eval\s*\(',
            r'exec\s*\(',
            r'os\.(system|popen|execv|spawn)',
            r'subprocess\.(Popen|call|run)',
            r'open\s*\(.+,\s*[\'"]w',  # 写入文件操作
            r'shutil\.(rmtree|remove)',
            r'glob\.',
            r'(rm|del|remove)\s+',
            r'request\.(get|post)'
        ]
    
    def add_safety_checks(self, code: str, affected_columns: List[str]) -> str:
        """
        添加安全检查确保代码正确执行
        
        参数:
            code: 原始代码
            affected_columns: 受影响的列
            
        返回:
            添加安全检查后的代码
        """
        # 获取函数名
        func_match = re.search(r'def\s+(\w+)', code)
        if not func_match:
            if self.verbose:
                print("⚠️ 无法从代码中提取函数名")
            return code
            
        # 添加列存在性检查
        column_checks = []
        for col in affected_columns:
            if col:
                column_checks.append(
                    f'    # 检查列 "{col}" 是否存在\n'
                    f'    if "{col}" not in df.columns:\n'
                    f'        print(f"警告: 列 \\"{col}\\" 不存在，跳过该列处理")\n'
                    f'        return df'
                )
        
        # 如果有需要检查的列，插入检查代码
        if column_checks:
            # 查找函数定义的末尾
            func_def_end = code.find(":", code.find("def ")) + 1
            
            # 插入安全检查代码
            safety_code = "\n" + "\n".join(column_checks) + "\n    \n    # 创建副本避免修改原始数据\n    df = df.copy()\n"
            code = code[:func_def_end] + safety_code + code[func_def_end:]
        
        return code
    
    def check_code_safety(self, code: str) -> Dict[str, Any]:
        """
        检查代码是否存在安全风险
        
        参数:
            code: 要检查的代码
            
        返回:
            安全检查结果
        """
        # 初始化检查结果
        result = {
            "is_safe": True,
            "warnings": [],
            "details": {}
        }
        
        # 检查危险操作
        for pattern in self.dangerous_patterns:
            matches = re.findall(pattern, code)
            if matches:
                result["is_safe"] = False
                warning = f"代码中包含可能的危险操作: {pattern}"
                result["warnings"].append(warning)
                result["details"][pattern] = matches
                
                if self.verbose:
                    print(f"⚠️ {warning}")
        
        # 检查其他潜在问题
        # 1. 递归调用检查
        func_name_match = re.search(r'def\s+(\w+)', code)
        if func_name_match:
            func_name = func_name_match.group(1)
            if re.search(fr'{func_name}\s*\(', code[code.find("def ")+len(f"def {func_name}"):]): 
                result["warnings"].append(f"代码中可能存在递归调用: {func_name}")
                
        # 2. 检查无限循环风险
        for loop_keyword in ["while", "for"]:
            loop_matches = re.findall(fr'{loop_keyword}\s+.*:', code)
            for loop_match in loop_matches:
                loop_body_start = code.find(loop_match) + len(loop_match)
                # 检查循环体中是否有break语句
                next_break = code.find("break", loop_body_start)
                if next_break == -1 or (code.find("def ", loop_body_start) != -1 and code.find("def ", loop_body_start) < next_break):
                    if loop_keyword == "while" and "True" in loop_match:
                        result["warnings"].append("代码中可能存在没有break条件的无限循环")
                        
        return result
    
    def sanitize_code(self, code: str) -> str:
        """
        清理代码中的潜在危险部分
        
        参数:
            code: 原始代码
            
        返回:
            清理后的代码
        """
        # 移除危险的导入
        for pattern in [r'import\s+os.*\n', r'import\s+sys.*\n', r'import\s+subprocess.*\n']:
            code = re.sub(pattern, '# 导入已移除，出于安全考虑\n', code)
        
        # 替换危险函数调用
        code = re.sub(r'eval\s*\(', '# eval(', code)
        code = re.sub(r'exec\s*\(', '# exec(', code)
        code = re.sub(r'os\.(system|popen|execv|spawn)', '# os.\\1', code)
        code = re.sub(r'subprocess\.(Popen|call|run)', '# subprocess.\\1', code)
        
        # 添加安全注释
        safe_code = "# 注意：此代码已经过安全检查和清理\n" + code
        
        return safe_code