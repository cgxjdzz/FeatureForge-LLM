"""
JSON解析相关功能
"""
import re
import json
from typing import Dict, List, Any, Union

class JsonParser:
    """
    解析LLM响应中的JSON内容
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化JSON解析器
        
        参数:
            verbose: 是否打印详细信息
        """
        self.verbose = verbose
    
    def parse_json_from_response(self, response: str) -> Union[Dict, List]:
        """
        从LLM回复中提取JSON内容
        
        参数:
            response: LLM回复的内容
            
        返回:
            提取的JSON内容（字典或列表）
        """
        if self.verbose:
            print("\n==== LLM原始响应 ====")
            print(response)
            print("=====================\n")
        
        # 首先尝试直接解析完整响应中的JSON部分
        try:
            # 查找最外层的JSON结构
            json_pattern = r"```json(.*?)```"
            matches = re.findall(json_pattern, response, re.DOTALL)
            
            if matches:
                # 提取JSON字符串并清理
                json_str = matches[0].strip()
                
                # 替换内嵌的代码块
                code_pattern = r"```python(.*?)```"
                json_str = re.sub(code_pattern, lambda m: json.dumps(m.group(1)), json_str)
                
                # 标准化换行符和空格
                json_str = re.sub(r'[\r\n\t]+', ' ', json_str)
                json_str = re.sub(r'\s{2,}', ' ', json_str)
                
                # 尝试解析
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    # 尝试使用更严格的解析方式
                    return self._extract_json_array_or_object(json_str)
                    
            # 尝试从整个文本中提取JSON数组或对象
            return self._extract_json_array_or_object(response)
        
        except Exception as e:
            if self.verbose:
                print(f"⚠️ JSON解析失败: {e}")
            return self._fallback_parse_suggestions(response)
    
    def _extract_json_array_or_object(self, text: str) -> Union[Dict, List]:
        """
        从文本中提取JSON数组或对象
        
        参数:
            text: 输入文本
            
        返回:
            提取的JSON内容
        """
        # 查找JSON数组模式：[...]
        array_match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
        if array_match:
            try:
                return json.loads(array_match.group(0))
            except json.JSONDecodeError:
                pass
        
        # 查找JSON对象模式：{...}
        object_match = re.search(r'\{\s*".*"\s*:.*\}', text, re.DOTALL)
        if object_match:
            try:
                return json.loads(object_match.group(0))
            except json.JSONDecodeError:
                pass
        
        # 如果都失败了，返回空结果
        return {}
    
    def _fallback_parse_suggestions(self, text: str) -> List[Dict]:
        """
        作为最后的手段，从文本中提取建议
        
        参数:
            text: 输入文本
            
        返回:
            提取的建议列表
        """
        suggestions = []
        
        # 使用正则表达式从文本中提取单个建议
        suggestion_pattern = r'"suggestion_id":\s*"([^"]+)".*?"description":\s*"([^"]+)".*?"rationale":\s*"([^"]+)"'
        matches = re.findall(suggestion_pattern, text, re.DOTALL)
        
        for i, match in enumerate(matches):
            suggestion_id, description, rationale = match
            
            # 为每个匹配项提取代码实现
            implementation_pattern = r'"implementation":\s*"(.*?)"'
            impl_match = re.search(implementation_pattern, text[text.find(suggestion_id):], re.DOTALL)
            implementation = impl_match.group(1) if impl_match else ""
            
            # 提取受影响的列
            affected_cols_pattern = r'"affected_columns":\s*\[(.*?)\]'
            cols_match = re.search(affected_cols_pattern, text[text.find(suggestion_id):], re.DOTALL)
            affected_columns = self._parse_string_array(cols_match.group(1)) if cols_match else []
            
            # 提取新特征
            new_features_pattern = r'"new_features":\s*\[(.*?)\]'
            features_match = re.search(new_features_pattern, text[text.find(suggestion_id):], re.DOTALL)
            new_features = self._parse_string_array(features_match.group(1)) if features_match else []
            
            suggestion = {
                "suggestion_id": suggestion_id,
                "suggestion_type": self._guess_suggestion_type(description),
                "description": description,
                "rationale": rationale,
                "implementation": implementation,
                "affected_columns": affected_columns,
                "new_features": new_features
            }
            
            suggestions.append(suggestion)
        
        if not suggestions:
            # 如果上面的方法都失败了，回退到原来的提取方法
            suggestions = self._extract_suggestions_from_text(text)
        
        return suggestions
    
    def _parse_string_array(self, array_str: str) -> List[str]:
        """
        解析字符串数组
        
        参数:
            array_str: 数组字符串
            
        返回:
            解析的字符串列表
        """
        values = []
        for item in array_str.split(','):
            item = item.strip().strip('"\'')
            if item:
                values.append(item)
        return values
    
    def _extract_suggestions_from_text(self, text: str) -> List[Dict]:
        """
        从文本回复中提取建议
        
        参数:
            text: LLM回复文本
            
        返回:
            提取的建议列表
        """
        if self.verbose:
            print("\n==== 尝试从文本中提取建议 ====")
            print(f"文本长度: {len(text)} 字符")
            print("前500个字符预览:")
            print(text[:500] + "..." if len(text) > 500 else text)
            print("============================\n")
            
        suggestions = []
        
        # 寻找可能的建议部分
        suggestion_blocks = re.split(r'\n\d+[\.\)]\s+', text)
        
        if self.verbose:
            print(f"找到 {len(suggestion_blocks) - 1} 个潜在的建议块")
        
        for i, block in enumerate(suggestion_blocks[1:], 1):  # 跳过第一个可能是介绍的部分
            if self.verbose and i <= 3:  # 只显示前3个块作为示例
                print(f"\n== 建议块 #{i} 预览 ==")
                preview = block[:200] + "..." if len(block) > 200 else block
                print(preview)
                print("===================")
                
            lines = block.strip().split('\n')
            
            if not lines:
                continue
                
            # 提取建议信息
            title = lines[0].strip()
            description = "\n".join(lines[1:])
            
            # 提取代码部分（这里假设有CodeParser可用，实际实现时需要导入）
            from ..parsers.code_parser import CodeParser
            code_parser = CodeParser(verbose=self.verbose)
            code = code_parser.parse_code_from_response(block)
            
            if self.verbose and code:
                print(f"从建议 #{i} 中提取到代码:")
                print(code[:200] + "..." if len(code) > 200 else code)
            
            suggestion = {
                "suggestion_id": f"auto_extracted_{i}",
                "suggestion_type": self._guess_suggestion_type(title),
                "description": title,
                "rationale": description,
                "implementation": code if code else "# 需要手动实现",
                "affected_columns": [],
                "new_features": []
            }
            
            suggestions.append(suggestion)
        
        if self.verbose:
            print(f"📝 从文本中提取了{len(suggestions)}个建议")
            
        return suggestions
    
    def _guess_suggestion_type(self, text: str) -> str:
        """
        根据文本猜测建议类型
        
        参数:
            text: 建议文本
            
        返回:
            猜测的建议类型
        """
        text = text.lower()
        
        if any(word in text for word in ["交互", "组合", "乘积", "比率", "interaction"]):
            return "交互"
        elif any(word in text for word in ["标准化", "归一化", "编码", "二值化", "transform", "encoding"]):
            return "转换"
        elif any(word in text for word in ["领域", "知识", "domain", "knowledge"]):
            return "领域知识"
        else:
            return "其他"