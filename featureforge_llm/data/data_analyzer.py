"""
数据分析工具
"""
from typing import Dict, Any, List
import pandas as pd

class DataAnalyzer:
    """
    分析数据帧并提取有用信息的工具
    """
    
    def __init__(self, verbose: bool = True):
        """
        初始化数据分析器
        
        参数:
            verbose: 是否打印详细信息
        """
        self.verbose = verbose
    
    def get_dataframe_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        获取数据帧的基本信息
        
        参数:
            df: 输入数据帧
            
        返回:
            数据帧信息字典
        """
        info = {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "missing_values": {col: int(df[col].isna().sum()) for col in df.columns},
            "unique_values": {col: int(df[col].nunique()) for col in df.columns}
        }
        
        # 对分类特征收集值分布
        cat_cols = df.select_dtypes(include=['object', 'category']).columns
        if len(cat_cols) > 0:
            info["categorical_distributions"] = {}
            for col in cat_cols:
                if df[col].nunique() < 15:  # 只包括较少唯一值的特征
                    info["categorical_distributions"][col] = df[col].value_counts().to_dict()
        
        # 对数值特征收集基本统计信息
        num_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if len(num_cols) > 0:
            info["numerical_statistics"] = {}
            for col in num_cols:
                info["numerical_statistics"][col] = {
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None,
                    "median": float(df[col].median()) if not pd.isna(df[col].median()) else None
                }
                
        return info
    
    def analyze_correlations(self, df: pd.DataFrame, target_column: str = None) -> Dict[str, Any]:
        """
        分析数值特征之间的相关性
        
        参数:
            df: 输入数据帧
            target_column: 目标列名称（如果有）
            
        返回:
            相关性分析结果
        """
        # 只选择数值列
        num_df = df.select_dtypes(include=['int64', 'float64'])
        if num_df.empty:
            return {"message": "没有数值列可以分析相关性"}
            
        # 计算相关性矩阵
        corr_matrix = num_df.corr().round(3)
        
        result = {
            "correlation_matrix": corr_matrix.to_dict()
        }
        
        # 如果指定了目标列，提取与目标的相关性
        if target_column and target_column in corr_matrix.columns:
            target_corrs = corr_matrix[target_column].drop(target_column).sort_values(ascending=False)
            result["target_correlations"] = target_corrs.to_dict()
            
            # 找出高相关特征
            high_corr_threshold = 0.7
            high_corr_features = target_corrs[abs(target_corrs) > high_corr_threshold]
            if not high_corr_features.empty:
                result["high_corr_features"] = high_corr_features.to_dict()
        
        return result
    
    def detect_skewed_features(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        检测高度偏斜的数值特征
        
        参数:
            df: 输入数据帧
            
        返回:
            特征偏度字典
        """
        try:
            from scipy.stats import skew
            
            # 只选择数值列
            num_df = df.select_dtypes(include=['int64', 'float64'])
            if num_df.empty:
                return {}
                
            # 计算偏度
            skewed_features = {}
            for col in num_df.columns:
                if num_df[col].nunique() > 1:  # 至少有两个不同的值
                    sk = skew(num_df[col].dropna())
                    if abs(sk) > 0.5:  # 只包括明显偏斜的特征
                        skewed_features[col] = float(sk)
            
            return dict(sorted(skewed_features.items(), key=lambda x: abs(x[1]), reverse=True))
        except ImportError:
            if self.verbose:
                print("⚠️ 未安装scipy，无法检测特征偏度")
            return {}
    
    def suggest_feature_transformations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        基于数据分析建议特征转换
        
        参数:
            df: 输入数据帧
            
        返回:
            特征转换建议列表
        """
        suggestions = []
        
        # 检查类别特征
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        if cat_cols:
            # 建议对高基数类别特征进行特殊处理
            high_cardinality_cols = [col for col in cat_cols if df[col].nunique() > 10]
            if high_cardinality_cols:
                suggestions.append({
                    "suggestion_type": "转换",
                    "description": "对高基数类别特征进行编码",
                    "affected_columns": high_cardinality_cols,
                    "technique": "target_encoding"  # 或 frequency_encoding 等
                })
            
            # 建议对低基数类别特征进行独热编码
            low_cardinality_cols = [col for col in cat_cols if df[col].nunique() <= 10]
            if low_cardinality_cols:
                suggestions.append({
                    "suggestion_type": "转换",
                    "description": "对低基数类别特征进行独热编码",
                    "affected_columns": low_cardinality_cols,
                    "technique": "one_hot_encoding"
                })
        
        # 检测偏斜特征并建议转换
        skewed_features = self.detect_skewed_features(df)
        if skewed_features:
            suggestions.append({
                "suggestion_type": "转换",
                "description": "对偏斜数值特征进行变换",
                "affected_columns": list(skewed_features.keys()),
                "technique": "log_transform",
                "skewness_values": skewed_features
            })
        
        # 检查是否有日期时间列
        date_cols = []
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_cols.append(col)
            elif df[col].dtype == 'object':
                # 尝试转换为日期时间
                try:
                    pd.to_datetime(df[col], errors='raise')
                    date_cols.append(col)
                except:
                    pass
        
        if date_cols:
            suggestions.append({
                "suggestion_type": "领域知识",
                "description": "从日期时间特征提取组件",
                "affected_columns": date_cols,
                "technique": "datetime_features"
            })
        
        return suggestions