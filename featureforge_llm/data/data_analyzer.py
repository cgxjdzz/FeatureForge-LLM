"""
Data Analysis Tools
"""
from typing import Dict, Any, List
import pandas as pd

class DataAnalyzer:
   """
   Tool for analyzing dataframes and extracting useful information
   """
   
   def __init__(self, verbose: bool = True):
       """
       Initialize data analyzer
       
       Parameters:
           verbose: Whether to print detailed information
       """
       self.verbose = verbose
   
   def get_dataframe_info(self, df: pd.DataFrame) -> Dict[str, Any]:
       """
       Get basic information about the dataframe
       
       Parameters:
           df: Input dataframe
           
       Returns:
           Dataframe information dictionary
       """
       info = {
           "shape": df.shape,
           "columns": df.columns.tolist(),
           "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
           "missing_values": {col: int(df[col].isna().sum()) for col in df.columns},
           "unique_values": {col: int(df[col].nunique()) for col in df.columns}
       }
       
       # Collect value distributions for categorical features
       cat_cols = df.select_dtypes(include=['object', 'category']).columns
       if len(cat_cols) > 0:
           info["categorical_distributions"] = {}
           for col in cat_cols:
               if df[col].nunique() < 15:  # Only include features with fewer unique values
                   info["categorical_distributions"][col] = df[col].value_counts().to_dict()
       
       # Collect basic statistical information for numerical features
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
       Analyze correlations between numerical features
       
       Parameters:
           df: Input dataframe
           target_column: Target column name (if any)
           
       Returns:
           Correlation analysis results
       """
       # Select only numerical columns
       num_df = df.select_dtypes(include=['int64', 'float64'])
       if num_df.empty:
           return {"message": "No numerical columns to analyze correlation"}
           
       # Calculate correlation matrix
       corr_matrix = num_df.corr().round(3)
       
       result = {
           "correlation_matrix": corr_matrix.to_dict()
       }
       
       # If target column is specified, extract correlations with target
       if target_column and target_column in corr_matrix.columns:
           target_corrs = corr_matrix[target_column].drop(target_column).sort_values(ascending=False)
           result["target_correlations"] = target_corrs.to_dict()
           
           # Find highly correlated features
           high_corr_threshold = 0.7
           high_corr_features = target_corrs[abs(target_corrs) > high_corr_threshold]
           if not high_corr_features.empty:
               result["high_corr_features"] = high_corr_features.to_dict()
       
       return result
   
   def detect_skewed_features(self, df: pd.DataFrame) -> Dict[str, float]:
       """
       Detect highly skewed numerical features
       
       Parameters:
           df: Input dataframe
           
       Returns:
           Feature skewness dictionary
       """
       try:
           from scipy.stats import skew
           
           # Select only numerical columns
           num_df = df.select_dtypes(include=['int64', 'float64'])
           if num_df.empty:
               return {}
               
           # Calculate skewness
           skewed_features = {}
           for col in num_df.columns:
               if num_df[col].nunique() > 1:  # At least two different values
                   sk = skew(num_df[col].dropna())
                   if abs(sk) > 0.5:  # Only include clearly skewed features
                       skewed_features[col] = float(sk)
           
           return dict(sorted(skewed_features.items(), key=lambda x: abs(x[1]), reverse=True))
       except ImportError:
           if self.verbose:
               print("⚠️ scipy not installed, cannot detect feature skewness")
           return {}
   
   def suggest_feature_transformations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
       """
       Suggest feature transformations based on data analysis
       
       Parameters:
           df: Input dataframe
           
       Returns:
           List of feature transformation suggestions
       """
       suggestions = []
       
       # Check categorical features
       cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
       if cat_cols:
           # Suggest special handling for high cardinality categorical features
           high_cardinality_cols = [col for col in cat_cols if df[col].nunique() > 10]
           if high_cardinality_cols:
               suggestions.append({
                   "suggestion_type": "Transformation",
                   "description": "Encode high cardinality categorical features",
                   "affected_columns": high_cardinality_cols,
                   "technique": "target_encoding"  # or frequency_encoding etc.
               })
           
           # Suggest one-hot encoding for low cardinality categorical features
           low_cardinality_cols = [col for col in cat_cols if df[col].nunique() <= 10]
           if low_cardinality_cols:
               suggestions.append({
                   "suggestion_type": "Transformation",
                   "description": "One-hot encode low cardinality categorical features",
                   "affected_columns": low_cardinality_cols,
                   "technique": "one_hot_encoding"
               })
       
       # Detect skewed features and suggest transformations
       skewed_features = self.detect_skewed_features(df)
       if skewed_features:
           suggestions.append({
               "suggestion_type": "Transformation",
               "description": "Transform skewed numerical features",
               "affected_columns": list(skewed_features.keys()),
               "technique": "log_transform",
               "skewness_values": skewed_features
           })
       
       # Check for datetime columns
       date_cols = []
       for col in df.columns:
           if pd.api.types.is_datetime64_any_dtype(df[col]):
               date_cols.append(col)
           elif df[col].dtype == 'object':
               # Try to convert to datetime
               try:
                   pd.to_datetime(df[col], errors='raise')
                   date_cols.append(col)
               except:
                   pass
       
       if date_cols:
           suggestions.append({
               "suggestion_type": "Domain Knowledge",
               "description": "Extract components from datetime features",
               "affected_columns": date_cols,
               "technique": "datetime_features"
           })
       
       return suggestions