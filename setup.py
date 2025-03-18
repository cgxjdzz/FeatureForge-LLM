from setuptools import setup, find_packages

setup(
    name="featureforge-llm",               # 你的包名
    version="1.0.0",                 # 版本号
    author="Feifan Zhang",              # 作者
    author_email="cgxjdzz@gmail.com",   # 联系邮箱
    description="Automated Feature Engineering Toolkit Based on Large Language Models",  # 描述
    long_description=open("README.md", encoding="utf-8").read(),  # 读取 README 作为长描述
    long_description_content_type="text/markdown",
    url="https://github.com/cgxjdzz/FeatureForge-LLM/",  # 你的代码仓库（如果有）
    packages=find_packages(),  # 自动寻找所有 Python 包
    install_requires=[
        "numpy>=2.2.4",
        "pandas>=2.2.3",
        "pydantic>=2.11.0b1",
        "pydantic_core>=2.31.1",
        "requests>=2.32.3",
        "httpx>=0.28.1",
        "openai>=1.66.3",
        "google-auth>=2.38.0",
        "google-genai>=1.5.0",
        "tqdm>=4.67.1"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",  # 适用 Python 版本
)
