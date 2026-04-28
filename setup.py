from setuptools import setup, find_packages

setup(
    name="sos-analyzer",
    version="0.2.0",
    packages=find_packages(),
    package_data={"sos_analyzer": ["conf/*.txt"]},
    entry_points={
        "console_scripts": [
            "sos-analyzer=sos_analyzer.cli:main",
            "sos-analyzer-diff=diff_reports:main",
        ],
    },
    python_requires=">=3.9",
)
