"""Setup script for the Databricks Ingestion & ETL Pipeline wheel package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="databricks-ingestion-etl-pipeline",
    version="1.0.0",
    author="Data Engineering Team",
    description="Azure Databricks data ingestion and ETL pipeline for Salesforce & PMM",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/databricks-ingestion-etl-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=requirements,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "salesforce-ingestion=entry_points.salesforce_ingestion_entry:main",
            "pmm-ingestion=entry_points.pmm_ingestion_entry:main",
            "etl-pipeline=entry_points.etl_pipeline_entry:main",
        ],
    },
)
