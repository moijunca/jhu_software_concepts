"""Setup configuration for GradCafe Analytics Module 5."""
from setuptools import setup, find_packages

setup(
    name="gradcafe-analytics",
    version="5.0.0",
    description="GradCafe Analytics - Software Assurance Hardened Version",
    author="Your Name",
    packages=find_packages(),
    package_dir={"": "src"},
    install_requires=[
        "Flask>=3.0.0",
        "psycopg[binary]>=3.2.0",
    ],
    extras_require={
        "dev": [
            "pylint>=4.0.0",
            "pytest>=8.0.0",
            "pytest-cov>=5.0.0",
            "pydeps>=3.0.0",
        ]
    },
    python_requires=">=3.11",
)
