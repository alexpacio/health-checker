from setuptools import setup, find_packages

setup(
    name="db_settings",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "tomli; python_version < '3.11'",  # tomli is built-in for Python 3.11+
    ],
)