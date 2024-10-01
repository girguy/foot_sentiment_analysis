from setuptools import find_packages, setup

setup(
    name="foot_sa_etl",
    packages=find_packages(exclude=["foot_sa_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
