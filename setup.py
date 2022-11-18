from glob import glob

from setuptools import find_packages, setup

setup(
    name="next-gen-attribution",
    version="0.1",
    packages=find_packages(),
    url="http://www.ef.com",
    license="",
    author="",
    author_email="",
    description="Next gen attribution libraries",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    data_files=[
        ("workflows", [x for x in glob("workflows/**", recursive=True) if "." in x])
    ],
    install_requires=[
        "matplotlib==3.6.2",
        "pandas==1.5.1",
        "scikit-learn==1.1.3",
        "pytest==7.2.0",
        "pytest-runner==6.0.0",
        "pre-commit==2.20.0",
    ],
    extras_require={
        "mac": [
        ],
        "linux": [
        ],
    },
)
