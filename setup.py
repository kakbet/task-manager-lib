from setuptools import setup, find_packages

setup(
    name="task-manager-lib",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pydantic>=2.0.0",
        "httpx>=0.24.0"
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="Shared library for Task Manager",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
