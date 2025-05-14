from setuptools import setup, find_packages

setup(
    name="task_manager_lib",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pydantic>=2.0.0"
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="Client library for Task Manager API",
    python_requires=">=3.8",
)
