from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="inkling",
    version="0.2.0",
    url="https://github.com/cloudjo21/inkling.git",
    packages=find_packages("src"),
    package_dir={"inkling": "src/inkling"},
    python_requires=">=3.11.6",
    long_description=open("README.md").read(),
    install_requires=required,
)
