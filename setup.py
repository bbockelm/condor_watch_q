from pathlib import Path

from setuptools import setup, find_packages

THIS_DIR = Path(__file__).parent

setup(
    name="condor_watch_q",
    version="0.1.0",
    author="Josh Karpel",
    author_email="josh.karpel@gmail.com",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Natural Language :: English",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
    ],
    py_modules=["condor_watch_q"],
    entry_points={"console_scripts": ["condor_watch_q = condor_watch_q:cli"]},
    install_requires=Path("requirements.txt").read_text().splitlines(),
)
