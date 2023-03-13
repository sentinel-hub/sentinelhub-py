import io
import os
from typing import List

from setuptools import find_packages, setup


def parse_requirements(filename: str) -> List[str]:
    required_packages = []
    with open(os.path.join(os.path.dirname(__file__), filename)) as req_file:
        for line in req_file:
            required_packages.append(line.strip())
    return required_packages


def get_version() -> str:
    path = os.path.join(os.path.dirname(__file__), "sentinelhub", "_version.py")
    with open(path) as version_file:
        for line in version_file:
            if line.find("__version__") >= 0:
                version = line.split("=")[1].strip()
                return version.strip('"').strip("'")

    raise ValueError(f"Version not found in {path}")


def get_long_description() -> str:
    return io.open("README.md", encoding="utf-8").read()


setup(
    name="sentinelhub",
    python_requires=">=3.7",
    version=get_version(),
    description="Python API for Sentinel Hub",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/sentinel-hub/sentinelhub-py",
    project_urls={
        "Documentation": "https://sentinelhub-py.readthedocs.io",
        "Source Code": "https://github.com/sentinel-hub/sentinelhub-py",
        "Bug Tracker": "https://github.com/sentinel-hub/sentinelhub-py/issues",
        "Forum": "https://forum.sentinel-hub.com",
    },
    author="Sinergise EO research team",
    author_email="eoresearch@sinergise.com",
    license="MIT",
    packages=find_packages(),
    package_data={
        "sentinelhub": [
            "sentinelhub/config.json",
            "sentinelhub/.utmzones.geojson",
            "sentinelhub/py.typed",
        ]
    },
    include_package_data=True,
    install_requires=parse_requirements("requirements.txt"),
    extras_require={
        "AWS": parse_requirements("requirements-aws.txt"),
        "DEV": parse_requirements("requirements-dev.txt"),
        "DOCS": parse_requirements("requirements-docs.txt"),
    },
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "sentinelhub=sentinelhub.commands:main_help",
            "sentinelhub.aws=sentinelhub.aws.commands:aws",
            "sentinelhub.config=sentinelhub.commands:config",
            "sentinelhub.download=sentinelhub.commands:download",
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
    ],
)
