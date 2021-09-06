import io
import os
from setuptools import setup, find_packages
from sys import version_info


def parse_requirements(file):
    required_packages = []
    with open(os.path.join(os.path.dirname(__file__), file)) as req_file:
        for line in req_file:
            if '/' not in line:
                required_packages.append(line.strip())
    return required_packages


def get_main_requirements():
    """ Collects requirements from a file and adds additional requirement for Python 3.6
    """
    requirements = parse_requirements('requirements.txt')

    if version_info.major == 3 and version_info.minor == 6:
        requirements.append('dataclasses')

    return requirements


def get_version():
    for line in open(os.path.join(os.path.dirname(__file__), 'sentinelhub', '_version.py')):
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            return version.strip('"').strip("'")


def get_long_description():
    return io.open('README.md', encoding="utf-8").read()


def update_package_config():
    """ Every time sentinelhub package is installed entire config.json is overwritten. However this function
    will check if sentinelhub is already installed and try to copy those parameters from old config.json that are by
    default set to an empty value (i.e. instance_id, aws_access_key_id and aws_secret_access_key) into new config.json
    file.
    """
    try:
        import importlib
        import sys
        import json

        path = importlib.machinery.PathFinder().find_spec('sentinelhub', sys.path[1:]).submodule_search_locations[0]
        old_config_filename = os.path.join(path, 'config.json')

        with open(old_config_filename, 'r') as file:
            old_config = json.load(file)

        from sentinelhub.config import SHConfig

        config = SHConfig()
        for attr, value in old_config.items():
            if hasattr(config, attr) and not getattr(config, attr):
                setattr(config, attr, value)

        config.save()

    except BaseException:
        pass


def try_create_config_file():
    """ After the package is installed it will try to trigger saving a config.json file
    """
    try:
        from sentinelhub.config import SHConfig
        SHConfig()
    except BaseException:
        pass


update_package_config()

setup(
    name='sentinelhub',
    python_requires='>=3.6',
    version=get_version(),
    description='Sentinel Hub Utilities',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url='https://github.com/sentinel-hub/sentinelhub-py',
    author='Sinergise ltd.',
    author_email='info@sentinel-hub.com',
    license='MIT',
    packages=find_packages(),
    package_data={'sentinelhub': ['sentinelhub/config.json', 'sentinelhub/.utmzones.geojson']},
    include_package_data=True,
    install_requires=get_main_requirements(),
    extras_require={
        'DEV': parse_requirements('requirements-dev.txt'),
        'DOCS': parse_requirements('requirements-docs.txt')
    },
    zip_safe=False,
    entry_points={'console_scripts': ['sentinelhub=sentinelhub.commands:main_help',
                                      'sentinelhub.aws=sentinelhub.commands:aws',
                                      'sentinelhub.config=sentinelhub.commands:config',
                                      'sentinelhub.download=sentinelhub.commands:download']},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development'
    ]
)
try_create_config_file()
