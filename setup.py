import os
from setuptools import setup, find_packages


def parse_requirements(file):
    return sorted(set(
        line.partition('#')[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    ) - set(''))


def get_version():
    for line in open(os.path.join(os.path.dirname(__file__), 'sentinelhub', '__init__.py')):
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"').strip("'")
    return version


setup(
    name='sentinelhub',
    python_requires='>=3.5',
    version=get_version(),
    description='Sentinel Hub Utilities',
    url='https://github.com/sentinel-hub/sentinelhub-py',
    author='Sinergise ltd.',
    author_email='info@sentinel-hub.com',
    license='MIT',
    packages=find_packages(),
    package_data={'sentinelhub': ['sentinelhub/config.json']},
    include_package_data=True,
    install_requires=parse_requirements("requirements.txt"),
    extras_require={'DEV': parse_requirements("requirements-dev.txt")},
    zip_safe=False,
    entry_points={'console_scripts': ['sentinelhub=sentinelhub.commands:main_help',
                                      'sentinelhub.aws=sentinelhub.commands:aws',
                                      'sentinelhub.config=sentinelhub.commands:config',
                                      'sentinelhub.download=sentinelhub.commands:download']}
)
