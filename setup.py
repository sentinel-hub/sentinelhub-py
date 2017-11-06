import os

from setuptools import setup, find_packages

def parse_requirements(file):
    return sorted(set(
        line.partition('#')[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    ) - set(''))

INSTALL_REQUIRES = parse_requirements("requirements.txt")

EXTRA_REQUIRES = parse_requirements("requirements-dev.txt")

setup(
    name='sentinelhub',
    version='0.1.7',
    description='Sentinel Hub Utilities',
    url='https://github.com/sinergise/sentinelhub',
    author='Sinergise ltd.',
    author_email='info@sinergise.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'DEV' : EXTRA_REQUIRES
    },
    zip_safe=False,
    entry_points={
    'console_scripts' : [
        'sentinelhub.aws=sentinelhub.commands:aws',
        'sentinelhub.download=sentinelhub.commands:download'
        ]
    },)
