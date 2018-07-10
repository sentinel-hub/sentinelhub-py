import io
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


def get_long_description():
    return io.open('README.md', encoding="utf-8").read()


setup(
    name='sentinelhub',
    python_requires='>=3.5,<3.7',
    version=get_version(),
    description='Sentinel Hub Utilities',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development'
    ]
)
