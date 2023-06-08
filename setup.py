from setuptools import setup, find_packages

setup(
    name='phaistos_importer',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'requests',
        'xlrd'
    ],
    entry_points={
        'console_scripts': [
            'phaistos_importer = phaistos_importer:cli',
        ],
    },
    license='',
    author='Filippos Slavik',
    author_email='filippos@slavik.gr',
    description=''
)
