from setuptools import setup, find_packages

setup(
    name='csv_diff',
    version='0.3',
    packages=find_packages(where='src'),
    package_dir={'':'src'},
    install_requires=[
        'pandas>=2.0.0',  # Specify the version you need
    ],
    entry_points={
        'console_scripts': [
            'csv-diff=csv_diff:main',  # Replace with your module and function
        ],
    },
    include_package_data=True,
    description='script to automate the looker data validation process using csv inputs',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/joon-solutions/looker_data_validation',
    author='Ken Luu',
    author_email='luutuankiet.ftu2@gmail.com',
    license='MIT',
)