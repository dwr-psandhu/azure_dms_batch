from setuptools import setup
import versioneer

requirements = [
    'azure-batch==11.0.0',
    'azure-storage-blob==12.9.0',
    'azure-mgmt-resource>=20.0.0',
    'azure-identity>=1.7.1',
    'msrest>=0.6.21'
]

setup(
    name='azure_dms_batch',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Azure Batch for Delta Modeling Section",
    license="MIT",
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    url='https://github.com/dwr-psandhu/azure_dms_batch',
    packages=['dmsbatch'],
    entry_points={
        'console_scripts': [
            'dmsbatch=dmsbatch.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='azure_dms_batch',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
