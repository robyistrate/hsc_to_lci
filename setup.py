from setuptools import setup, find_packages

setup(
    name="hsc_to_lci", 
    version="0.0.1",
    python_requires=">=3.9,<3.12",
    author="Robert Istrate <i.r.istrate@cml.leidenuniv.nl>",
    license=open("LICENSE").read(),
    include_package_data=True,
    install_requires=[
        "brightway2",
    ],
    url="https://github.com/robyistrate/hsc_to_lci",
    description="Convert HSC Chemistry simulation results to Brightway-format life cycle inventories",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
    ],
)
