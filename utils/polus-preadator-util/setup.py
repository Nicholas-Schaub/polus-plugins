import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
with open("VERSION",'r') as fh:
    version = fh.read()

setuptools.setup(
    name="preadator",
    version=version,
    author="Nick Schaub",
    author_email="nick.schaub@labshare.org",
    description="Process and Thread Management Utility.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/LabShare/polus-plugins/tree/master/utils/polus-preadator-util/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)