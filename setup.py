import setuptools

setuptools.setup(
    name="expyre-wfl",
    version="0.1.2",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=["tests"]),
    install_requires=["click>=7.0"],
    entry_points="""
    [console_scripts]
    xpr=expyre.cli.cli:cli
    """
)
