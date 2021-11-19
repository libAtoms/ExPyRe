import setuptools

setuptools.setup(
    name="expyre-wfl",
    version="0.1.0b",
    packages=setuptools.find_packages(exclude=["tests"]),
    install_requires=["click>=7.0"],
    entry_points="""
    [console_scripts]
    xpr=expyre.cli.cli:cli
    """
)
