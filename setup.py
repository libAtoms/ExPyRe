import setuptools

setuptools.setup(
    name="expyre",
    version="0.1.0b",
    packages=setuptools.find_packages(),
    install_requires=["click>=7.0"],
    entry_points="""
    [console_scripts]
    xpr=expyre.cli.cli:cli
    """
)
