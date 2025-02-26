from setuptools import find_packages, setup

setup(
    name="mempipe",
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "bnd=bnd.cli:app",
        ],
    },
    python_requires=">=3.10",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
