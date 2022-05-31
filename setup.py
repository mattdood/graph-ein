import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="graph-ein",
    version="0.0.2",
    author="Matthew Wimberly",
    author_email="matthew.wimb@gmail.com",
    description="A graph database implemented in SQLite.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mattdood/graph-ein",
    project_urls={
        "Bug Tracker": "https://github.com/mattdood/graph-ein/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
