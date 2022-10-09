import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ein-graph",
    version="0.0.12",
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
    package_data={"ein": ["sql/*.sql"],},
    python_requires=">=3.6",
)
