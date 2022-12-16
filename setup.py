"""
    :license: MIT, see LICENSE for more details.
"""

from setuptools import find_packages, setup

setup(
    name="uxi-celery-scheduler",
    python_requires=">=3.8",
    author="Aruba UXI",
    version="1.0.0-beta.2",
    description="A SQLAlchemy-based scheduler for celery-beat",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3",
    ],
    url="https://github.com/aruba-uxi/celery-sqlalchemy-scheduler",
    packages=find_packages(exclude=["tests"]),
    package_data={"uxi_celery_scheduler": ["py.typed"]},
    include_package_data=True,
    zip_safe=False,
    install_requires=["celery~=5.2", "sqlalchemy~=1.4", "alembic", "pydantic", "python-dotenv"],
)
