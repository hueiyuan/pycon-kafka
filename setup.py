from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    "dataclasses==0.8",
    "retrying==1.3.3",
    "confluent-kafka==1.8.2",
    "fastavro==1.4.5",
    "python-dotenv==0.20.0"
]

setup(
    name="PyConKafkaExample",
    version='0.1.0',
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=REQUIRED_PACKAGES,
    zip_safe=False,
    python_requires=">=3.8",
    description="Pycon Kafka library",
    author="hueiyuansu",
    author_email="hueiyuansu@gmail.com",
    license="MIT",
    platforms="Independant",
)
