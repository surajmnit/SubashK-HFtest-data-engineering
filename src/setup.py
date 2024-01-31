from pathlib import Path
import setuptools
import subprocess
import sys
from datetime import datetime

# Get the current date in YY.MM.DD format
current_date = datetime.now().strftime("%y.%m.%d")

# Get the current version from the VERSION file
this_directory = Path(__file__).resolve().parent
version_file = this_directory / "VERSION"
current_version = version_file.read_text().strip()

# If the version doesn't match the current date, reset the counter
if not current_version.startswith(current_date):
    counter = 1
else:
    counter = int(current_version.split(".")[-1]) + 1

# Construct the new version
new_version = f"{current_date}.{counter}"

# Update the version in the VERSION file
version_file.write_text(new_version)

# Read the README.md file for the long description
long_description = (this_directory / "ETL_README.md").read_text()

# Read dependencies from requirements.txt or PEX file
if (this_directory / "requirements.txt").exists():
    with open(this_directory / "requirements.txt", "r") as f:
        install_requires = f.read().splitlines()
elif (this_directory / "dependencies.pex").exists():
    subprocess.run([sys.executable, "-m", "pip", "install", str(this_directory / "dependencies.pex")])
    # Extract dependencies from PEX file
    output = subprocess.run([sys.executable, str(this_directory / "dependencies.pex"), "--print-entry-point", "requirements"], capture_output=True, text=True)
    install_requires = output.stdout.strip().split("\n")
else:
    raise FileNotFoundError("Neither requirements.txt nor dependencies.pex found")

# Run tests with pytest
test_script_path = this_directory / "tests" / "test.py"
result = subprocess.run([sys.executable, "-m", "pytest", str(test_script_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if result.returncode != 0:
    print("Test(s) failed. Aborting package building.")
    print(result.stdout.decode())
    print(result.stderr.decode())
    sys.exit(1)

# Setup configuration
setuptools.setup(
    name="data-engineering",
    version=new_version,  # Use the updated version
    author="Subash Konar",
    author_email="subashkonar13@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
