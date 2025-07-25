# GWASStudio Functional Test Scripts

## Overview

The `test_0*.sh` scripts are designed to automate the testing of the GWASStudio CLI tool. They perform a series of operations including data ingestion, querying, and exporting, ensuring that the core functionalities of GWASStudio work as expected.

## Purpose

This script serves multiple purposes:

1. **Automated Testing**: It provides a streamlined way to test the ingestion, querying, and export processes of GWASStudio, helping to identify any issues or inconsistencies in the tool's behavior.

2. **Learning Tool**: The script can be used as a practical guide to understand how to use GWASStudio's CLI commands. By examining the script, users can see how different commands are structured and how they interact with each other.

3. **Documentation**: The script itself serves as a form of documentation, demonstrating real-world usage scenarios and providing a concrete example of how to perform common tasks with GWASStudio.

## Script Breakdown

The script performs the following steps:

1. **Clean Up Existing Data**
   - Removes any existing data and test directories to ensure a clean state for the test.

2. **Navigate to Data Directory**
   - Changes the current directory to the `data` directory, where the input files are located.

3. **Create Test Directory**
   - Creates a directory structure for storing test results.

4. **Ingest Data**
   - Ingests data from a specified metadata file into the test directory.

5. **Query Data**
   - Performs a metadata query using a search file and outputs the results.

6. **Export Data**
   - Exports data based on the search criteria using different algorithms and outputs the results to the test directory.

## Usage

### Prerequisites

- Ensure that GWASStudio is installed and properly configured on your system.

### Running the Script

* **Run the Script**
   ```bash
   cd scripts
   ./test_00.sh
   ```

## Example Output

After running the script, you should see output indicating the progress of each step (ingestion, querying, and exporting). The results will be stored in the specified test directory.

## Troubleshooting

- **Permission Issues**
  - If you encounter permission issues, ensure that the script has executable permissions (`chmod +x scripts/test_0*.sh`).

- **Missing Files**
  - Verify that the input files (`metadata_table.tsv`, `search_example_0*.yml`, `hapmap3/hapmap3_snps.csv` and `regions_query.tsv`) are present in the `data` directory.

- **GWASStudio Not Installed**
  - Ensure that GWASStudio is installed and accessible from your command line.

## Conclusion

The `test_0*.sh` scripts provide a convenient way to test the core functionalities of GWASStudio. By automating the ingestion, querying, and export processes, it helps ensure that the tool works as expected and identifies any potential issues.
