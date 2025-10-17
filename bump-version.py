import tomli
import tomli_w


def update_pyproject_version(version_file_path, pyproject_path):
    # Read the new version from the version file
    with open(version_file_path, "r") as version_file:
        new_version = version_file.read().strip()

    # Read the pyproject.toml file
    with open(pyproject_path, "rb") as pyproject_file:
        pyproject_data = tomli.load(pyproject_file)

    # Update the version in the pyproject.toml data
    pyproject_data["project"]["version"] = new_version

    # Write the updated data back to pyproject.toml
    with open(pyproject_path, "wb") as pyproject_file:
        tomli_w.dump(pyproject_data, pyproject_file)

    print(f"Updated version in {pyproject_path} to {new_version}")


version_file_path = "version.txt"  # Path to the file containing the new version
pyproject_path = "pyproject.toml"  # Path to the pyproject.toml file

update_pyproject_version(version_file_path, pyproject_path)
