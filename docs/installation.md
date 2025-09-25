# **GWASStudio: Installation Guide**

This guide will walk you through the installation process.

---

## **Installation from source**

You can install **GWASStudio** using either the `make` command (recommended for simplicity) or manually. Choose the method that best fits your workflow.

---

### **1. Installation Using `make` (Recommended)**

The `make` method automates most of the installation steps, making it easier and less error-prone.

#### **Prerequisites**
- Ensure you have [`make`](https://www.gnu.org/software/make/) installed on your system.
- Ensure you have [`conda`](https://docs.conda.io/en/latest/) or [`miniconda`](https://docs.conda.io/en/latest/miniconda.html) installed for environment management.

#### **Steps**
**1. Clone the Repository**

Clone the repository to your local machine:
```shell
git clone https://github.com/ht-diva/gwasstudio.git
cd gwasstudio
```

**2. Create the Conda Environment**

Run the following command to create a dedicated conda environment for **GWASStudio**:

```sh
make create-env
```

This command reads the environment configuration from `base_environment.yml` and sets up a clean, isolated environment.

**3. Install Dependencies**

Install all required dependencies using:
```sh
make dependencies
```
This step ensures that all necessary Python packages and system libraries are installed.

**4. Install the Program**

Finally, install the program in your conda environment:
```sh
make install
```
This installs the program globally within your conda environment, making it available from the command line.

---

### **2. Manual Installation (Without `make`)**

If you prefer not to use `make`, you can install **GWASStudio** manually. This method gives you more control over each step.

#### **Prerequisites**
- Ensure you have [`conda`](https://docs.conda.io/en/latest/) or [`miniconda`](https://docs.conda.io/en/latest/miniconda.html) installed.

#### **Steps**
**1. Clone the Repository**

Clone the repository to your local machine:
```shell
git clone https://github.com/ht-diva/gwasstudio.git
cd gwasstudio
```

**1. Create the Conda Environment**

Create a new conda environment using the provided configuration file:
```sh
conda env create --file base_environment.yml
```
This command sets up a conda environment with all the base dependencies specified in `base_environment.yml`.

**2. Activate the Conda Environment**

Activate the environment with the following commands:
```sh
  conda activate gwasstudio
```

**4. Install the program**

Use `poetry` to install the project dependencies (excluding development dependencies) and the program:
```sh
  poetry install --without dev
```
This installs **gwasstudio** in your active conda environment.
