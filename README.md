# PimPam: Graph Pattern Matching on UPMEM DPU

**PimPam** is a functional implementation of graph pattern matching algorithms leveraging the UPMEM Processing-in-Memory (PIM) architecture.

## Prerequisites & Preparation

### 1. UPMEM SDK Environment
The UPMEM SDK must be downloaded and installed. Activate the environment by sourcing the script (please adjust the path to match your installation):

```
# Example: sourcing SDK version 2025.1.0
source ./upmem-2025.1.0-Linux-x86_64/upmem_env.sh
```
### 2. System Dependencies
This project relies on `libnuma` and specific Python libraries required by the DPU backend.

For Ubuntu/Debian:

```
# Install NUMA support
sudo apt-get update
sudo apt-get install libnuma-dev

# Install Python 3.8 development libraries (Required for UPMEM SDK compatibility)
# Note: Newer Ubuntu versions (22.04+) need the deadsnakes PPA.
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install libpython3.8-dev
```
### 3. Data Preparation
Prepare the binary graph data file. Place your dataset in the data/ directory. The file path should follow this format: ./data/${DATA_NAME}.bin.

## Usage
To run the pattern matching test, use make test with the specified graph and pattern parameters.

Syntax:
```
GRAPH=<graph_name> PATTERN=<pattern_name> make test
```
Example: Run a "Clique-3" pattern search on the "CA" graph dataset:
```
GRAPH=CA PATTERN=CLIQUE3 make test
```
## Contact
For any questions or issues, please contact: **Yen-Chu Lo** (yenchulo818@gmail.com)