# Distributed File System

## Overview
This project implements a Distributed File System (DFS) using **MPI (Message Passing Interface)** for distributed storage, retrieval, searching, load balancing, and replication. The system consists of a **Master Node** and multiple **Storage Nodes**, facilitating efficient file distribution across different nodes.

## Features
- **File Uploading**: Splits files into 32-byte chunks and stores them in multiple storage nodes.
- **File Retrieval**: Assembles the stored chunks to reconstruct the original file.
- **File Searching**: Searches for words within a file, even if split across chunks.
- **Load Balancing**: Distributes chunks evenly across storage nodes.
- **Replication**: Each chunk is stored in **three different nodes** for fault tolerance.
- **Failover Handling**: Detects and manages node failures.
- **Recovery Handling**: Restores storage nodes after failures.
- **Heartbeat Monitoring**: Ensures active nodes are responsive.

## System Architecture
### Master Node
- Manages file storage and metadata.
- Tracks active storage nodes using heartbeats.
- Assigns least-occupied storage nodes for file uploads.
- Handles user commands for file operations.

### Storage Nodes
- Store file chunks.
- Respond to retrieval and search requests.
- Send heartbeat signals to indicate availability.
- Handle failure and recovery requests.

## Prerequisites
Ensure you have the following installed:
- Python 3
- `mpi4py` (MPI for Python)
- An MPI implementation (e.g., OpenMPI or MPICH)

### Installation
To install `mpi4py`, run:
```sh
pip install mpi4py
```

## Usage
### Running the Distributed File System
To start the system, run the following command:
```sh
mpirun -np <num_processes> python distributed_fs.py
```
- `<num_processes>` should be at least 2 (1 for the master node, the rest for storage nodes).

### Supported Commands
Run the following commands in the **Master Node**:

#### 1. Upload a File
```sh
upload <file_name> <absolute_file_path>
```
Uploads a file, splitting it into 32-byte chunks and distributing it across storage nodes.

#### 2. Retrieve a File
```sh
retrieve <file_name>
```
Reconstructs the original file from stored chunks.

#### 3. Search for a Word in a File
```sh
search <file_name> <word>
```
Finds the occurrences of a word in a file and returns offsets.

#### 4. List File Metadata
```sh
list_file <file_name>
```
Displays chunk locations for a file.

#### 5. Simulate Node Failure (Failover)
```sh
failover <rank>
```
Simulates a failure in the specified storage node.

#### 6. Recover a Node
```sh
recover <rank>
```
Restores a previously failed storage node.

#### 7. Exit the System
```sh
exit
```
Terminates all processes and shuts down the DFS.

## Example Usage
```sh
upload myfile.txt /home/user/documents/myfile.txt
retrieve myfile.txt
search myfile.txt keyword
list_file myfile.txt
failover 2
recover 2
exit
```

## Notes
- The system dynamically handles storage node failures.
- Ensure all storage nodes are running before uploading a file.
- File retrieval requires at least one replica of each chunk to be available.

## License
This project is licensed under the MIT License.


