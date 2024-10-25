# On Disk Spec

## Filesystem IO operation

- Golang WriteAt
    - https://pkg.go.dev/os#File.WriteAt
    - writes a certain number of bytes at a specified offset
- Golang ReadAt
    - https://pkg.go.dev/os#File.ReadAt
    - reads a certain number of bytes at a specified offset
- All serializations should be stored within one file
    - Each node serialization should include a data pointer to the merkle tree
    - No need for filesystem API
        - db is only one file so no need for overhead
        - potential use for mmap in future to optimize file I/O
