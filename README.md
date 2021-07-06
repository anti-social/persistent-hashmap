[![Java CI](https://github.com/anti-social/persistent-hashmap/actions/workflows/java.yaml/badge.svg)](https://github.com/anti-social/persistent-hashmap/actions/workflows/java.yaml)

# Persistent hashmap

A hash map implementation that stores data on disk. The hash map can be shared by multiple processes. 
The only requirement: there can be a single writer. So it is possible to use it for interprocess communication.

Only primitive types are supported: `int` and `long` for keys; `short`, `int`, `long`, `float` and `double` for values.

At the moment there is no atomicity for multiple operations.
The only garantee is that every single operation (`put`, `get`, `remove`) is atomic.
