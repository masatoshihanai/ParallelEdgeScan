# Parallel Edge Scan
There are two implementations.
- Cache optimized implementation ([CacheOptimizedParallelEdgeScan.cpp](./CacheOptimizedParallelEdgeScan.cpp))
- Naive implementation ([NaiveParallelEdgeScan.cpp](./NaiveParallelEdgeScan.cpp))

## Requirement
- `C++11` compiler
- `pthread` library

## Build
Using Intel compiler
```
$ icc -O2 -pthread -std=c++11 CacheOptimizedParallelEdgeScan.cpp -o CacheOptimizedParallelEdgeScan
```
Using GCC compiler
```
$ g++ -O2 -pthread -std=c++11 CacheOptimizedParallelEdgeScan.cpp -o CacheOptimizedParallelEdgeScan
```
## Run
```
$ ./CacheOptimizedParallelEdgeScan {# of threads} {Edge file} {Meta data file}
```
