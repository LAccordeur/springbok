Single object get (average of 500 times from local requests)
64KB 36.31462925851704ms
128KB 51.833667334669336ms
256KB 58.96192384769539ms
512KB 106.0120240480962ms
1MB 181.5130260521042ms

16 threads for 64KB x 16 212 ms
8 threads for 64KB x 16 294.03 ms
8 threads for 128KB x 8 163.224 ms
4 threads for 128KB x 8 253.076 ms
4 threads for 256KB x 4 236.608 ms
2 threads for 256KB x 4 225.196 ms
2 threads for 512KB x 2 229.22 ms
1 threads for 512KB x 2 239.178 ms
1 threads for 1MB x 1 207.894 ms

- How to organize blocks in S3?
    - given a file of 64 MB, how to put it into S3, then we can fetch all its data in an efficient and cost-effective way
      - two extra cases
         - 1 read threads for whole files
         - multiple read threads (each read for each block)