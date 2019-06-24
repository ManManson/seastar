# Task implementation analysis
* * *

This document aims to provide a thorough analysis of what needs to be improved or fixed in the current implementation of the task.

## Implementation overview

This external sorting algorithm implementation consists of two main phases:

1. Partitioning of the input file into `N` (almost) equal-sized partitions (also called "runs"), each sorted lexicographical (ascending order). Let `S` be the initial partition size (and the memory limit available to each core for reading input data). The total partition count would be `T = ceil(InputSize / S)`.
2. K-way merge sort (in this case `K` being equal to `seastar::smp::count - 1`). This phase step by step merges each `K` partitions from level `j` into a larger sorted partition assigned to the level `j + 1`. The algorithm goes through all available partitions on each level to produce enlarged partitions again and again until only one partition remains. This partition is the end goal of the algorithm.

Let's elaborate on the first phase implementation details.

### First phase: initial runs creation

Each core (at least on the machine used for development and testing) has `1GB` of free memory, as reported by `seastar::memory::stats().free_memory`. We occupy `0.8` fraction of this memory size (just to be sure that we don't over-allocate above this limit while sorting and using temporary output buffers). Let it be `S`. Note that `S` needs to be aligned to be a multiple of record size, which is `4096` bytes.

Initial run creation is handled by the `seastar::sharded<InitialRunService>` class. Use of `sharded` means that `InitialRunService` instances would be distributed among all available CPU cores (vCPUs), each of them at first would calculate total partitions count (`== ceil(InputSize / S)`). Rounding up is required since the very last partition probably would be smaller than the others.

Each shard (occupying the `i`-th core), creates all partitions with indices `i_k`, that are multiples of `seastar::smp::count + i`, by reading input file from the offset `i_k * S` with the block size of `S` bytes at most. With this approach, the shards compute and create a non-overlapping set of runs independently of each other.

During iteration `i_k`, internal data sorting is being done on `i`-th core via `std::sort` using `std::lexicographical_compare` comparator. It should be noted that the sorting procedure operates on pointers instead of moving around `4K` blocks of data, which avoids expensive data copies. `std::vector`, which is used as a container for sorted block descriptors, is preallocated by calling `.reserve()` with the number of records that would fit into a single partition (`== S / 4096` bytes).

A temporary buffer of size `32MB` is used for writing to the run file. It is filled by sequentially walking through the vector obtained from the previous step and copying records in the sorted order to the buffer. When the buffer becomes filled, it would be flushed to the output file. The process is repeated until the pointer vector is exhausted and all the data is written back to the output file. This approach aims to solve the following problems:

1. Don't consume too much CPU time while writing the data to the output buffer to avoid reactor stalls.
2. Don't allocate too much extra memory on `i`-th core.

All `InitialRunService` instances start in parallel and process corresponding run ranges independently of each other. The second phase of the algorithm waits for all shards to complete their work by waiting on the corresponding `.invoke_on_all` future.

### Second phase: K-way merging

This solution uses the most traditional (and the most convenient in terms of debugging) variant of K-way merge sorting algorithm, which makes use of temporary files to store each merged partition:

1. `N_j` temporary files (merge runs) are created on `j`-th iteration (level) of the algorithm.
2. These files are then merged into `N_{j+1} == N_j / K` enlarged runs on the `j + 1`-th level.
3. Files that make up the parts of the output run are removed once exhausted (all the data is read and already processed).

`K` constant is selected to be equal to `seastar::smp::count - 1` to make all cores to be readers except `0`-th, which does the merge and outputs to the new run. The data from each core is passed via pointers to avoid unnecessary copying.

Reader cores are coordinated by the `0`-th core by issuing `.invoke_on(i, ...)` calls for each reader shard.

Local sorting is done on `0`-th core by fetching `DataFragment`s (which store a pointer to the data on `i`-th core and an integer representing the size of a data block).

The sorting is implemented via `std::priority_queue` which is a `max heap` structure adaptor on top of `std::vector` container. This time we should use inverted `std::lexicographical_compare` comparator to turn `max heap` into `min heap`.

One of the main advantages of using the `std::priority_queue` is that it allows insertion and min element retrieval to have at most logarithmic complexity.

`priority_queue` is populated by sequentially iterating 'DataFragments' obtained from each reader shard. After data from all shards is placed into 'priority_queue', it's written to a temporary output buffer of size '32MB', which is flushed to the output file every time the buffer gets filled.

Buffer size is made small for the same reasons as in the first phase:

1. Don't use too much CPU time, so we could visit the reactor event loop at some points to make sure we don't get reactor stall reports.
2. Don't reserve too much memory (actually, this one is arguable, because `priority_queue` stores only pointers and not the actual data and is kept relatively small).

Manual calls to `seastar::thread::yield` are made after all the data is written to the file to briefly return to the reactor event loop and continue further.

After all data fragments are processed and the data is written to the file completely, successive data chunks are read from the input runs and the process is continued until all partitions are exhausted. After that temporary files from the previous level, that participated in the creation of a new run, are removed.

Outer loop assigns `K` runs at once that would be passed to the procedure mentioned above to merge them into one run. This loop continues until all runs from the previous level are processed.

It should be noted that the last outer loop iteration is likely to consume less than `K` runs. That means that the last iteration would leave some of the cores without work and lead to uneven workload distribution between cores.

## Drawbacks of the current implementation

There we again group the issues by the algorithm phase.

### First phase.

1. Internal sorting uses `std::sort` which has `O[N * log[N]]` time complexity at average. Some algorithms have linear time complexity, such as, `radix sort`, that takes up to `O[N * W]` operations at average, where `W` is constant (`== 4096`) and `N` is record count.
2. The next chunk of data is not read from the drive until the former is completely processed, sorted and written to the output file. Here we could split one big temporary buffer into two smaller buffers and just after the first buffer is filled with data and being sorted, another buffer is read in parallel. This way after we have sorted and written the first chunk to the file, the next one is ready (just need to swap the buffers). This would help to distribute CPU and IO workload more evenly.

### Second phase.

1. Local sorting could be replaced with `tournament trees` and `replacement selection` instead of using `min heap`. Although both of them have `O[log[N]]` asymptotic complexity for basic operations, tournament trees use less comparisons on each step, so they should be generally more performing than using a heap structure. Note: tournament trees and replacement selection algorithm also could be generalized to the first stage of the algorithm. See `Donald E. Knuth "The Art of Computer Programming: Volume 3: Sorting and Searching", Chapter 5.4.1 "Multiway merging and Replacement Selection"`
2. Analogous to the first phase, we could make use of the second buffer in `RunReaderService` instances which will be filled asynchronously to distribute IO workload better.
3. In a case where a machine has sufficiently high core count (for example, `>= 32`) K-way merging with `K == 31` is inadvisable. Most of the cores would have no work most of the time. One proposed solution would be to divide all vCPUs into equal-sized blocks (8 cores in each). Each block does 7-way merge. With this approach, many runs can be created in parallel which would speed up the overall algorithm execution substantially. For more precise `K` constant choice it can be convenient to use `io_tester` utility and take into account the IO performance of the disk subsystem.
4. Exhausted partitions from the previous level are deleted after the new run is complete. If bullet 2 of this list is implemented properly, we could just issue a continuation chain that would check if there is more data available in the buffer that is filled ahead-of-time, and unlink the file instantly if there is no more data available.
5. `fetch_data` is done sequentially on each shard. It makes sense to issue this command in parallel on all reader shards.
6. When the last iteration of the outer loop has less than `K` runs, divide them into smaller pieces to make exactly `K` runs. This way there wouldn't be starving shards on the last iteration.

### Some more generic issues

This implementation extensively uses temporary files to store initial runs and intermediate merge runs. Worst-case space overhead, in this case, is `InputSize * 2` (on the last merge pass we have one last output partition that would be of `InputSize` size and `K` partitions which have a total size of `InputSize` bytes too).
We could avoid creating temporary files and use rewriting output file `in-place`.

Exception handling is poor, at least it should be checked that all reads and writes are secured in terms of error reporting.

The code uses `seastar::async` paired with a series of `.wait()` at some places. It potentially limits the code parallelization degree. Also, it forces to manually invoke `seastar::thread::yield` to yield to reactor event loop for a while. So, all `while` and `for` (plus `seastar::async`) uses should be replaced with Seastar primitives like `seastar::do_until/seastar::do_for_each`. Explicit `wait()`s should be replaced with continuation chains.
