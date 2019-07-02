# Task implementation analysis
* * *

This document aims to provide a thorough analysis of what needs to be improved or fixed in the current implementation of the task.

## Implementation overview

This external sorting algorithm implementation consists of two main phases:

1. Partitioning of the input file into `N` (almost) equal-sized partitions (also called "runs"), each sorted lexicographical (ascending order). Let `S` be the initial partition size (and the memory limit available to each core for reading input data). The total partition count would be `T = ceil(InputSize / S)`.
2. K-way merge sort. This phase step by step merges each `K` partitions from level `j` into a larger sorted partition assigned to the level `j + 1`. The algorithm goes through all available partitions on each level to produce enlarged partitions again and again until only one partition remains. The last remaining partition is the end goal of the algorithm. `K` constant selection considerations will be discussed in detail below.

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
3. Files that make up the parts of the output run are removed once exhausted (*i.e.* all the data is read and already processed).

Abstract outline of the algorithm is as follows: 
```c++
K = <notes on selection below>;

lvl = 1
prev_lvl_run_count = initial_runs_count
current_cpu_id = 0
current_run_id = 0

while(prev_lvl_run_count > 1) {
    while(have more runs from prev lvl to process) {
        span = get at most K ids for the runs to merge
        submit "merge pass" for `current_run_id`-th run on core with id: `current_cpu_id`
        current_cpu_id = (current_cpu_id + 1) % seastar::smp::count
        ++current_run_id;
    }
    wait for all merge passes to finish
    ++lvl
    update prev_lvl_run_count correspondingly
}

rename last generated temporary partition into final output file name
```

`K` constant is selected as follows:

```
Each merge pass manages `K` run readers. We are bounded by `per_cpu_memory` limit per core.
So each reader should occupy at most `per_cpu_memory / K` fraction of the shard memory.

On the one hand we want to minimize overall levels count.
On the other hand we don't want to have *too* small buffers and an enormous count of readers per merge pass.

So by default we put `K = <number of initial runs>` but also check if the buffers
for the individual readers are not getting small (define this threshold as 4MiB).
```

Each core executes a single merge pass operation which merges K files from the previous level into the single larger file. 

Merge pass algorithm outline can be illustrated as follows:
```c++
`open output file descriptor`
  .then([...]{
      `create K run readers, then fetch data for each reader`
        .then([...]{
            `for each reader fill priority_queue with pairs
              of kind <first record from reader buf, reader_ptr>`
                .then([...]{
                   do_until(readers.count() <= 1) {
                      `take min record from the queue, replace with new one
                        from the reader which had previous min record;
                        flush to the temporary buffer and
                        accidentially write to the output file`
                    
                      `if the reader is empty, remove it from processing list
                        and remove the run file`
                    }
                })
                .then([...] {
                  `write back to the output file what is left in the last reader`  
                })
      })
  })
```

The sorting is implemented via `std::priority_queue` which is a `max heap` structure adaptor on top of `std::vector` container. This time we should use inverted `std::lexicographical_compare` comparator to turn `max heap` into `min heap`.

One of the main advantages of using the `std::priority_queue` is that it allows insertion and min element retrieval to have at most logarithmic complexity.

The data is at first being written to a temporary output buffer of size `32MB`, which is flushed to the output file every time the buffer gets filled.

Buffer size is made small for the same reasons as in the first phase:

1. Don't use too much CPU time, so we could visit the reactor event loop at some points to make sure we don't get reactor stall reports.
2. Don't reserve too much memory (actually, this one is arguable, because `priority_queue` stores only pointers and not the actual data and is kept relatively small).

Manual calls to `seastar::thread::yield` are made after all the data is written to the file to briefly return to the reactor event loop and continue further.

## Drawbacks of the current implementation

There we again group the issues by the algorithm phase.

### First phase.

1. Internal sorting uses `std::sort` which has `O[N * log[N]]` time complexity at average. Some algorithms have linear time complexity, such as, `radix sort`, that takes up to `O[N * W]` operations at average, where `W` is constant (`== 4096`) and `N` is record count.
2. The next chunk of data is not read from the drive until the former is completely processed, sorted and written to the output file. Here we could split one big temporary buffer into two smaller buffers and just after the first buffer is filled with data and being sorted, another buffer is read in parallel. This way after we have sorted and written the first chunk to the file, the next one is ready (just need to swap the buffers). This would help to distribute CPU and IO workload more evenly.

### Second phase.

1. Local sorting could be replaced with `tournament trees` and `replacement selection` instead of using `min heap`. Although both of them have `O[log[N]]` asymptotic complexity for basic operations, tournament trees use less comparisons on each step, so they should be generally more performing than using a heap structure. Note: tournament trees and replacement selection algorithm also could be generalized to the first stage of the algorithm. See `Donald E. Knuth "The Art of Computer Programming: Volume 3: Sorting and Searching", Chapter 5.4.1 "Multiway merging and Replacement Selection"`
2. Analogous to the first phase, we could make use of the second buffer in `RunReader` instances which will be filled asynchronously to distribute IO workload better.
3. `K` constant selection is far from being optimal. It should be evaulated carefully whether it should be large enough to minimize runs and levels count (but vCPUs utilization will suffer) or it should be adjusted to be small enough to keep every core busy with even distribution of workload between the cores. It also greatly depends on filesystem and disk subsystem hardware characteristics.
4. `fetch_data` is done sequentially on each shard. It makes sense to issue this command in parallel on all reader shards after readers will be created.

### Some more generic issues

* This implementation extensively uses temporary files to store initial runs and intermediate merge runs. Worst-case space overhead, in this case, is `InputSize * 2` (on the last merge pass we have one last output partition that would be of `InputSize` size and `K` partitions which have a total size of `InputSize` bytes too).
We could avoid creating temporary files and use rewriting output file `in-place`.

* Exception handling is poor, at least it should be checked that all reads and writes are secured in terms of error reporting.

* The code uses `seastar::async` paired with a series of `.wait()` at some places. It potentially limits the code parallelization degree. Also, it forces to manually invoke `seastar::thread::yield` to yield to reactor event loop for a while. So, all `while` and `for` (plus `seastar::async`) uses should be replaced with Seastar primitives like `seastar::do_until/seastar::do_for_each`. Explicit `wait()`s should be replaced with continuation chains.

* It would be nice to have CMake unit tests for the solution.
