# External merge sort implementation using Seastar framework
* * *

## Build

```sh
# Clone Seastar repo, use the branch with merge sort implementation
git clone git@github.com:ManManson/seastar.git -b external_merge_sort

cd seastar
# Don't use "git submodule" since it breaks when used from forked repo
git clone git@github.com:scylladb/dpdk

# Configure seastar and the test apps
./cooking.sh

# Build external_merge_sort
ninja -C build external_merge_sort
```

## Generate test data

```sh
# random file composed of 1 million of 4K records
head -c 4096000000 </dev/urandom >/tmp/test_file
```

## Run

```sh
cd build

# also supports overriding temporary files directory with `--tmp` argument
apps/external_merge_sort --input /tmp/test_file --output /tmp/output_file
```
