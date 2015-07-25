# pliant

TODO:
    Finish garbage collector
        Assuming world is stopped
    Improve error handling
    Add fuse client
    Garbage collector extension:
        Tricolor collector with concurrent labeling

----------------


Time to switch to a B-Tree for directories:

min block size
max block size

branch:
element_count
list of (is_leaf,hash) # name-range

leaf:
element_count
list of (name,hash,flags,timestamp,length)

# copy on write
Insert(tree, name, value) -> new root, list of new blocks
# may require splitting leaf.  If leaf > max block, split leaf in half and replace entry in parent.   If parent branch > max block size, split branch

Delete(tree, name)
# may require merging branches/leaves
if after delete, leaf < min block size, merge with adjacent entry.   If after merge, leaf > max block, split in half

Walk(tree)
Get(tree, name) -> entries

kv Get(key) -> value
(if key does not exist, hard error)
kv Put(key, value)
(if key exists, no-op or error)

TreeOp = {settings: max block, min block}


