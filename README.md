# pliant

TODO:
	add goroutine to StartMaster to clears expired leases
	add persistence of Leases and roots to Master (maybe as an append log for now?)
	add cmd start master proc (StartMaster)
	add method to query for all keys/leases

	add to AtomicState:
	    local GC
	    registration and renewal of leased keys (Maybe GC should determine lease list)
	    persistence of local cache state


    Add creation leases (has effect of treating any keys created in the window as "marked")
	Create(timeout) -> lease_id (Or should it use root block id?)
	Renew(lease_id, timeout)  (Timeout = 0 to clear lease)

	Change upload to create lease upon start
	Periodically renew lease while upload is ongoing
	Cancel lease after upload is complete.  Only if all renews are successful, is upload considered success.   (Should we avoid setting blocks in local cache as "in remote" until successful upload?)

    Add persistence of labels and leases.  (And recovery to load them on startup)
    Change minion to use master for label service.
    Collect stats during GC
    Collect stats during push
    Collect stats during pull
    Report more metadata in ls
    Fuzzying test client

    Improve error handling
    Add fuse client



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


