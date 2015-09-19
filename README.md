# pliant

Pliant is an experiment in creating a service to help bridge software which assumes data is accessible on the filesystem and remote object stores such as S3 and GCS.

The design choices are targeting examples with:
  - Write once batch processing (input files consumed and new files writen)
  - Read heavy workloads
  - Large scale, parallel, share-nothing workloads

The core ideas are:
  - The "pliant" service mirrors objects from S3 locally in a managed arena.  All read-only processing can use the version from there.
  - Files are atomically added only after they are fully written.  There is no mechanism for updating.
  - Files are only uploaded upon a "push" operation.  At which time all files not yet stored in S3 are uploaded.
  - Files are lazily downloaded on demand and kept in the arena until the max quota is exceeded.
  - Metadata about files is similarly stored in immutable btree blocks.  Any modifications to data or metadata results in copy-on-write logic.
  - This makes snapshots trivial as knowing the key of the root block directory node.  
  - This also makes coping with eventual consistency trivial.  Objects are never deleted, so knowing a block address implies the data will eventually appear at that address.  
  - Garbage collection is performed to reclaim space in the object store as well as the local cache arena

The "pliant" command can be used to perform operations such as "push" and "pull".   There is also a FUSE client which makes the files stored in pliant visible as a filesystem.
