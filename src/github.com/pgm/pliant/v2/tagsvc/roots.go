package tagsvc

import (
	"container/heap"
	"fmt"
	"github.com/pgm/pliant/v2"
	"sync"
)

type Color int

const (
	UNDEFINED Color = iota
	WHITE
	GRAY
	BLACK
)

type Roots struct {
	lock sync.Mutex

	// all named roots
	labels map[string]*v2.Key

	// all anonymous roots with a time-to-live.  After which they expire
	leases Leases

	log *Log

	// the GC current state
	coloring *Coloring
}

func NewRoots(logName string) *Roots {
	leases := Leases(make([]KeyLease, 0))
	labels := make(map[string]*v2.Key)

	log := OpenLog(logName, func(label string, key *v2.Key) {
		labels[label] = key
	},
		func(key *v2.Key, timestamp uint64) {
			leases = append(leases, KeyLease{timestamp: timestamp, key: key})
		})

	roots := &Roots{
		log:      log,
		labels:   labels,
		leases:   leases,
		coloring: &Coloring{gray: make(map[v2.Key]int), black: make(map[v2.Key]int)}}
	heap.Init(&roots.leases)
	//fmt.Printf("%s", roots)
	return roots
}

func (r *Roots) Set(label string, key *v2.Key) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if key == nil {
		delete(r.labels, label)
	} else {
		r.labels[label] = key
		r.coloring.mark(key, GRAY)
	}

	r.log.appendLabel(label, key)
}

func (r *Roots) Get(label string) *v2.Key {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.labels[label]
}

func (r *Roots) AddLease(expiry uint64, key *v2.Key) {
	r.lock.Lock()
	defer r.lock.Unlock()

	heap.Push(&r.leases, KeyLease{expiry, key})
	r.coloring.mark(key, GRAY)

	r.log.appendLease(key, expiry)
}

// Find all leases which have expired, remove them and return the list of the removed
func (r *Roots) Expire(oldestToKeep uint64) []*v2.Key {
	r.lock.Lock()
	defer r.lock.Unlock()

	l := r.leases
	expired := make([]*v2.Key, 0, 10)
	for len(l) > 0 && l.Peek().timestamp < oldestToKeep {
		kl := heap.Pop(&l).(KeyLease)
		next := kl.key
		expired = append(expired, next)
	}
	r.leases = l
	return expired
}

// return a snapshot of all root keeps to be used for reachability analysis
func (r *Roots) GetNamedRoots() []NameAndKey {
	r.lock.Lock()
	defer r.lock.Unlock()

	roots := make([]NameAndKey, 0, len(r.leases)+len(r.labels))
	l := r.leases
	for i := 0; i < len(l); i++ {
		roots = append(roots, NameAndKey{"", r.leases[i].key})
	}
	for name, key := range r.labels {
		roots = append(roots, NameAndKey{name, key})
	}
	return roots
}

func (r *Roots) GetRoots() []*v2.Key {
	namedRoots := r.GetNamedRoots()
	result := make([]*v2.Key, len(namedRoots))
	for i, nr := range namedRoots {
		result[i] = nr.Key
	}
	return result
}

func (r *Roots) GC(dirService v2.DirectoryService, chunks v2.IterableChunkService, freeCallback FreeCallback) {
	roots := r.GetRoots()
	r.coloring.colorKeys(roots, chunks, dirService)
	r.coloring.freeWhiteKeys(chunks, freeCallback)
}

type KeyLease struct {
	timestamp uint64
	key       *v2.Key
}

type Leases []KeyLease

func (l Leases) Len() int           { return len(l) }
func (l Leases) Less(i, j int) bool { return l[i].timestamp < l[j].timestamp }
func (l Leases) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *Leases) Push(x interface{}) {
	*l = append(*l, x.(KeyLease))
}

func (l *Leases) Pop() interface{} {
	old := *l
	n := len(old)
	x := old[n-1]
	*l = old[0 : n-1]
	return x
}

func (l *Leases) Peek() KeyLease {
	return (*l)[0]
}

type Coloring struct {
	lock sync.Mutex

	gray  map[v2.Key]int
	black map[v2.Key]int
}

func (c *Coloring) mark(key *v2.Key, color Color) {
	c.lock.Lock()
	defer c.lock.Unlock()

	//fmt.Printf("Mark %s %s\n", key.String(), color)

	if color == GRAY {
		_, isBlack := c.black[*key]
		if !isBlack {
			c.gray[*key] = 1
		}
	} else if color == BLACK {
		delete(c.gray, *key)
		c.black[*key] = 1
	} else {
		panic("invalid color")
	}
}

func (c *Coloring) get(key *v2.Key) Color {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, isGray := c.gray[*key]
	_, isBlack := c.black[*key]
	if isGray {
		return GRAY
	}
	if isBlack {
		return BLACK
	}
	return WHITE
}

func (c *Coloring) pickGray() *v2.Key {
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, _ := range c.gray {
		return &key
	}

	return nil
}

func (c *Coloring) reset() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.gray = make(map[v2.Key]int)
	c.black = make(map[v2.Key]int)
}

func (c *Coloring) colorKeys(roots []*v2.Key, chunks v2.ChunkService, dirService v2.DirectoryService) {
	fmt.Printf("colorKeys reset")
	c.reset()

	fmt.Printf("colorKeys gray")
	for _, root := range roots {
		c.mark(root, GRAY)
	}

	c.mark(v2.EMPTY_DIR_KEY, BLACK)

	for {
		next := c.pickGray()
		if next == nil {
			break
		}

		fmt.Printf("pick gray %s\n", next.String())

		entry := chunks.Get(next)
		if entry == nil {
			panic("Could not find cache entry for " + next.String())
		}

		// now record all the keys that this references
		dir := dirService.GetDirectory(next)
		it := dir.Iterate()
		for it.HasNext() {
			_, meta := it.Next()
			child := v2.KeyFromBytes(meta.GetKey())
			if meta.GetIsDir() {
				if c.get(child) == WHITE {
					c.mark(child, GRAY)
				}
			} else {
				c.mark(child, BLACK)
			}
		}
		c.mark(next, BLACK)
	}
}

type FreeCallback func(key *v2.Key)

// if set label is called in the middle of coloring, and ref is white, mark ref as gray
// color, then walk through all keys on remote.   Assert all keys are white or black.  If white, delete
// TODO: chunks Iterate needs to be filtered to only return chunks which were created a while ago.
// There will be a window of time between when the first chunk is uploaded and a root pointer is updated to point to the newly uploaded
// root node.  As a result, any GC in that window would think the chunks were unused and free them prematurely.
// So, we should have an estimate of that window max duration and not free anything that could have been created within that window.
// A more advanced strategy would be to have the client report an expected window which it resized as time elapses, and then deletes when the upload is
// done.   The expected window would timeout if the client never finishes.   That would avoid having to pick a global upload timeout.
func (coloring *Coloring) freeWhiteKeys(chunks v2.IterableChunkService, freeCallback FreeCallback) {
	it := chunks.Iterate()
	for it.HasNext() {
		key := it.Next()
		fmt.Printf("next key %s\n", key.String())

		color := coloring.get(key)
		if color == WHITE {
			// TODO: check created time and skip if key is new
			freeCallback(key)
		} else if color != BLACK {
			panic("Key was neither black nor white")
		}
	}
}

//////////////////////////
