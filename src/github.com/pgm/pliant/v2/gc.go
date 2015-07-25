package v2

type Color int
const (
	UNDEFINED Color = itoa
	WHITE
	GRAY
	BLACK
)

type Coloring struct {
	keyColor map[Key] int
}

func (c *Coloring) mark(key *Key, color Color) {
	c.keyColor[*key] = color
}

func (c *Coloring) get(key *Key) Color {
	color, present := c.keyColor[*key]
	if ! present {
		return WHITE
	}
	return color
}

func (c *Coloring) pickGray() *Key {
	panic("unimp")
}

func colorKeys (roots []*Key, chunks ChunkService, dirService DirectoryService) *Coloring {
	c := &Coloring{keyColor: make(map[Key] int)}
	for root := range(roots) {
		c.mark(root, GRAY)
	}

	for {
		next := c.pickGray()
		if next == nil {
			break
		}

		if *next == *EMPTY_DIR_KEY {
			continue
		}

		entry := chunks.Get(next)
		if entry == nil {
			panic("Could not find cache entry for "+next.String())
		}

		// now record all the keys that this references
		dir := dirService.GetDirectory(next)
		it := dir.Iterate()
		for it.HasNext() {
			_, meta := it.Next()
			child := KeyFromBytes(meta.GetKey())
			if meta.GetIsDir() {
				if c.get(child) == WHITE {
					c.mark(child, GRAY)
				}
			} else {
				c.mark(child.BLACK)
			}
		}
		c.mark(next, BLACK)
	}
}

// if set label is called in the middle of coloring, and ref is white, mark ref as gray
// color, then walk through all keys on remote.   Assert all keys are white or black.  If white, delete

func free(coloring *Coloring, chunks ChunkService) {
	it := chunks.Iterate()
	for it.HasNext() {
		key := it.Next()

		color := coloring.get(key)
		if color == WHITE {
			chunks.Delete(key)
		} else if(color != BLACK) {
			panic("Key was neither black nor white")
		}
	}
}


func gc(roots []*Key, chunks ChunkService) {
	dirService := NewLeafDirService(chunks)
	coloring := colorKeys (roots, chunks, dirService)
	free(coloring, chunks)
}
