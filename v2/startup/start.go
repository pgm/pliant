package startup

import (
	"fmt"
	"os"

	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"github.com/pgm/pliant/v2/tagsvc"
)

func StartLocalService(rootServiceAddr string, authSecret string, cachePath string, PliantServiceAddress string, jsonBindAddr string) chan int {
	// contact the master and get the config
	tagsvcClient := tagsvc.NewClient(rootServiceAddr, []byte(authSecret))
	config, err := tagsvcClient.GetConfig()
	if err != nil {
		panic(err.Error())
	}

	if _, err := os.Stat(PliantServiceAddress); err == nil {
		os.Remove(PliantServiceAddress)
	}

	root := cachePath
	_, err = os.Stat(root)
	if os.IsNotExist(err) {
		os.MkdirAll(root, 0770)
	}

	db, err := v2.InitDb(root + "/db.bolt")
	if err != nil {
		panic(err.Error())
	}

	cache, _ := v2.NewFilesystemCacheDB(root, db)
	tags := tagsvc.NewTagService(tagsvcClient)
	chunkService := s3.NewS3ChunkService(config.AccessKeyId, config.SecretAccessKey, config.Endpoint, config.Bucket, config.Prefix, cache.AllocateTempFilename)
	chunks := v2.NewChunkCache(chunkService, cache)
	ds := v2.NewLeafDirService(chunks)
	as := v2.NewAtomicState(ds, chunks, cache, tags, v2.NewDbRootMap(db))
	fmt.Printf("bindAddr=%s\n", PliantServiceAddress)
	completed, err := v2.StartServer(PliantServiceAddress, jsonBindAddr, as)
	if err != nil {
		panic(err.Error())
	}
	return completed
}
