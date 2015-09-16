package s3

import (
	"sync"
	"io"
)

type GetReq struct {
	url string
	writer io.WriteCloser
	progressChan chan *Progress
}

type Status int

const (
	INVALID Status = iota
	PENDING
	IN_PROGRESS
	FINISHED
	FAILED
)

type Progress struct {
	request *GetReq
	size uint64
	progress uint64
	status Status
	error error
}

type Client struct {
	lock sync.Mutex
	state map[*GetReq] *Progress

	workQueue chan *GetReq
	responseQueue chan *Progress
}

func workerLoop(workQueue chan *GetReq) {
	for {
		req := <- workQueue
		req.progressChan <- &Progress{request: req, size: -1, progress: -1, status: IN_PROGRESS, error: nil}
		//req.writer
		req.progressChan <- &Progress{request: req, size: -1, progress: -1, status: FINISHED, error: nil}
		req.Close()
	}
}

func (c *Client) Get(url string, writer io.WriteCloser) {
	req := &GetReq{url: url, progressChan: c.responseQueue, writer: writer}
	c.lock.Lock()
	c.state[req] = &Progress{request: req, size: -1, progress: -1, status: PENDING, error: nil}
	c.lock.Unlock()

	c.workQueue <- req
}

func (c *Client) GetState() []*Progress {
	c.lock.Lock()
	state := make([]*Progress, len(c.state))
	i := 0
	for _, v := range(c.state) {
		state[i] = v
	}
	c.lock.Unlock()
	return state
}

func (c *Client) ForgetAll() {
	c.lock.Lock()
	toDelete := make([]*GetReq, 0, len(c.state))
	for k, v := range(c.state) {
		if v.status == FAILED || v.status.FINISHED {
			toDelete = append(toDelete, k)
		}
	}
	for k := range toDelete {
		delete(c.state, k)
	}
	c.lock.Unlock()
}
