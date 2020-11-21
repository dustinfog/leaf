package g

import (
	"github.com/name5566/leaf/conf"
	"github.com/name5566/leaf/log"
	"github.com/name5566/leaf/util/mpsc"
	"go.uber.org/atomic"
	"runtime"
)

// one Go per goroutine (goroutine not safe)
type Go struct {
	ChanCb  chan func()
	pending map[*Task]*Task
}

type Task struct {
	f  func()
	cb func()
}

type LinearContext struct {
	g       *Go
	tasks   *mpsc.Queue
	length  atomic.Int64
	running atomic.Bool
}

func New(l int) *Go {
	g := new(Go)
	g.ChanCb = make(chan func(), l)
	return g
}

func (g *Go) Go(f func(), cb func()) {
	t := &Task{f, cb}
	g.pending[t] = t

	go g.run(t)
}

func (g *Go) run(t *Task) {
	func() {
		defer func() {
			g.ChanCb <- g.wrapCb(t)
			recoverPanic()
		}()

		t.f()
	}()
}

func (g *Go) wrapCb(t *Task) func() {
	return func() {
		defer func() {
			delete(g.pending, t)
			recoverPanic()
		}()

		if t.cb != nil {
			t.cb()
		}
	}
}

func (g *Go) Close() {
	for len(g.pending) > 0 {
		(<-g.ChanCb)()
	}
}

func (g *Go) Idle() bool {
	return len(g.pending) == 0
}

func (g *Go) NewLinearContext() *LinearContext {
	c := new(LinearContext)
	c.g = g
	c.tasks = mpsc.New()
	return c
}

func (c *LinearContext) Go(f func(), cb func()) {
	t := &Task{f, cb}
	c.g.pending[t] = t

	c.tasks.Push(t)
	c.length.Inc()
	if !c.running.Swap(true) {
		go func() {
		process:
			for !c.tasks.Empty() {
				t := c.tasks.Pop().(*Task)
				c.g.run(t)
				c.length.Dec()
			}

			c.running.Store(false)
			if c.length.Load() > 0 && !c.running.Swap(true) {
				goto process
			}
		}()
	}
}

func recoverPanic() {
	if r := recover(); r != nil {
		if conf.LenStackBuf > 0 {
			buf := make([]byte, conf.LenStackBuf)
			l := runtime.Stack(buf, false)
			log.Error("%v: %s", r, buf[:l])
		} else {
			log.Error("%v", r)
		}
	}
}
