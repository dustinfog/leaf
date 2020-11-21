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
	ChanCb    chan func()
	pendingGo int
}

type LinearContext struct {
	g        *Go
	linearGo *mpsc.Queue
	length   atomic.Int64
	running  atomic.Bool
}

func New(l int) *Go {
	g := new(Go)
	g.ChanCb = make(chan func(), l)
	return g
}

func (g *Go) Go(f func(), cb func()) {
	g.pendingGo++

	go g.run(f, cb)
}

func (g *Go) run(f func(), cb func()) {
	func() {
		defer func() {
			g.ChanCb <- g.wrapCb(cb)
			recoverPanic()
		}()

		f()
	}()
}

func (g *Go) wrapCb(cb func()) func() {
	return func() {
		defer func() {
			g.pendingGo--
			recoverPanic()
		}()

		if cb != nil {
			cb()
		}
	}
}

func (g *Go) Close() {
	for g.pendingGo > 0 {
		(<-g.ChanCb)()
	}
}

func (g *Go) Idle() bool {
	return g.pendingGo == 0
}

func (g *Go) NewLinearContext() *LinearContext {
	c := new(LinearContext)
	c.g = g
	c.linearGo = mpsc.New()
	return c
}

func (c *LinearContext) Go(f func(), cb func()) {
	c.g.pendingGo++

	c.linearGo.Push(func() { c.g.run(f, cb) })
	c.length.Inc()
	if !c.running.Swap(true) {
		go func() {
		process:
			for !c.linearGo.Empty() {
				r := c.linearGo.Pop().(func())
				r()
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
