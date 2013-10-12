package gossip

func (g *Gossiper) AddListener(ln Listener) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.listeners = append(g.listeners, ln)
}

func (g *Gossiper) RemoveListener(ln Listener) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	listeners := make([]Listener, 0)
	for _, v := range g.listeners {
		if v == ln {
			continue
		} else {
			listeners = append(listeners, v)
		}
	}
	g.listeners = listeners
}

func (g *Gossiper) handleEvent() {
	listeners := g.listeners
	for v := range g.events {
		switch e := v.(type) {
		case *startEvent:
			for _, ln := range listeners {
				ln.OnStart(g)
			}
		case *stopEvent:
			for _, ln := range listeners {
				ln.OnStop(g)
			}
		case *joinEvent:
			for _, ln := range listeners {
				ln.OnJoin(e.peer)
			}
		case *updateEvent:
			for _, ln := range listeners {
				ln.OnUpdate(e.peer, e.key, e.value)
			}
		case *aliveEvent:
			for _, ln := range listeners {
				ln.OnAlive(e.peer)
			}
		case *deadEvent:
			for _, ln := range listeners {
				ln.OnDead(e.peer)
			}
		case *restartEvent:
			for _, ln := range listeners {
				ln.OnRestart(e.peer)
			}
		case *leaveEvent:
			for _, ln := range listeners {
				ln.OnLeave(e.peer)
			}
		}
	}
}

type startEvent struct {
}

type stopEvent struct {
}

type joinEvent struct {
	peer *Peer
}

type aliveEvent struct {
	peer *Peer
}

type deadEvent struct {
	peer *Peer
}

type restartEvent struct {
	peer *Peer
}

type leaveEvent struct {
	peer *Peer
}

type updateEvent struct {
	peer  *Peer
	key   string
	value string
}

type Listener interface {
	OnStart(g *Gossiper)
	OnStop(g *Gossiper)
	OnJoin(p *Peer)
	OnUpdate(p *Peer, k, v string)
	OnAlive(p *Peer)
	OnDead(p *Peer)
	OnRestart(p *Peer)
	OnLeave(p *Peer)
}

type NoOpListener struct {
}

func (l *NoOpListener) OnStart(g *Gossiper)           {}
func (l *NoOpListener) OnStop(g *Gossiper)            {}
func (l *NoOpListener) OnJoin(p *Peer)                {}
func (l *NoOpListener) OnUpdate(p *Peer, k, v string) {}
func (l *NoOpListener) OnAlive(p *Peer)               {}
func (l *NoOpListener) OnDead(p *Peer)                {}
func (l *NoOpListener) OnRestart(p *Peer)             {}
func (l *NoOpListener) OnLeave(p *Peer)               {}
