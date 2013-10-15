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
	for e := range g.events {
		listeners := g.listeners
		for _, ln := range listeners {
			e.visit(ln)
		}
	}
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

type event interface {
	visit(ln Listener)
}

type startEvent struct {
	gossiper *Gossiper
}

func (e *startEvent) visit(ln Listener) {
	ln.OnStart(e.gossiper)
}

type stopEvent struct {
	gossiper *Gossiper
}

func (e *stopEvent) visit(ln Listener) {
	ln.OnStop(e.gossiper)
}

type joinEvent struct {
	peer *Peer
}

func (e *joinEvent) visit(ln Listener) {
	ln.OnJoin(e.peer)
}

type aliveEvent struct {
	peer *Peer
}

func (e *aliveEvent) visit(ln Listener) {
	ln.OnAlive(e.peer)
}

type deadEvent struct {
	peer *Peer
}

func (e *deadEvent) visit(ln Listener) {
	ln.OnDead(e.peer)
}

type restartEvent struct {
	peer *Peer
}

func (e *restartEvent) visit(ln Listener) {
	ln.OnRestart(e.peer)
}

type leaveEvent struct {
	peer *Peer
}

func (e leaveEvent) visit(ln Listener) {
	ln.OnLeave(e.peer)
}

type updateEvent struct {
	peer  *Peer
	key   string
	value string
}

func (e *updateEvent) visit(ln Listener) {
	ln.OnUpdate(e.peer, e.key, e.value)
}

type DefaultListener struct {
}

func (l *DefaultListener) OnStart(g *Gossiper)           {}
func (l *DefaultListener) OnStop(g *Gossiper)            {}
func (l *DefaultListener) OnJoin(p *Peer)                {}
func (l *DefaultListener) OnUpdate(p *Peer, k, v string) {}
func (l *DefaultListener) OnAlive(p *Peer)               {}
func (l *DefaultListener) OnDead(p *Peer)                {}
func (l *DefaultListener) OnRestart(p *Peer)             {}
func (l *DefaultListener) OnLeave(p *Peer)               {}
