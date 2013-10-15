package gossip

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	. "github.com/hujh/gossip/message"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	interval time.Duration = time.Second
	timeout                = 5 * time.Second
)

type Gossiper struct {
	mutex     sync.RWMutex
	id        string
	origin    string
	gen       int64
	peer      *Peer
	peers     map[string]*Peer
	seeds     map[string]*net.TCPAddr
	ln        net.Listener
	ticker    *time.Ticker
	events    chan event
	listeners []Listener
	phi       float64
	timeout   time.Duration
	rand      *rand.Rand
	logger    *log.Logger
	started   bool
	stoped    bool
}

func New(id, origin string, gen int64, seeds []string) *Gossiper {
	g := &Gossiper{
		id:        id,
		origin:    origin,
		gen:       gen,
		peers:     make(map[string]*Peer),
		seeds:     make(map[string]*net.TCPAddr),
		events:    make(chan event, 1024),
		listeners: make([]Listener, 0),
		phi:       8,
		timeout:   timeout,
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		started:   false,
		stoped:    false,
	}

	for _, sid := range seeds {
		if sid != g.id {
			g.seeds[sid] = nil
		}
	}

	return g
}

func (g *Gossiper) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.started {
		return fmt.Errorf("gossiper <id: %s, origin: %s, gen: %v> already started.", g.id, g.origin, g.gen)
	}

	for sid, _ := range g.seeds {
		if addr, err := resolve(sid); err == nil {
			g.seeds[sid] = addr
		} else {
			return fmt.Errorf("invalid seed %s: %s", sid, err)
		}
	}

	laddr, err := resolve(g.id)
	if err != nil {
		return err
	}

	g.peer = newPeer(g, g.id, g.origin, g.gen, laddr, true)
	g.peers[g.id] = g.peer

	ln, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return err
	}
	g.ln = ln

	g.ticker = time.NewTicker(interval)
	if g.logger == nil {
		g.logger = log.New(&discard{}, "", 0)
	}

	go g.serve()
	go g.gossip()
	go g.handleEvent()

	g.started = true

	g.events <- &startEvent{
		gossiper: g,
	}
	g.events <- &joinEvent{
		peer: g.peer,
	}

	return nil
}

func (g *Gossiper) Stop() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if !g.started {
		return fmt.Errorf("gossiper <id: %s, origin: %s, gen: %v> is not started.", g.id, g.origin, g.gen)
	}

	if g.stoped {
		return fmt.Errorf("gossiper <id: %s, origin: %s, gen: %v> already stoped.", g.id, g.origin, g.gen)
	}

	g.stop()

	return nil
}

func (g *Gossiper) stop() {
	g.stoped = true
	g.peer.makeLeave()
	g.ticker.Stop()
	g.ln.Close()
	g.events <- &stopEvent{
		gossiper: g,
	}
	close(g.events)
}

func (g *Gossiper) error(err error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if !g.stoped {
		g.stop()
		g.logger.Printf("gossiper stoped because an error occurred: %s\n", err)
	}
}

func (g *Gossiper) serve() {
	var delay time.Duration
	for {
		conn, err := g.ln.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if max := 1 * time.Second; delay > max {
					delay = max
				}
				g.logger.Printf("gossiper accept error: %v; retrying in %v\n", err, delay)
				time.Sleep(delay)
				continue
			}
			g.error(fmt.Errorf("gossiper accept error: %s\n", err))
			return
		}
		go g.readMessage(conn)
	}
}

func (g *Gossiper) gossip() {
	for {
		<-g.ticker.C

		g.mutex.Lock()
		if g.stoped {
			break
		}

		g.peer.beatHeart()

		var lp, dp *Peer

		lives := make([]*Peer, 0)
		deads := make([]*Peer, 0)

		for _, peer := range g.peers {
			if peer == g.peer {
				continue
			}
			if peer.isAlive() {
				lives = append(lives, peer)
			} else {
				deads = append(deads, peer)
			}
		}

		digests := g.digests()
		gossipedToSeed := false

		livesSize := len(lives)
		deadsSize := len(deads)
		seedsSize := len(g.seeds)

		if livesSize > 0 {
			lp = lives[g.rand.Intn(livesSize)]
			go g.gossipTo(lp.addr, digests)

			if g.seeds[lp.id] != nil {
				gossipedToSeed = true
			}
		}

		if deadsSize > 0 && g.rand.Intn(livesSize+1) < deadsSize {
			dp = deads[g.rand.Intn(deadsSize)]
			go g.gossipTo(dp.addr, digests)
		}

		if seedsSize > 0 && (!gossipedToSeed || livesSize < seedsSize) {
			srand := g.rand.Intn(seedsSize)
			i := 0
			for _, saddr := range g.seeds {
				if i == srand {
					go g.gossipTo(saddr, digests)
					break
				} else {
					i++
				}
			}
		}

		for _, p := range g.peers {
			if p.local {
				continue
			}
			if !p.suspect(g.phi, g.timeout) {
				delete(g.peers, p.id)
			}
		}

		g.mutex.Unlock()
	}
}

func (g *Gossiper) readMessage(conn net.Conn) {
	v, err := decode(conn)
	if err != nil {
		if err != io.EOF {
			g.logger.Printf("gossiper read message error: %s\n", err)
		}
		conn.Close()
		return
	}
	switch msg := v.(type) {
	case *SynMessage:
		g.handleSynMessage(conn, msg)
	case *AckMessage:
		g.handleAckMessage(conn, msg)
	case *Ack2Message:
		g.handleAck2Message(conn, msg)
	default:
		g.logger.Printf("gossiper read message error: unknown message %T\n", v)
	}
}

func (g *Gossiper) gossipTo(addr *net.TCPAddr, digests []*Digest) {
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		g.logger.Printf("gossiper gossip to %s error: %s\n", addr, err)
		return
	}

	g.mutex.Lock()
	if g.stoped {
		g.mutex.Unlock()
		conn.Close()
		return
	}

	syn := &SynMessage{
		Id:      proto.String(g.id),
		Origin:  proto.String(g.origin),
		Digests: digests,
	}

	g.mutex.Unlock()

	if err := encode(conn, syn); err != nil {
		conn.Close()
	}

	g.readMessage(conn)
}

func (g *Gossiper) handleSynMessage(conn net.Conn, syn *SynMessage) {
	if g.origin != syn.GetOrigin() {
		conn.Close()
		return
	}

	g.mutex.Lock()
	if g.stoped {
		g.mutex.Unlock()
		conn.Close()
		return
	}

	digests, deltas := g.scuttlebutt(syn.GetId(), syn.GetDigests())

	ack := &AckMessage{
		Id:      proto.String(g.id),
		Origin:  proto.String(g.origin),
		Digests: digests,
		Deltas:  deltas,
	}

	g.mutex.Unlock()

	if err := encode(conn, ack); err != nil {
		conn.Close()
	}

	g.readMessage(conn)
}

func (g *Gossiper) handleAckMessage(conn net.Conn, ack *AckMessage) {
	if g.origin != ack.GetOrigin() {
		conn.Close()
		return
	}

	g.mutex.Lock()
	if g.stoped {
		g.mutex.Unlock()
		conn.Close()
		return
	}

	g.updateDeltas(ack.GetDeltas())

	ack2 := &Ack2Message{
		Id:     proto.String(g.id),
		Origin: proto.String(g.origin),
		Deltas: g.fetchDeltas(ack.GetDigests()),
	}

	g.mutex.Unlock()

	if err := encode(conn, ack2); err != nil {
		conn.Close()
	}

	g.readMessage(conn)
}

func (g *Gossiper) handleAck2Message(conn net.Conn, ack2 *Ack2Message) {
	if g.origin != ack2.GetOrigin() {
		conn.Close()
		return
	}

	g.mutex.Lock()
	if g.stoped {
		g.mutex.Unlock()
		conn.Close()
		return
	}

	g.updateDeltas(ack2.GetDeltas())
	g.mutex.Unlock()

	conn.Close()
}

func (g *Gossiper) Id() string {
	return g.id
}

func (g *Gossiper) Origin() string {
	return g.origin
}

func (g *Gossiper) Gen() int64 {
	return g.gen
}

func (g *Gossiper) Seeds() []string {
	seeds := make([]string, len(g.seeds))
	for s, _ := range g.seeds {
		seeds = append(seeds, s)
	}
	return seeds
}

func (g *Gossiper) Peer() *Peer {
	return g.peer
}

func (g *Gossiper) Peers() []*Peer {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	peers := make([]*Peer, len(g.peers))

	i := 0
	for _, p := range g.peers {
		peers[i] = p
		i++
	}

	return peers
}

func (g *Gossiper) SetTimeout(timeout time.Duration) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.timeout = timeout
}

func (g *Gossiper) SetLogger(logger *log.Logger) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if logger == nil {
		g.logger = log.New(&discard{}, "", 0)
	} else {
		g.logger = logger
	}
}

func resolve(id string) (*net.TCPAddr, error) {
	if addr, err := net.ResolveTCPAddr("tcp", id); err != nil {
		return nil, err
	} else {
		return addr, nil
	}
}

type discard struct {
}

func (d *discard) Write(p []byte) (n int, err error) {
	return len(p), nil
}
