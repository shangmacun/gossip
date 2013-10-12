package gossip

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type versionedValue struct {
	value   string
	version int64
}

type Peer struct {
	mutex     sync.RWMutex
	id        string
	origin    string
	gen       int64
	local     bool
	heartbeat int64
	version   int64
	values    map[string]*versionedValue
	addr      *net.TCPAddr
	detector  *detector
	deadTime  time.Time
	events    chan interface{}
	alive     bool
	leave     bool
}

func newPeer(id, origin string, gen int64, addr *net.TCPAddr, events chan interface{}, local bool) *Peer {
	return &Peer{
		id:       id,
		origin:   origin,
		gen:      gen,
		local:    local,
		addr:     addr,
		detector: newDetector(interval),
		values:   make(map[string]*versionedValue),
		events:   events,
		alive:    true,
		leave:    false,
	}
}

func (p *Peer) Id() string {
	return p.id
}

func (p *Peer) Origin() string {
	return p.origin
}

func (p *Peer) Gen() int64 {
	return p.gen
}

func (p *Peer) Local() bool {
	return p.local
}

func (p *Peer) Set(key, value string) error {
	if !p.local {
		return fmt.Errorf("peer '%s' is a remote readonly peer.", p.id)
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.leave {
		return fmt.Errorf("peer '%s' has left.", p.id)
	}

	p.version++
	p.values[key] = &versionedValue{value: value, version: p.version}

	p.events <- &updateEvent{
		peer:  p,
		key:   key,
		value: value,
	}
	return nil
}

func (p *Peer) Get(key string) string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	v := p.values[key]
	if v != nil {
		return v.value
	}
	return ""
}

func (p *Peer) Values() map[string]string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	values := make(map[string]string)

	for k, v := range p.values {
		values[k] = v.value
	}

	return values
}

func (p *Peer) isAlive() bool {
	return p.alive
}

func (p *Peer) beatHeart() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.heartbeat++
	if p.heartbeat > p.version {
		p.version = p.heartbeat
	}
}

func (p *Peer) updateHeartBeat(heartbeat int64, gen int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.gen > gen {
		return
	} else if p.gen < gen {
		p.restart(gen)
	}

	p.heartbeat = heartbeat
	if p.heartbeat > p.version {
		p.version = p.heartbeat
	}

	p.detector.add(time.Now())

	if !p.alive && !p.leave {
		p.alive = true
		p.deadTime = time.Time{}
		p.events <- &aliveEvent{
			peer: p,
		}
	}
}

func (p *Peer) updateValue(key, value string, version int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if version > p.version {
		p.values[key] = &versionedValue{value: value, version: version}
		p.version = version
		p.events <- &updateEvent{
			peer:  p,
			key:   key,
			value: value,
		}
	}
}

func (p *Peer) compare(gen int64, version int64) int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.gen == gen {
		return p.version - version
	}

	if p.gen > gen {
		return 1
	}

	return -1
}

func (p *Peer) restart(gen int64) {
	p.gen = gen
	p.version = 0
	p.heartbeat = 0
	p.alive = true
	p.deadTime = time.Time{}
	p.detector.reset()
	p.values = make(map[string]*versionedValue)

	p.events <- &restartEvent{
		peer: p,
	}
}

func (p *Peer) suspect(phi float64, timeout time.Duration) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.leave {
		return false
	}

	now := time.Now()
	if phi < p.detector.phi(now) {
		if p.alive {
			p.alive = false
			p.deadTime = time.Now()
			p.events <- &deadEvent{
				peer: p,
			}
		} else {
			if now.Sub(p.deadTime) > timeout {
				p.leave = true
				p.events <- &leaveEvent{
					peer: p,
				}
				return false
			}
		}

		return true
	} else if !p.alive {
		p.alive = true
		p.deadTime = time.Time{}
		p.events <- &aliveEvent{
			peer: p,
		}
	}

	return true
}

func (p *Peer) makeAlive() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.alive && !p.leave {
		p.alive = true
		p.deadTime = time.Time{}
		p.events <- &aliveEvent{
			peer: p,
		}
	}
}

func (p *Peer) makeDead() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.alive && !p.leave {
		p.alive = false
		p.deadTime = time.Now()
		p.events <- &deadEvent{
			peer: p,
		}
	}
}

func (p *Peer) makeLeave() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.leave {
		p.alive = false
		p.leave = true

		p.events <- &leaveEvent{
			peer: p,
		}
	}
}
