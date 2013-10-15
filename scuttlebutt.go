package gossip

import (
	"code.google.com/p/goprotobuf/proto"
	. "github.com/hujh/gossip/message"
)

func (g *Gossiper) digests() []*Digest {
	digests := make([]*Digest, 0, len(g.peers))

	for _, p := range g.peers {
		if p.isAlive() {
			digests = append(digests, &Digest{
				Id:      proto.String(p.id),
				Gen:     proto.Int64(p.gen),
				Version: proto.Int64(p.version),
			})
		}
	}

	for i := len(digests) - 1; i > 0; i-- {
		j := g.rand.Intn(i)
		if i != j {
			digests[i], digests[j] = digests[j], digests[i]
		}
	}

	return digests
}

func (g *Gossiper) scuttlebutt(id string, reqd []*Digest) ([]*Digest, []*Delta) {
	digests := make([]*Digest, 0)
	deltas := make([]*Delta, 0)

	for _, d := range reqd {
		p := g.peers[d.GetId()]

		if p == nil {
			digests = append(digests, &Digest{
				Id:      proto.String(d.GetId()),
				Gen:     proto.Int64(0),
				Version: proto.Int64(0),
			})
		} else {
			cr := p.compare(d.GetGen(), d.GetVersion())
			switch {
			case cr > 0:
				deltas = append(deltas, g.deltaAfterVersion(p, d.GetVersion()))
			case cr < 0:
				digests = append(digests, &Digest{
					Id:      proto.String(p.id),
					Gen:     proto.Int64(p.gen),
					Version: proto.Int64(p.version),
				})
			}
		}
	}

	return digests, deltas
}

func (g *Gossiper) updateDeltas(deltas []*Delta) {
	for _, d := range deltas {
		p := g.peers[d.GetId()]

		if p == nil {
			addr, err := resolve(d.GetId())
			if err != nil {
				g.logger.Printf("gossiper remote update error: invalid remote id '%s'", d.GetId())
				continue
			}
			np := newPeer(g, d.GetId(), g.origin, d.GetGen(), addr, false)
			p = np

			g.peers[d.GetId()] = np
			g.events <- &joinEvent{
				peer: np,
			}
		}

		p.updateHeartBeat(d.GetHeartbeat(), d.GetGen())
		for _, v := range d.GetValues() {
			p.updateValue(v.GetKey(), v.GetValue(), v.GetVersion())
		}
	}
}

func (g *Gossiper) fetchDeltas(digests []*Digest) (deltas []*Delta) {
	deltas = make([]*Delta, 0, len(g.peers)-1)

	for _, d := range digests {
		p := g.peers[d.GetId()]

		if p == nil {
			continue
		}

		if p.compare(d.GetGen(), d.GetVersion()) > 0 {
			deltas = append(deltas, g.deltaAfterVersion(p, d.GetVersion()))
		}
	}

	return deltas
}

func (g *Gossiper) deltaAfterVersion(peer *Peer, lowestVersion int64) *Delta {
	delta := &Delta{
		Id:        proto.String(peer.id),
		Gen:       proto.Int64(peer.gen),
		Version:   proto.Int64(peer.version),
		Heartbeat: proto.Int64(peer.heartbeat),
		Values:    make([]*Value, 0),
	}

	for k, v := range peer.values {
		if v.version > lowestVersion {
			delta.Values = append(delta.Values, &Value{
				Key:     proto.String(k),
				Value:   proto.String(v.value),
				Version: proto.Int64(v.version),
			})
		}
	}

	return delta
}
