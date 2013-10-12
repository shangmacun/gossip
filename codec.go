package gossip

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	. "github.com/hujh/gossip/message"
	"io"
)

const (
	typeSyn  = 0x01
	typeAck  = 0x02
	typeAck2 = 0x03
)

func encode(w io.Writer, v interface{}) error {
	var t byte
	switch v.(type) {
	case *SynMessage:
		t = typeSyn
	case *AckMessage:
		t = typeAck
	case *Ack2Message:
		t = typeAck2
	default:
		return fmt.Errorf("unknown message type %t", v)
	}

	b, err := proto.Marshal(v.(proto.Message))
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, t); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return nil
}

func decode(r io.Reader) (interface{}, error) {
	var t byte
	var l uint32
	var v proto.Message

	if err := binary.Read(r, binary.BigEndian, &t); err != nil {
		return nil, err
	}

	switch t {
	case typeSyn:
		v = &SynMessage{}
	case typeAck:
		v = &AckMessage{}
	case typeAck2:
		v = &Ack2Message{}
	default:
		return nil, fmt.Errorf("unknown message type %v", t)
	}

	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return nil, err
	}

	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}

	if err := proto.Unmarshal(b, v); err != nil {
		return nil, err
	}

	return v, nil
}
