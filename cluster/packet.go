package cluster

import (
	"bytes"
	"encoding/binary"
)

const (
	msgRequest = 1
	msgReply   = 2
	msgError   = 3
)

type packetFixedSizeHeader struct {
	PacketType  byte
	RequestType uint8
	AddressSize uint16
	RequestID   uint32
	PayloadSize uint32
}

type rpcPacket struct {
	PacketType     byte
	RequestType    uint8
	RequestID      uint32
	ReplyToAddress string
	Payload        []byte
}

func (pkt rpcPacket) Encode() []byte {
	var (
		buffer bytes.Buffer
	)

	header := packetFixedSizeHeader{
		PacketType:  pkt.PacketType,
		RequestID:   pkt.RequestID,
		RequestType: pkt.RequestType,
		AddressSize: uint16(len(pkt.ReplyToAddress)),
		PayloadSize: uint32(len(pkt.Payload)),
	}

	err := binary.Write(&buffer, binary.BigEndian, header)
	if err != nil {
		logger.Fatalf("Enable to encode rpcPacket: %v", err)
	}

	_, err = buffer.Write([]byte(pkt.ReplyToAddress))
	if err != nil {
		logger.Fatalf("Enable to encode rpcPacket: %v", err)
	}

	_, err = buffer.Write(pkt.Payload)
	if err != nil {
		logger.Fatalf("Enable to encode rpcPacket: %v", err)
	}

	return buffer.Bytes()
}

func decode(buffer []byte) (rpcPacket, error) {
	var header packetFixedSizeHeader

	reader := bytes.NewReader(buffer)

	err := binary.Read(reader, binary.BigEndian, &header)
	if err != nil {
		return rpcPacket{}, err
	}

	reply := rpcPacket{
		PacketType:  header.PacketType,
		RequestID:   header.RequestID,
		RequestType: header.RequestType,
	}

	size := header.PayloadSize
	if size < uint32(header.AddressSize) {
		size = uint32(header.AddressSize)
	}

	tmp := make([]byte, 0, size)

	if header.AddressSize > 0 {
		tmp = tmp[0:header.AddressSize]

		_, err = reader.Read(tmp)
		if err != nil {
			return reply, err
		}

		reply.ReplyToAddress = string(tmp)
	}

	tmp = tmp[0:header.PayloadSize]

	if header.PayloadSize > 0 {
		_, err = reader.Read(tmp)
		if err != nil {
			return reply, err
		}
	}

	reply.Payload = tmp

	return reply, nil
}
