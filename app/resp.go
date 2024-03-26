package main

import (
	"bufio"
	"io"
	"log"
	"strconv"
)

const (
	String     = "+" // Format: +OK\r\n
	Error      = "-" // Format:
	Integer    = ":" // Format:
	BulkString = "$" // Format: $6\r\nfoobar\r\n
	Array      = "*" // Format: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
)

type Value struct {
	Type   string
	String string
	Num    int
	Array  []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(r io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(r)}
}

func (r *Resp) Parse() (Value, error) {
	b, err := r.reader.ReadByte()
	_type := string(b)

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case String:
		return r.parseString()
	case BulkString:
		return r.parseBulkString()
	case Array:
		return r.parseArray()
	default:
		log.Printf("Unknown type: %s", _type)
		return Value{}, nil
	}
}

func (r *Resp) parseString() (Value, error) {
	v := Value{Type: String}
	b, _ := r.reader.ReadBytes('\n')
	v.String = string(b)

	return v, nil
}
func (r *Resp) parseBulkString() (Value, error) {
	v := Value{Type: BulkString}
	size, _ := r.reader.ReadByte()
	strSize, _ := strconv.ParseInt(string(size), 10, 64)
	// remove the first character and \r\n
	r.reader.Discard(2)
	data := make([]byte, strSize)
	r.reader.Read(data)

	v.String = string(data)

	return v, nil
}
func (r *Resp) parseArray() (Value, error) {
	v := Value{Type: Array}
	sb, _ := r.reader.ReadByte()
	arraySize, _ := strconv.ParseInt(string(sb), 10, 64)

	v.Array = make([]Value, arraySize)
	for i := 0; i < int(arraySize); i++ {
		log.Print("Parsing Array element")
		r.reader.Discard(2)
		value, _ := r.Parse()
		v.Array[i] = value
	}

	return v, nil
}

func encodeBulkString(data string) []byte {
	return []byte("$" + strconv.Itoa(len(data)) + "\r\n" + data + "\r\n")
}
func encodeString(data string) []byte {
	return []byte("+" + data + "\r\n")
}
func encodeError(data string) []byte {
	return []byte("-ERR " + data + "\r\n")
}
