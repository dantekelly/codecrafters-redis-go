package main

import (
	"bufio"
	"io"
	"log"
	"strconv"
)

const (
	String     = "+" // Format: +OK\r\n
	Error      = "-" // Format: -ERR message\r\n
	Integer    = ":" // Format: :1000\r\n
	BulkString = "$" // Format: $6\r\nfoobar\r\n
	Array      = "*" // Format: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
)

type Value struct {
	Type   string
	String string
	Bulk   string
	Raw    string
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
	if err != nil {
		return Value{}, err
	}

	_type := string(b)
	switch _type {
	case String:
		return r.parseString()
	case BulkString:
		// Check if it's a RDB file or a normal bulk string
		return r.parseBulkString()
	case Array:
		return r.parseArray()
	default:
		log.Printf("Unknown byte: %b", b)
		log.Printf("Unknown string byte: %s", string(b))
		log.Printf("Unknown type: %s", _type)
		return Value{}, nil
	}
}

func (r *Resp) parseLine() (string, error) {
	b, err := r.reader.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	cleanB := string(b[:len(b)-2]) // Remove the \r\n

	return string(cleanB), nil
}
func (r *Resp) parseInt() (int, error) {
	b, err := r.parseLine()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(b)
}

func (r *Resp) parseString() (Value, error) {
	v := Value{Type: String}
	b, err := r.parseLine()
	if err != nil {
		return Value{}, err
	}
	v.String = b
	v.Raw = b

	return v, nil
}

func (r *Resp) parseBulkString() (Value, error) {
	v := Value{Type: BulkString}

	strSize, err := r.parseInt()
	if err != nil {
		return Value{}, err
	}

	data := make([]byte, strSize)
	_, err = io.ReadFull(r.reader, data)
	if err != nil {
		return Value{}, err
	}

	v.Bulk = string(data)
	v.Raw = string(data)

	// Sometimes this is actually an RDB file and not a normal bulk string that has a trailing \r\n
	// Check if the next bytes are \r\n
	b, err := r.reader.Peek(1)
	if err != nil {
		return Value{}, err
	}

	// If it's a \r, then it's a BulkString
	if string(b) == "\r" {
		// Discard the trailing \r\n
		_, err = r.reader.Discard(2)
		if err != nil {
			return Value{}, err
		}
	}

	return v, nil
}
func (r *Resp) parseArray() (Value, error) {
	v := Value{Type: Array}
	arraySize, err := r.parseInt()
	if err != nil {
		return Value{}, err
	}

	v.Array = make([]Value, arraySize)
	for i := 0; i < int(arraySize); i++ {
		value, err := r.Parse()
		if err != nil {
			return Value{}, err
		}
		v.Array[i] = value
	}

	return v, nil
}

func encodeRDB(data string) []byte {
	return []byte("$" + strconv.Itoa(len(data)) + "\r\n" + data)
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
func encodeArray(data []Value) []byte {
	resultString := ""
	for _, v := range data {
		switch v.Type {
		case String:
			resultString += string(encodeString(v.String))
		case BulkString:
			resultString += string(encodeBulkString(v.Bulk))
		}
	}

	return []byte("*" + strconv.Itoa(len(data)) + "\r\n" + resultString)
}
