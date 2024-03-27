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
	// byteCount := r.reader.Size()
	// fullBytes, _ := r.reader.Peek(byteCount)
	// fmt.Println("Full Bytes: ", fullBytes)
	// fmt.Println("Full String: ", string(fullBytes))

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

func (r *Resp) parseLine() (string, error) {
	b, err := r.reader.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	cleanB := string(b[:len(b)-2])

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
	b, _ := r.parseLine()
	v.String = b
	v.Raw = b

	return v, nil
}
func (r *Resp) parseBulkString() (Value, error) {
	v := Value{Type: BulkString}

	strSize, _ := r.parseInt()

	data := make([]byte, strSize)
	r.reader.Read(data)

	v.Bulk = string(data)
	v.Raw = string(data)

	return v, nil
}
func (r *Resp) parseArray() (Value, error) {
	v := Value{Type: Array}
	sb, _ := r.reader.ReadByte()
	arraySize, _ := strconv.ParseInt(string(sb), 10, 64)

	v.Array = make([]Value, arraySize)
	for i := 0; i < int(arraySize); i++ {
		r.reader.Discard(2)
		value, _ := r.Parse()
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
