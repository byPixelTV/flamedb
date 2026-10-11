package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const eventBinaryVersion byte = 1

// eventEntityValue reads only the score and requested tag. Binary events do not
// allocate a tag map or copy strings; the returned ID borrows data until the
// caller advances/closes its iterator or releases its Get value. Legacy JSON
// values remain readable without a migration.
func eventEntityValue(data []byte, tag string) (id []byte, value float64, found bool, err error) {
	if len(data) == 0 || data[0] != eventBinaryVersion {
		e, err := decodeEventValue(data, "")
		if err != nil {
			return nil, 0, false, err
		}
		entity, ok := e.Tags[tag]
		return []byte(entity), e.Value, ok, nil
	}
	if len(data) < 19 {
		return nil, 0, false, errors.New("invalid event encoding")
	}
	value = math.Float64frombits(binary.BigEndian.Uint64(data[9:17]))
	count := int(binary.BigEndian.Uint16(data[17:19]))
	off := 19
	for i := 0; i < count; i++ {
		if len(data)-off < 2 {
			return nil, 0, false, errors.New("invalid event encoding")
		}
		klen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if len(data)-off < klen+2 {
			return nil, 0, false, errors.New("invalid event encoding")
		}
		key := data[off : off+klen]
		off += klen
		vlen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if len(data)-off < vlen {
			return nil, 0, false, errors.New("invalid event encoding")
		}
		if string(key) == tag {
			id, found = data[off:off+vlen], true
		}
		off += vlen
	}
	return id, value, found, nil
}

func encodeEventValue(e Event) ([]byte, error) {
	if len(e.Tags) > math.MaxUint16 {
		return json.Marshal(e)
	}

	size := 1 + 8 + 8 + 2
	for k, v := range e.Tags {
		if len(k) > math.MaxUint16 || len(v) > math.MaxUint16 {
			return json.Marshal(e)
		}
		size += 2 + len(k) + 2 + len(v)
	}

	buf := make([]byte, size)
	buf[0] = eventBinaryVersion
	off := 1
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Timestamp))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], math.Float64bits(e.Value))
	off += 8
	binary.BigEndian.PutUint16(buf[off:], uint16(len(e.Tags)))
	off += 2

	for k, v := range e.Tags {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)

		binary.BigEndian.PutUint16(buf[off:], uint16(len(v)))
		off += 2
		copy(buf[off:], v)
		off += len(v)
	}

	return buf, nil
}

func decodeEventValue(data []byte, metric string) (Event, error) {
	if len(data) == 0 {
		return Event{}, errors.New("empty event")
	}

	if data[0] != eventBinaryVersion {
		var e Event
		if err := json.Unmarshal(data, &e); err != nil {
			return Event{}, err
		}
		if metric != "" {
			e.Metric = metric
		}
		return e, nil
	}

	off := 1
	if len(data) < off+8+8+2 {
		return Event{}, errors.New("invalid event encoding")
	}

	ts := int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	value := math.Float64frombits(binary.BigEndian.Uint64(data[off:]))
	off += 8
	tagCount := int(binary.BigEndian.Uint16(data[off:]))
	off += 2

	var tags map[string]string
	if tagCount > 0 {
		tags = make(map[string]string, tagCount)
	}

	for i := 0; i < tagCount; i++ {
		if len(data) < off+2 {
			return Event{}, errors.New("invalid event encoding")
		}
		klen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if len(data) < off+klen+2 {
			return Event{}, errors.New("invalid event encoding")
		}
		key := string(data[off : off+klen])
		off += klen

		vlen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if len(data) < off+vlen {
			return Event{}, errors.New("invalid event encoding")
		}
		val := string(data[off : off+vlen])
		off += vlen
		tags[key] = val
	}

	return Event{Timestamp: ts, Metric: metric, Value: value, Tags: tags}, nil
}
