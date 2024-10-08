// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tablepb

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// Load TableState with THREAD-SAFE
func (s *TableState) Load() TableState {
	return TableState(atomic.LoadInt32((*int32)(s)))
}

// Store TableState with THREAD-SAFE
func (s *TableState) Store(new TableState) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

// CompareAndSwap is just like sync/atomic.Atomic*.CompareAndSwap.
func (s *TableState) CompareAndSwap(old, new TableState) bool {
	oldx := int32(old)
	newx := int32(new)
	return atomic.CompareAndSwapInt32((*int32)(s), oldx, newx)
}

// TableID is the ID of the table
type TableID = int64

// Ts is the timestamp with a logical count
type Ts = uint64

// Key is a custom type for bytes encoded in memcomparable format.
// Key is read-only, must not be mutated.
type Key []byte

var (
	_ fmt.Stringer = Key{}
	_ fmt.Stringer = (*Key)(nil)
)

func (k Key) String() string {
	return hex.EncodeToString(k)
}

var (
	_ json.Marshaler = Key{}
	_ json.Marshaler = (*Key)(nil)
)

// MarshalJSON implements json.Marshaler.
// It is helpful to format span in log.
func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

var (
	_ encoding.TextMarshaler = &Span{}
	_ encoding.TextMarshaler = (*Span)(nil)
)

// MarshalText implements encoding.TextMarshaler (used in proto.CompactTextString).
// It is helpful to format span in log.
func (s *Span) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Span) String() string {
	length := len("{tableID:, startKey:, endKey:}")
	length += 8 // for TableID
	length += len(s.StartKey) + len(s.EndKey)
	var b strings.Builder
	b.Grow(length)
	b.Write([]byte("{tableID:"))
	b.Write([]byte(strconv.Itoa(int(s.TableID))))
	if len(s.StartKey) > 0 {
		b.Write([]byte(", startKey:"))
		b.Write([]byte(s.StartKey.String()))
	}
	if len(s.EndKey) > 0 {
		b.Write([]byte(", endKey:"))
		b.Write([]byte(s.EndKey.String()))
	}
	b.Write([]byte("}"))
	return b.String()
}

// Less compares two Spans, defines the order between spans.
func (s *Span) Less(b *Span) bool {
	if s.TableID < b.TableID {
		return true
	}
	if bytes.Compare(s.StartKey, b.StartKey) < 0 {
		return true
	}
	return false
}

// Eq compares two Spans, defines the equality between spans.
func (s *Span) Eq(b *Span) bool {
	return s.TableID == b.TableID &&
		bytes.Equal(s.StartKey, b.StartKey) &&
		bytes.Equal(s.EndKey, b.EndKey)
}
