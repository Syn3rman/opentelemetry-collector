// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fluentforwardreceiver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.opencensus.io/stats"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/data"
	v11 "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/common/v1"
	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/trace/v1"
	"go.opentelemetry.io/collector/receiver/fluentforwardreceiver/observ"
)

// The initial size of the read buffer. Messages can come in that are bigger
// than this, but this serves as a starting point.
const readBufferSize = 10 * 1024

type server struct {
	outCh   chan<- Event
	traceCh chan<- pdata.Traces
	logger  *zap.Logger
}

func newServer(outCh chan<- Event, traceCh chan<- pdata.Traces, logger *zap.Logger) *server {
	return &server{
		outCh:   outCh,
		traceCh: traceCh,
		logger:  logger,
	}
}

func (s *server) Start(ctx context.Context, listener net.Listener) {
	go func() {
		s.handleConnections(ctx, listener)
		if ctx.Err() == nil {
			panic("logic error in receiver, connections should always be listened for while receiver is running")
		}
	}()
}

func (s *server) handleConnections(ctx context.Context, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if ctx.Err() != nil {
			return
		}
		// If there is an error and the receiver isn't shutdown, we need to
		// keep trying to accept connections if at all possible. Put in a sleep
		// to prevent hot loops in case the error persists.
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}
		stats.Record(ctx, observ.ConnectionsOpened.M(1))

		s.logger.Debug("Got connection", zap.String("remoteAddr", conn.RemoteAddr().String()))

		go func() {
			defer stats.Record(ctx, observ.ConnectionsClosed.M(1))

			err := s.handleConn(ctx, conn)
			if err != nil {
				if err == io.EOF {
					s.logger.Debug("Closing connection", zap.String("remoteAddr", conn.RemoteAddr().String()), zap.Error(err))
				} else {
					s.logger.Debug("Unexpected error handling connection", zap.String("remoteAddr", conn.RemoteAddr().String()), zap.Error(err))
				}
			}
			conn.Close()
		}()
	}
}

func (s *server) handleConn(ctx context.Context, conn net.Conn) error {
	reader := msgp.NewReaderSize(conn, readBufferSize)

	for {
		mode, err := DetermineNextEventMode(reader.R)
		if err != nil {
			return err
		}

		var event Event
		switch mode {
		case UnknownMode:
			return errors.New("could not determine event mode")
		case MessageMode:
			event = &MessageEventLogRecord{}
		case ForwardMode:
			event = &ForwardEventLogRecords{}
		case PackedForwardMode:
			event = &PackedForwardEventLogRecords{}
		default:
			panic("programmer bug in mode handling")
		}

		tag, err := readTag(reader)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("Error in reading tag: %v", err)
		}

		if tag == "span.test" {
			fmt.Println("Parsing span")
			traces, err := parseSpan(reader)
			if err != nil {
				return fmt.Errorf("Error in parsing span: %v", err)
			}

			s.traceCh <- traces

		} else {
			err = event.DecodeMsg(reader)
			if err != nil {
				if err != io.EOF {
					stats.Record(ctx, observ.FailedToParse.M(1))
				}
				return fmt.Errorf("failed to parse %s mode event: %v", mode.String(), err)
			}

			stats.Record(ctx, observ.EventsParsed.M(1))

			s.outCh <- event

			// We must acknowledge the 'chunk' option if given. We could do this in
			// another goroutine if it is too much of a bottleneck to reading
			// messages -- this is the only thing that sends data back to the
			// client.
			if event.Chunk() != "" {
				err := msgp.Encode(conn, AckResponse{Ack: event.Chunk()})
				if err != nil {
					return fmt.Errorf("failed to acknowledge chunk %s: %v", event.Chunk(), err)
				}
			}
		}
	}
}

func readTag(dc *msgp.Reader) (string, error) {
	arrLen, err := dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return "", err
	}
	if arrLen < 2 || arrLen > 3 {
		err = msgp.ArrayError{Wanted: 2, Got: arrLen}
		return "", err
	}

	tag, err := dc.ReadString()
	if err != nil {
		return "", msgp.WrapError(err, "Tag")
	}
	return tag, nil
}

func parseSpan(dc *msgp.Reader) (pdata.Traces, error) {

	var err error
	var traces pdata.Traces

	_, err = dc.ReadArrayHeader()
	if err != nil {
		return traces, msgp.WrapError(err)
	}

	_, err = dc.ReadArrayHeader()
	if err != nil {
		return traces, msgp.WrapError(err, "Record")
	}

	// Since the first field is the end timestamp of the span and it is already present in the span field,
	// there is no need to read it

	err = dc.Skip()

	var resourceSpan otlptrace.ResourceSpans
	var ils otlptrace.InstrumentationLibrarySpans
	var span otlptrace.Span
	var instrumentationLibrary v11.InstrumentationLibrary
	var status otlptrace.Status

	// Decode map of attributes
	attrLen, err := dc.ReadMapHeader()
	if err != nil {
		return traces, msgp.WrapError(err, "Map")
	}
	for attrLen > 0 {
		attrLen--
		key, err := dc.ReadString()
		if err != nil {
			return traces, msgp.WrapError(err, "Attribute key")
		}
		val, err := dc.ReadIntf()
		if err != nil {
			return traces, msgp.WrapError(err, "Attribute value")
		}
		switch key {
		case "traceId":
			var traceId [16]byte
			copy(traceId[:], val.(string))
			span.TraceId = data.NewTraceID(traceId)
		case "spanId":
			var spanId [8]byte
			copy(spanId[:], val.(string))
			span.SpanId = data.NewSpanID(spanId)
		case "parentSpanId":
			var parentSpanId [8]byte
			copy(parentSpanId[:], val.(string))
			span.ParentSpanId = data.NewSpanID(parentSpanId)
		case "name":
			span.Name = val.(string)
		case "spanKind":
			span.Kind = otlptrace.Span_SpanKind(val.(int64))
		case "startTime":
			span.StartTimeUnixNano = val.(uint64)
		case "endTime":
			span.EndTimeUnixNano = val.(uint64)
		case "attrs":
			attributes := attributesToInternal(val.(map[string](interface{})))
			span.Attributes = attributes
		case "statusCode":
			code, _ := strconv.Atoi(val.(string))
			status.Code = otlptrace.Status_StatusCode(code)
		case "statusMessage":
			status.Message = val.(string)
		case "instrumentationLibraryName":
			instrumentationLibrary.Name = val.(string)
		case "instrumentationLibraryVersion":
			instrumentationLibrary.Version = val.(string)
		default:
			fmt.Printf("%v: %v\n", key, val)
			// return fmt.Errorf("Encountered unknown field while parsing span")
		}
	}
	span.Status = status
	ils.InstrumentationLibrary = instrumentationLibrary
	fmt.Printf("Span: %+v\n", span)
	// var ilsSpans []*otlptrace.Span
	// ilsSpans = append(ilsSpans, &span)
	ils.Spans = []*otlptrace.Span{}
	ils.Spans = append(ils.Spans, &span)
	resourceSpan.InstrumentationLibrarySpans = []*otlptrace.InstrumentationLibrarySpans{&ils}
	fmt.Printf("Resource span: %#v\n", resourceSpan.InstrumentationLibrarySpans)
	traces = pdata.TracesFromOtlp([]*otlptrace.ResourceSpans{&resourceSpan})
	fmt.Printf("Traces: %+v\n", traces)
	return traces, nil
}

func attributesToInternal(attrs map[string]interface{}) []v11.KeyValue {
	var attributes []v11.KeyValue
	for k, v := range attrs {
		switch val := v.(type) {
		case string:
			sval := &v11.AnyValue_StringValue{val}
			temp := v11.AnyValue{Value: sval}
			attributes = append(attributes, v11.KeyValue{Key: k, Value: temp})
		}
	}
	return attributes
}

// DetermineNextEventMode inspects the next bit of data from the given peeker
// reader to determine which type of event mode it is.  According to the
// forward protocol spec: "Server MUST detect the carrier mode by inspecting
// the second element of the array."  It is assumed that peeker is aligned at
// the start of a new event, otherwise the result is undefined and will
// probably error.
func DetermineNextEventMode(peeker Peeker) (EventMode, error) {
	var chunk []byte
	var err error
	chunk, err = peeker.Peek(2)
	if err != nil {
		return UnknownMode, err
	}

	// The first byte is the array header, which will always be 1 byte since no
	// message modes have more than 4 entries. So skip to the second byte which
	// is the tag string header.
	tagType := chunk[1]
	// We already read the first type for the type
	tagLen := 1

	isFixStr := tagType&0b10100000 == 0b10100000
	if isFixStr {
		tagLen += int(tagType & 0b00011111)
	} else {
		switch tagType {
		case 0xd9:
			chunk, err = peeker.Peek(3)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 1 + int(chunk[2])
		case 0xda:
			chunk, err = peeker.Peek(4)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 2 + int(binary.BigEndian.Uint16(chunk[2:]))
		case 0xdb:
			chunk, err = peeker.Peek(6)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 4 + int(binary.BigEndian.Uint32(chunk[2:]))
		default:
			return UnknownMode, errors.New("malformed tag field")
		}
	}

	// Skip past the first byte (array header) and the entire tag and then get
	// one byte into the second field -- that is enough to know its type.
	chunk, err = peeker.Peek(1 + tagLen + 1)
	if err != nil {
		return UnknownMode, err
	}

	secondElmType := msgp.NextType(chunk[1+tagLen:])

	switch secondElmType {
	case msgp.IntType, msgp.UintType, msgp.ExtensionType:
		return MessageMode, nil
	case msgp.ArrayType:
		return ForwardMode, nil
	case msgp.BinType, msgp.StrType:
		return PackedForwardMode, nil
	default:
		return UnknownMode, fmt.Errorf("unable to determine next event mode for type %v", secondElmType)
	}
}
