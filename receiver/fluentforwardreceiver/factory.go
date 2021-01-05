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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

// This file implements factory for SignalFx receiver.

const (
	// The value of "type" key in configuration.
	typeStr = "fluentforward"
)

func NewFactory() component.ReceiverFactory {
	return receiverhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		receiverhelper.WithLogs(createLogsReceiver),
		receiverhelper.WithTraces(createTracesReceiver))
}

func createDefaultConfig() configmodels.Receiver {
	return &Config{
		ReceiverSettings: configmodels.ReceiverSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
	}
}

func createLogsReceiver(
	_ context.Context,
	params component.ReceiverCreateParams,
	cfg configmodels.Receiver,
	consumer consumer.LogsConsumer,
) (component.LogsReceiver, error) {

	recv, err := createReceiver(cfg, params)
	if err != nil {
		return nil, err
	}
	recv.collector.logConsumer = consumer
	return recv, err
}

func createTracesReceiver(
	_ context.Context,
	params component.ReceiverCreateParams,
	cfg configmodels.Receiver,
	consumer consumer.TracesConsumer,
) (component.TracesReceiver, error) {

	recv, err := createReceiver(cfg, params)
	if err != nil {
		return nil, err
	}
	recv.collector.traceConsumer = consumer
	return recv, err
}

// This is the map of already created fluent receivers for particular configurations.
// We maintain this map because the Factory is asked trace and log receivers separately
// when it gets CreateTracesReceiver() and CreateLogsReceiver() but they must not
// create separate objects, they must use one ocReceiver object per configuration.
var receivers = map[*Config]*fluentReceiver{}

func createReceiver(cfg configmodels.Receiver, params component.ReceiverCreateParams) (*fluentReceiver, error) {
	rCfg := cfg.(*Config)
	var err error
	receiver, ok := receivers[rCfg]
	if !ok {
		receiver, err = newFluentReceiver(params.Logger, rCfg, nil, nil)
		if err != nil {
			return &fluentReceiver{}, nil
		}
		receivers[rCfg] = receiver
	}
	return receiver, nil
}
