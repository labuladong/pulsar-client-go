// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
)

func main() {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	defer client.Close()

	topicsPattern := "persistent://public/default/topic.*"
	opts := pulsar.ConsumerOptions{
		// fill `TopicsPattern` field will create a regex consumer
		TopicsPattern:    topicsPattern,
		SubscriptionName: "regex-sub",
	}
	consumer, err := client.Subscribe(opts)
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received a message from %v, msgId: %#v -- content: '%s'\n",
			msg.Topic(), msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
}
