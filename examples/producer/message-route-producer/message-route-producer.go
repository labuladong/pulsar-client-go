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

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-partitioned-topic",
		MessageRouter: func(msg *pulsar.ProducerMessage, tm pulsar.TopicMetadata) int {
			fmt.Println("Top has", tm.NumPartitions(), "partitions. Routing message ", msg, " to partition 2.")
			// always push msg to partition 2
			return 2
		},
	})

	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("message-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}

func a() {
	type testJSON struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	var (
		exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
			"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	properties := make(map[string]string)
	properties["pulsar"] = "hello"
	jsonSchemaWithProperties := pulsar.NewJSONSchema(exampleSchemaDef, properties)
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:  "jsonTopic",
		Schema: jsonSchemaWithProperties,
	})

	if err != nil {
		log.Fatal(err)
	}

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Value: &testJSON{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	producer.Close()
}
