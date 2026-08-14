package com.seminar.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionProducer {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        String[] accounts = {
                "acc-01",
                "acc-02",
                "acc-03"
        };

        String[] merchants = {
                "m-01",
                "m-02",
                "m-03"
        };

        System.out.println("=== Transaction Producer Started ===");
        System.out.println("Sending transactions to Kafka topic: transactions");
        System.out.println();

        while (true) {

            Transaction transaction = new Transaction();

            transaction.transaction_id =
                    "tx-" + UUID.randomUUID().toString().substring(0, 8);

            transaction.account_id =
                    accounts[random.nextInt(accounts.length)];

            transaction.merchant_id =
                    merchants[random.nextInt(merchants.length)];

            transaction.amount =
                    50 + random.nextInt(951);

            transaction.event_time =
                    Instant.now().toString();

            String json = mapper.writeValueAsString(transaction);

            producer.send(
                    new ProducerRecord<>(
                            "transactions",
                            transaction.account_id,
                            json
                    )
            );

            System.out.printf(
                    "SENT | %-6s | %-4s | $%.2f%n",
                    transaction.account_id,
                    transaction.merchant_id,
                    transaction.amount
            );

            Thread.sleep(1000);
        }
    }
}