package com.seminar.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TransactionAnalyticsJob {

    public static void main(String[] args) throws Exception {

        // 1. Flink execution environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // Checkpoint mỗi 10 giây
        env.enableCheckpointing(10_000);

        // 2. Kafka Source
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:29092")
                        .setTopics("transactions")
                        .setGroupId("flink-transaction-analytics")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema()
                        )
                        .build();

        DataStream<String> rawStream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Transactions Source"
                );

        // 3. Parse JSON
        DataStream<Transaction> transactions =
                rawStream
                        .map(json -> {
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(
                                    json,
                                    Transaction.class
                            );
                        })
                        .returns(Transaction.class)
                        .name("Parse JSON");

        // 4. Filter invalid transactions
        DataStream<Transaction> validTransactions =
                transactions
                        .filter(transaction ->
                                transaction.account_id != null
                                        && transaction.amount > 0
                        )
                        .name("Filter Invalid Transactions");

        // 5. keyBy + Window + Aggregate
        DataStream<String> analytics =
                validTransactions

                        .keyBy(transaction ->
                                transaction.account_id
                        )

                        .window(
                                TumblingProcessingTimeWindows.of(
                                        Duration.ofSeconds(10)
                                )
                        )

                        .process(
                                new TransactionWindowProcessor()
                        )

                        .name("10s Transaction Analytics");

        // 6. Output
        analytics
                .print()
                .name("Realtime Analytics Output");

        env.execute("Real-time Transaction Analytics");
    }
}

class TransactionWindowProcessor
        extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                Transaction,
                String,
                String,
                org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

    @Override
    public void process(
            String accountId,
            Context context,
            Iterable<Transaction> transactions,
            Collector<String> out) {

        int count = 0;
        double total = 0;

        for (Transaction transaction : transactions) {
            count++;
            total += transaction.amount;
        }

        double average =
                count == 0 ? 0 : total / count;

        String result = String.format(
                "WINDOW | %s | count=%d | total=$%.2f | avg=$%.2f",
                accountId,
                count,
                total,
                average
        );

        if (total >= 2000) {
            result += " | ALERT: HIGH TRANSACTION VOLUME";
        }

        out.collect(result);
    }
}
