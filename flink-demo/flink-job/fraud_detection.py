from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # --- Kết nối Kafka source ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("transactions") \
        .set_group_id("flink-fraud-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        "Kafka Transactions"
    )

    # --- Xử lý: parse JSON, lọc giao dịch lớn ---
    def detect_fraud(record):
        tx = json.loads(record)
        amount = tx.get('amount', 0)
        user = tx.get('user_id')
        merchant = tx.get('merchant')

        if amount > 10_000_000:  # ngưỡng: 10 triệu VNĐ
            return f"🚨 ALERT | User: {user} | Amount: {amount:,} VNĐ | Merchant: {merchant}"
        else:
            return f"✅ OK    | User: {user} | Amount: {amount:,} VNĐ"

    result = stream.map(detect_fraud)
    result.print()

    env.execute("Fraud Detection Job")

if __name__ == '__main__':
    main()