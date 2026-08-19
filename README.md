# Apache Flink Seminar

Thư mục này chứa tài liệu seminar về Apache Flink, bao gồm slide, demo, report, tài liệu nền và bộ câu hỏi phản biện.

## Nội dung chính

- Giới thiệu Apache Flink và vai trò trong Big Data pipeline.
- So sánh true streaming và micro-batch.
- Kiến trúc Flink: Client, Dispatcher, ResourceManager, JobManager, TaskManager, Task Slot.
- Dataflow và API layers: SQL/Table API, DataStream API, ProcessFunction.
- Các cơ chế quan trọng: Event Time, Processing Time, Watermark, Window, State, Checkpoint, Savepoint.
- Tích hợp Kafka trong realtime transaction analytics.
- Exactly-once, fault tolerance, scaling, backpressure và hot key/data skew.
- So sánh Apache Flink, Spark Structured Streaming và Kafka Streams.

## Cấu trúc thư mục

```text
Flink/
├── demo/      # Source code và tài nguyên demo
├── docs/      # Tài liệu tham khảo/phụ trợ
├── q&a/       # Bộ câu hỏi phản biện và kiến thức ôn tập
├── report/    # Báo cáo seminar
├── slide/     # Slide thuyết trình
└── README.md
```

## Tài liệu nên đọc

### Slide

- `slide/slide.pdf`

File slide chính dùng khi thuyết trình seminar.

### Hỏi đáp phản biện demo

- `q&a/Bo_cau_hoi_phan_bien_Apache_Flink_Demo.md`
- `q&a/Bo_cau_hoi_phan_bien_Apache_Flink_Demo.pdf`

Bộ câu hỏi tập trung vào demo Kafka -> Flink -> realtime transaction analytics. Nên dùng để luyện các câu hỏi về partition, parallelism, task slot, keyBy, window, checkpoint, exactly-once và JobManager/TaskManager.

### Kiến thức tổng quát

- `q&a/Kien_thuc_tong_quat_Flink.md`
- `q&a/Kien_thuc_tong_quat_Flink.pdf`

Tài liệu ôn nền tảng ngoài demo, bao quát các chủ đề trong slide và các câu hỏi lý thuyết dễ bị hỏi khi phản biện.

## Cách ôn nhanh trước khi trình bày

1. Đọc `slide/slide.pdf` để nắm mạch trình bày.
2. Đọc `q&a/Kien_thuc_tong_quat_Flink.md` để chắc kiến thức nền.
3. Đọc `q&a/Bo_cau_hoi_phan_bien_Apache_Flink_Demo.md` để luyện trả lời câu hỏi sát demo.
4. Chạy demo và quan sát các phần quan trọng: Kafka topic/partition, Flink Job Graph, parallel subtasks, window output và completed checkpoints.

## Các ý cần nhớ khi phản biện

- Kafka lưu và phân phối stream, Flink xử lý stream.
- JobManager điều phối job, TaskManager xử lý dữ liệu thật.
- `keyBy(account_id)` tạo phân phối theo hash key và đảm bảo cùng account đi về cùng downstream subtask.
- Window giúp chia stream vô hạn thành các khoảng hữu hạn để tính toán.
- State là phần Flink phải nhớ trong lúc xử lý, ví dụ count/total trong window.
- Checkpoint lưu state và source progress để phục hồi nhất quán, không lưu toàn bộ dữ liệu Kafka.
- Exactly-once cần cả source, Flink state và sink cùng hỗ trợ đúng cơ chế.
- Tăng parallelism chỉ hiệu quả khi Kafka partitions, task slots, tài nguyên và sink không bị bottleneck.
- Nếu có hot key, `keyBy` không tự động chia một key sang nhiều subtasks.

## Mẫu trả lời tổng kết

Trong hệ thống demo, Kafka đóng vai trò nhận và lưu transaction stream theo topic/partition, còn Flink là engine xử lý realtime. Flink đọc dữ liệu từ Kafka, parse/filter, `keyBy(account_id)`, gom theo window và aggregate để tạo kết quả phân tích. JobManager điều phối job và checkpoint, còn TaskManager chạy subtasks và xử lý dữ liệu thật. Khi có lỗi, Flink restore từ checkpoint gần nhất và đọc lại Kafka từ offset đã lưu. Nếu scale từ laptop lên cluster, logic xử lý có thể giữ nguyên, nhưng cần tăng TaskManagers, task slots, parallelism và Kafka partitions phù hợp.
