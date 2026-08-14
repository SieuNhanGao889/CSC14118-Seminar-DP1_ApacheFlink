# Guide demo Apache Flink

File này dùng cho phần demo seminar. Mục tiêu là bám đúng yêu cầu: demo chức năng chính của Apache Flink thông qua một kịch bản làm việc với dữ liệu và hệ thống khác, không trình bày bước cài đặt trong lúc thuyết trình.

## Kịch bản demo khuyến nghị

### Real-time transaction analytics

Mô tả ngắn:

> Hệ thống sinh các giao dịch mua hàng theo thời gian thực. Kafka đóng vai trò ingestion/message broker để nhận transaction stream. Flink đọc stream từ Kafka, làm sạch dữ liệu, chia theo key, gom theo window thời gian, tính các chỉ số realtime và xuất kết quả ra console/alert/dashboard.

Luồng demo:

```text
Transaction Producer
  -> Kafka topic: transactions
  -> Flink Kafka Source
  -> parse JSON / validate records
  -> filter invalid transactions
  -> keyBy account_id / user_id / merchant_id
  -> tumbling window hoặc sliding window
  -> aggregate count, total_amount, average_amount
  -> output: console / alert / dashboard
```

Demo này thể hiện các chức năng chính của Flink:

- Đọc stream liên tục từ Kafka.
- Xử lý dữ liệu theo từng event, không phải batch một lần.
- Dùng `keyBy` để phân phối dữ liệu theo khóa.
- Dùng window để tính toán theo khoảng thời gian.
- Dùng state để giữ tổng tiền, số lượng giao dịch hoặc thông tin cảnh báo.
- Dùng checkpoint để bảo vệ state và hỗ trợ fault tolerance.
- Xuất kết quả realtime ra output dễ quan sát.

## Slide demo overview

Nên có đúng 1 slide trước khi chạy demo.

Nội dung slide:

```text
Transaction Producer -> Kafka -> Flink Job -> Real-time Output

Trong Flink Job:
Parse / Filter -> keyBy -> Window Aggregate -> Output

We will observe:
- Job graph & parallel subtasks
- Windowed output / alerts
- Successful checkpoints
```

Script nói 20-30 giây:

> Trước khi chạy demo, đây là workflow tổng quan. Producer sinh giao dịch liên tục vào Kafka. Flink đọc stream này, parse/filter, keyBy, gom theo window và xuất kết quả realtime. Trong demo, mình sẽ quan sát ba thứ: job graph và parallel subtasks trên Flink UI, output hoặc alert cập nhật theo thời gian thực, và checkpoint hoàn tất để thấy khả năng fault tolerance.

## Timeline demo 4-5 phút

| Thời gian | Màn hình nên mở | Nội dung nói |
|---|---|---|
| 0:00-0:30 | Slide demo overview | Giới thiệu workflow Kafka -> Flink -> realtime output. |
| 0:30-1:20 | Flink Web UI - Job Graph | Chỉ Source, operators, Sink. Nối với kiến trúc: JobManager điều phối, TaskManagers chạy subtasks. |
| 1:20-2:20 | Terminal producer + output | Gửi transaction vào Kafka, cho thấy output cập nhật theo stream/window. |
| 2:20-3:20 | Code pipeline hoặc Web UI subtasks | Chỉ nhanh pipeline: parse/filter, keyBy, window, aggregate. Không giải thích từng dòng code. |
| 3:20-4:20 | Flink Web UI - Checkpoints | Mở tab Checkpoints, chỉ checkpoint completed. Nói checkpoint bảo vệ state để recovery. |
| 4:20-5:00 | Output/alert cuối | Chốt lại: demo gom architecture, dataflow, state/window và checkpoint. |

## Script nói demo ngắn

### Mở demo

> Trước khi chạy demo, đây là workflow tổng quan. Producer sinh giao dịch liên tục vào Kafka. Flink đọc stream này, parse/filter, keyBy theo account hoặc merchant, gom theo window và xuất kết quả realtime như tổng giao dịch hoặc alert bất thường. Trong demo, mình sẽ quan sát job graph/subtasks trên Flink UI, output cập nhật theo stream và checkpoint completed.

### Khi mở Flink Web UI

> Trên Flink Web UI, đây là job graph của pipeline. Các operator trong graph được JobManager lập lịch thành subtasks và chạy trên TaskManagers. Như vậy phần kiến trúc vừa trình bày không chỉ là lý thuyết; nó chính là runtime đang thực thi dataflow này.

> Job graph trên Web UI chính là biểu diễn trực quan của pipeline Flink: source, các operator xử lý và sink. Ở phần kiến trúc mình đã nói chương trình được chuyển thành graph; ở đây mình sẽ cho mọi người thấy graph đó khi job đang chạy.

### Khi chạy producer và xem output

> Khi producer gửi thêm event mới, output được cập nhật liên tục theo stream. Với `keyBy`, các record cùng key được đưa về cùng downstream subtask, nên Flink có thể giữ state đúng cho từng user hoặc merchant. Với window, stream vô hạn được chia thành các khoảng thời gian hữu hạn để tính toán.

### Khi chỉ checkpoint

> Ở tab Checkpoints, ta thấy Flink tạo checkpoint định kỳ. Checkpoint lưu lại state của job, nên nếu có lỗi, Flink có thể khôi phục từ checkpoint gần nhất và tiếp tục xử lý từ vị trí phù hợp trong source.

### Kết demo

> Demo này thể hiện các chức năng chính của Flink: xử lý stream liên tục, dataflow graph, parallel execution, `keyBy`, window, state và fault tolerance bằng checkpoint.

## Hướng dẫn chuẩn bị môi trường để quay demo

Phần này dùng để chuẩn bị và quay video, không trình bày trong seminar.

### Cách triển khai khuyến nghị

Ưu tiên dùng Docker Compose để môi trường ổn định và dễ quay:

```text
Docker Compose
  -> Kafka
  -> Flink JobManager
  -> Flink TaskManager
  -> Transaction producer
  -> Flink job
```

Nếu chưa có Kafka hoặc sợ setup lâu, có thể dùng transaction generator trực tiếp làm source. Tuy nhiên kịch bản Kafka -> Flink vẫn ăn điểm hơn vì có hệ thống ingestion bên ngoài.

### Cần chuẩn bị

- Java/JDK phù hợp với project demo.
- Docker Desktop nếu chạy Kafka/Flink bằng container.
- Kafka topic `transactions`.
- Flink local cluster hoặc Flink containers.
- Một producer sinh transaction liên tục.
- Một Flink job đọc từ Kafka, xử lý và in output.
- Checkpoint được bật trong job.
- Flink Web UI mở được, thường ở `http://localhost:8081`.

### Dữ liệu transaction nên có

Mỗi event nên là JSON đơn giản:

```json
{
  "transaction_id": "tx-001",
  "account_id": "acc-01",
  "merchant_id": "m-01",
  "amount": 125.50,
  "event_time": "2026-08-14T10:00:01Z"
}
```

Nên chuẩn bị 2 nhóm event:

- Event bình thường để tạo aggregate theo window.
- Event bất thường, ví dụ nhiều giao dịch liên tiếp hoặc tổng tiền vượt ngưỡng, để tạo alert nếu demo có fraud detection.

### Cấu hình Flink job nên có

- Source: Kafka topic `transactions`.
- Transformation: parse JSON, validate/filter invalid records.
- Key: `account_id`, `user_id` hoặc `merchant_id`.
- Window: tumbling window ngắn, ví dụ 10 giây hoặc 30 giây để demo ra kết quả nhanh.
- Aggregate: `count`, `total_amount`, `avg_amount`.
- Optional alert rule: nếu `total_amount` trong window vượt ngưỡng thì output alert.
- Sink: console là đủ cho demo; dashboard/file/database chỉ dùng nếu đã có sẵn.
- Checkpoint interval: ngắn để dễ quan sát, ví dụ 10-30 giây.

## Checklist trước khi quay video

- [ ] Kafka/Flink/job chạy ổn ít nhất 2-3 phút.
- [ ] Producer sinh transaction liên tục.
- [ ] Output hiển thị rõ aggregate hoặc alert.
- [ ] Flink Web UI mở sẵn tab Job Graph.
- [ ] Flink Web UI mở sẵn tab Checkpoints và có checkpoint completed.
- [ ] Terminal font đủ lớn, khoảng 16-20px.
- [ ] Trình duyệt zoom 110-125% nếu chữ nhỏ.
- [ ] Ẩn thông tin không cần thiết trên desktop.
- [ ] Tắt thông báo hệ thống.
- [ ] Chuẩn bị sẵn câu mở và câu chốt demo.
- [ ] Quay thử 30 giây để kiểm tra âm thanh và độ rõ màn hình.

## Thứ tự quay video

1. Mở slide demo overview, nói workflow trong 20-30 giây.
2. Chuyển sang Flink Web UI, chỉ Job Graph.
3. Chuyển sang terminal producer, gửi hoặc cho thấy transaction đang được sinh.
4. Chuyển sang output, cho thấy kết quả window aggregate hoặc alert.
5. Quay lại Flink Web UI, chỉ subtasks/parallelism nếu có.
6. Mở tab Checkpoints, chỉ checkpoint completed.
7. Kết bằng câu: demo đã thể hiện stream processing, dataflow, parallel execution, state/window và checkpoint.

## Điểm cần tránh

- Không trình bày cài Docker, Java, Kafka, Flink trong demo chính.
- Không live code dài.
- Không để người nghe chỉ nhìn terminal quá nhỏ hoặc quá nhiều log.
- Không nói "Flink luôn exactly-once" nếu chưa giải thích điều kiện source/sink và checkpoint.
- Không để demo chỉ là đọc file local giống batch, vì sẽ không làm rõ thế mạnh streaming của Flink.
- Không crash container live nếu chưa luyện kỹ; chỉ cần show checkpoint completed là đủ.

## Nếu live demo bị lỗi

Luôn có video backup. Nếu live demo lỗi, nói ngắn:

> Do môi trường demo trực tiếp có vấn đề, nhóm em chuyển sang video đã quay sẵn. Phần xử lý trong video vẫn là cùng pipeline: Kafka nhận stream giao dịch, Flink đọc stream, xử lý `keyBy`/window/state và xuất kết quả.

Sau đó tiếp tục giải thích bằng video, không dừng lâu để sửa lỗi.
