# BÁO CÁO SEMINAR BIG DATA

## Apache Flink: Tổng quan, kiến trúc và ứng dụng thực tế

**Nhóm DP1 – CSC14118**

---

## Tóm tắt

Apache Flink là một nền tảng xử lý dữ liệu phân tán dành cho cả streaming và batch, được thiết kế để đáp ứng nhu cầu xử lý dữ liệu thời gian thực với độ trễ thấp, thông lượng cao và đảm bảo tính chính xác thông qua cơ chế checkpointing và exactly-once semantics. Báo cáo này trình bày bối cảnh ra đời của Flink, kiến trúc hệ thống, các cơ chế cốt lõi như event time, watermark, windowing, state management và fault tolerance, đồng thời so sánh Flink với các công cụ tương đương như Apache Spark Structured Streaming và Apache Kafka Streams. Ngoài ra, báo cáo cũng giới thiệu một kịch bản demo thực tế về phát hiện giao dịch thẻ tín dụng bất thường.

---

## Mục lục

1. [Lời mở đầu](#1-lời-mở-đầu)
2. [Tổng quan về Apache Flink](#2-tổng-quan-về-apache-flink)
3. [Kiến trúc và cơ chế hoạt động](#3-kiến-trúc-và-cơ-chế-hoạt-động)
4. [So sánh với các công cụ tương đương](#4-so-sánh-với-các-công-cụ-tương-đương)
5. [Kịch bản demo](#5-kịch-bản-demo)
6. [Kết luận](#6-kết-luận)
7. [Tài liệu tham khảo](#7-tài-liệu-tham-khảo)

---

## 1. Lời mở đầu

Trong bối cảnh dữ liệu được sinh ra liên tục và cần được xử lý ngay khi xuất hiện, các mô hình xử lý batch truyền thống như Hadoop MapReduce dần trở nên không đủ sức đáp ứng yêu cầu về độ trễ. Nhu cầu xử lý dữ liệu thời gian thực đã thúc đẩy sự xuất hiện của nhiều công cụ mới, trong đó Apache Flink là một trong những nền tảng nổi bật nhất. Flink không chỉ hỗ trợ stream processing mà còn coi batch processing như một trường hợp đặc biệt của stream bị giới hạn, tạo nên một mô hình xử lý thống nhất và linh hoạt.

Báo cáo này được thực hiện nhằm hệ thống hóa kiến thức về Apache Flink từ góc nhìn tổng quan, kiến trúc, kỹ thuật cốt lõi và ứng dụng thực tế. Nội dung được xây dựng phù hợp với mục tiêu của đề bài seminar, chú trọng vào các khía cạnh mà nhóm cần hiểu sâu thay vì chỉ liệt kê tên gọi.

---

## 2. Tổng quan về Apache Flink

### 2.1. Bối cảnh ra đời

Để hiểu vì sao Apache Flink ra đời, cần phân biệt hai mô hình xử lý dữ liệu cơ bản trong Big Data:

| Mô hình                                                       | Đặc điểm                                                                                                                                                                                                                       | Ví dụ                                                                                                                                 |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Batch Processing** (Xử lý theo lô)                          | Xử lý tập dữ liệu hữu hạn (Bounded Data) đã được thu thập đầy đủ và lưu trữ từ trước. Phù hợp với các báo cáo cuối kỳ hoặc phân tích offline.                                                                                  | Quản lý ngân hàng tổng hợp dữ liệu cuối tháng để xuất báo cáo số lượng chi phiếu bị hủy trong 30 ngày qua.                            |
| **Real-time / Stream Processing** (Xử lý dòng thời gian thực) | Xử lý dòng dữ liệu vô hạn (Unbounded Data) liên tục sinh ra theo thời gian thực, trả về kết quả ngay lập tức khi sự kiện vừa xuất hiện. Phù hợp với các hệ thống giám sát, cảnh báo, đề xuất nội dung hoặc phát hiện gian lận. | Hệ thống phân tích giao dịch và tự động gửi cảnh báo gian lận thẻ tín dụng cho khách hàng chỉ vài mili-giây sau khi giao dịch xảy ra. |

#### Bảng giải thích thuật ngữ cơ bản

| Thuật ngữ tiếng Anh          | Ý nghĩa & Giải thích chi tiết                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------------- |
| **Bounded Data**             | Dữ liệu có giới hạn: Có điểm bắt đầu và kết thúc rõ ràng (ví dụ: file log của ngày hôm qua).    |
| **Unbounded Data**           | Dữ liệu không giới hạn: Dòng chảy dữ liệu liên tục, không có điểm kết thúc (ví dụ: sensor IoT). |
| **Latency** (Độ trễ)         | Khoảng thời gian từ khi dữ liệu sinh ra cho đến khi hệ thống tính toán xong và trả về kết quả.  |
| **Throughput** (Thông lượng) | Số lượng bản ghi/sự kiện mà hệ thống có thể xử lý trong 1 giây (ví dụ: 1M/sec).                 |

#### Lịch sử tiến hóa kỹ thuật

| Thế hệ       | Công nghệ                       | Đặc điểm chính                                                                                                                                                                                                                                                                                                             |
| ------------ | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Thế hệ 1** | Apache Hadoop (MapReduce)       | Chuyên xử lý Batch, độ trễ rất cao (tính bằng giờ/ngày).                                                                                                                                                                                                                                                                   |
| **Thế hệ 2** | Apache Storm                    | Xử lý Stream thời gian thực đầu tiên, độ trễ thấp nhưng không đảm bảo tính chính xác của dữ liệu (At-most-once — có thể bị mất dữ liệu khi sự cố xảy ra).                                                                                                                                                                  |
| **Thế hệ 3** | Apache Spark                    | Xử lý Batch cực nhanh nhờ bộ nhớ RAM. Để xử lý Stream, Spark sử dụng cơ chế **Micro-batching** (gom các sự kiện xảy ra trong khoảng thời gian ngắn như 1–2 giây thành một lô nhỏ để xử lý).                                                                                                                                |
| **Thế hệ 4** | **Apache Flink** (Stream-First) | Ra đời từ dự án nghiên cứu Stratosphere tại Đại học Berlin (2011), gia nhập Apache năm 2014. Flink tiếp cận hoàn toàn khác biệt: **Natively Stream Processing** — coi mọi dữ liệu bản chất đều là dòng (stream). Batch processing chỉ là một trường hợp đặc biệt của stream khi dòng dữ liệu bị giới hạn (bounded stream). |

### 2.2. Đặc điểm nổi bật

Apache Flink có các ưu điểm trọng tâm sau:

- **Độ trễ rất thấp** (mili-giây), phù hợp cho xử lý dữ liệu thời gian thực.
- **Thông lượng cao**, có khả năng xử lý lượng sự kiện lớn trong thời gian ngắn.
- Hỗ trợ xử lý theo **event time** và **watermark**, giúp xử lý dữ liệu đến muộn hoặc không đúng thứ tự hiệu quả.
- Có cơ chế **stateful processing**, cho phép hệ thống nhớ trạng thái giữa các sự kiện, với khả năng lưu trữ State hàng Terabyte nhờ RocksDB.
- Hỗ trợ **fault tolerance** bằng checkpoint và savepoint, đảm bảo **exactly-once semantics** khi xảy ra lỗi.

### 2.3. Mức độ phổ biến trong cộng đồng Big Data

Flink hiện được nhiều công ty lớn sử dụng cho các hệ thống dữ liệu thời gian thực, bao gồm **Alibaba, Uber, Netflix, LinkedIn, Spotify** và **Lyft**. Những hệ thống này thường có nhu cầu xử lý hàng tỷ sự kiện mỗi ngày, từ phát hiện gian lận, tính toán giá cước xe công nghệ (surge pricing), cho đến phân tích hành vi người dùng và đề xuất nội dung.

> **Đáng chú ý**: Alibaba từng fork dự án Flink thành bản nội bộ tên là **Blink** để tối ưu riêng cho việc xử lý hàng tỷ sự kiện mỗi giây trong các ngày hội mua sắm lớn. Sau đó Alibaba đã đóng góp toàn bộ mã nguồn tối ưu này ngược lại cho cộng đồng Apache Flink.

### 2.4. Mã nguồn mở và chính sách giá

Apache Flink là phần mềm mã nguồn mở phát hành theo giấy phép **Apache License 2.0**, hoàn toàn miễn phí khi tự tải, cài đặt và vận hành trên hạ tầng tự quản lý (On-premise / Cloud VM).

Trong thực tế, chi phí triển khai không chỉ nằm ở phần mềm mà còn ở việc vận hành cụm phân tán và bảo trì hệ thống. Vì vậy, nhiều doanh nghiệp lựa chọn các dịch vụ quản lý thương mại:

| Dịch vụ                                           | Mô tả                                                                                                                   |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Ververica Platform**                            | Công ty do chính các kỹ sư sáng lập ra Flink thành lập (hiện thuộc Alibaba), cung cấp giải pháp Flink cho doanh nghiệp. |
| **Amazon Managed Service for Apache Flink** (AWS) | Tự động scale và quản lý cụm Flink trên hạ tầng AWS, tính phí theo kPU (Flink Processing Units/giờ).                    |
| **Confluent Cloud**                               | Tích hợp Flink SQL trực tiếp trên hạ tầng Kafka quản lý.                                                                |

---

## 3. Kiến trúc và cơ chế hoạt động

### 3.1. Hệ sinh thái của Apache Flink

Hệ sinh thái Flink được chia thành 4 tầng kiến trúc từ dưới lên trên:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        APIs & High-Level Libraries                     │
│  ┌──────────────────┬─────────────────┬──────────────┬──────────────┐  │
│  │ DataStream API   │ Table API / SQL │ Flink CEP    │ Gelly (Graph)│  │
│  └──────────────────┴─────────────────┴──────────────┴──────────────┘  │
├────────────────────────────────────────────────────────────────────────┤
│                          KERNEL (Runtime Layer)                        │
│  Core Streaming Engine (Distributed, Fault-tolerant, Memory Mgmt)     │
├────────────────────────────────────────────────────────────────────────┤
│                         DEPLOYMENT (Resource Mgmt)                     │
│  ┌───────────────┬──────────────────┬──────────────┬────────────────┐  │
│  │ Local Mode    │ Standalone Cluster│ Apache YARN  │ Kubernetes/Mesos│ │
│  └───────────────┴──────────────────┴──────────────┴────────────────┘  │
├────────────────────────────────────────────────────────────────────────┤
│                            STORAGE / SOURCES                           │
│  ┌───────────────┬──────────────────┬──────────────┬────────────────┐  │
│  │ HDFS / S3     │ Kafka / RabbitMQ │ HBase / Mongo│ RDBMS (JDBC)   │  │
│  └───────────────┴──────────────────┴──────────────┴────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

#### Giải thích các tầng:

| Tầng                                       | Mô tả                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Storage Layer** (Lớp lưu trữ)            | Nơi Flink đọc dữ liệu đầu vào (Source) và ghi dữ liệu đầu ra (Sink). Dữ liệu có thể đến từ File System (HDFS, S3), NoSQL (HBase, MongoDB), RDBMS (MySQL, Postgres qua JDBC) hoặc Messaging Queues (Apache Kafka, RabbitMQ).                                                                                                                                             |
| **Deployment Layer** (Lớp triển khai)      | **Local Mode**: Chạy trên 1 máy tính đơn lẻ trong 1 JVM, chủ yếu dùng để phát triển/debug. **Standalone Cluster**: Sử dụng trình quản lý tài nguyên mặc định của Flink. **YARN / Kubernetes / Mesos**: Tích hợp vào các trình quản lý tài nguyên phân tán để tự động cấp phát bộ nhớ, CPU.                                                                              |
| **Kernel Layer** (Lớp lõi)                 | Lớp quan trọng nhất, thực thi tính toán phân tán, quản lý bộ nhớ, điều phối luồng dữ liệu, đảm bảo fault tolerance và khôi phục trạng thái khi sự cố xảy ra.                                                                                                                                                                                                            |
| **APIs & Libraries Layer** (Lớp giao diện) | **DataStream API**: API cốt lõi xử lý luồng dữ liệu thời gian thực. **Table API & SQL**: Cho phép viết truy vấn dạng SQL declarative trên cả stream và batch. **Flink CEP**: Thư viện phát hiện các chuỗi mẫu sự kiện phức tạp (ví dụ: user nhập sai mật khẩu 3 lần trong 1 phút rồi chuyển tiền > $1.000). **Gelly / Flink ML**: Thư viện tính toán đồ thị và học máy. |

### 3.2. Mô hình lập trình DAG

Một chương trình Flink có thể được xem như một đồ thị có hướng không chu trình (**DAG — Directed Acyclic Graph**), mô tả luồng đi của dữ liệu qua các bước tính toán từ Source đến Sink, đảm bảo không tạo thành vòng lặp kín.

```
┌────────────┐     filter()     ┌───────────────┐     window()     ┌────────────┐
│ Kafka Source│ ───────────────>│ Transformation│ ───────────────>│ Sink (DB)  │
└────────────┘                  └───────────────┘                  └────────────┘
```

| Thuật ngữ          | Ý nghĩa                                                                        |
| ------------------ | ------------------------------------------------------------------------------ |
| **Source**         | Nơi dữ liệu đi VÀO Flink (ví dụ: Kafka topic).                                 |
| **Transformation** | Các bước xử lý và biến đổi dữ liệu (filter, map, keyBy, window, aggregate...). |
| **Sink**           | Nơi dữ liệu đi RA KHỎI Flink (ví dụ: Database, S3, Kafka topic khác).          |

### 3.3. Kiến trúc phân tán Master-Worker

Flink vận hành theo mô hình phân tán **Master-Worker** để xử lý khối lượng dữ liệu khổng lồ trên nhiều máy chủ cùng lúc:

```
                     ┌─────────────────────────────────────┐
                     │            Client Program           │
                     └──────────────┬──────────────────────┘
                                    │ Submit Dataflow Graph
                                    ▼
                     ┌─────────────────────────────────────┐
                     │            Dispatcher               │
                     │  (REST Interface / Web UI)          │
                     └──────────────┬──────────────────────┘
                                    │ Spawn
                                    ▼
                     ┌─────────────────────────────────────┐
                     │          JobManager (Master)        │
                     ├──────────────┬──────────────────────┤
                     │ResourceManager│     JobMaster        │
                     └──────────────┴──────────────────────┘
                                    │ Deploy Tasks
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│  TaskManager 1      │ │  TaskManager 2      │ │  TaskManager N      │
│  (Worker Node)      │ │  (Worker Node)      │ │  (Worker Node)      │
├──────────┬──────────┤ ├──────────┬──────────┤ ├──────────┬──────────┤
│ Slot 1   │ Slot 2   │ │ Slot 3   │ Slot 4   │ │ Slot N   │ Slot N+1 │
│ Task     │ Task     │ │ Task     │ Task     │ │ Task     │ Task     │
└──────────┴──────────┘ └──────────┴──────────┘ └──────────┴──────────┘
```

#### Vai trò của các thành phần:

| Thành phần                    | Vai trò                                                                                                                                                          |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Client**                    | Gửi Dataflow Graph (chương trình đã biên dịch) lên hệ thống.                                                                                                     |
| **Dispatcher**                | Cung cấp REST interface để nhận ứng dụng từ Client, khởi tạo Flink Web UI.                                                                                       |
| **ResourceManager**           | Quản lý các Task Slot trên cụm. Xin thêm tài nguyên từ YARN/K8s hoặc giải phóng tài nguyên thừa.                                                                 |
| **JobMaster**                 | Mỗi ứng dụng (Job) khi chạy sẽ có 1 JobMaster riêng để quản lý quá trình thực thi, lập lịch chạy task, theo dõi tiến trình và điều phối cơ chế Checkpoint.       |
| **TaskManager** (Worker Node) | Thực thi trực tiếp các phép toán (Operators) trên dữ liệu.                                                                                                       |
| **Task Slot**                 | Đơn vị tài nguyên nhỏ nhất trong 1 TaskManager (thường phân chia theo số nhân CPU). Các Task Slot chia sẻ tài nguyên JVM nhưng được cô lập bộ nhớ cho từng task. |

### 3.4. Thời gian và Watermark

#### Khái niệm thời gian (Time Semantics)

Trong xử lý luồng, việc xác định mốc thời gian của bản ghi quyết định tính chính xác của kết quả. Flink hỗ trợ 3 loại thời gian:

| Loại thời gian      | Mô tả                                                                                  | Ví dụ                                                                        |
| ------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **Event Time**      | Thời điểm sự kiện thực sự xảy ra tại thiết bị nguồn (ghi trong timestamp của bản ghi). | Điện thoại phát sự kiện click lúc 10:00:00.                                  |
| **Ingestion Time**  | Thời điểm bản ghi được nạp vào luồng Flink Source.                                     | —                                                                            |
| **Processing Time** | Thời điểm máy chủ Flink đang chạy code xử lý bản ghi đó.                               | Do nghẽn mạng, sự kiện lúc 10:00:00 mãi đến 10:05:00 mới tới Flink để xử lý. |

> **Event Time là loại thời gian quan trọng nhất trong thực tế.** Giả sử điện thoại của người dùng bị mất mạng 5 phút, khi có mạng lại, 100 sự kiện tạo ra trong 5 phút đó đồng loạt gửi về Flink cùng 1 giây. Nếu dùng **Processing Time**, Flink sẽ gom cả 100 sự kiện vào cùng 1 giây hiện tại — gây sai lệch bản chất dữ liệu. Dùng **Event Time** giúp Flink xếp lại đúng thời điểm sự kiện thực sự diễn ra.

#### Watermarks — Cơ chế xử lý dữ liệu đến trễ

Vì mạng có độ trễ, dữ liệu Event Time thường tới Flink không đúng thứ tự (out-of-order). **Watermark** là một bản ghi điều khiển đặc biệt (control signal) được nhúng trực tiếp vào dòng dữ liệu, mang giá trị mốc thời gian T. Một Watermark W(T) tuyên bố với hệ thống:

> _"Tôi tin rằng tất cả các sự kiện có Event Time ≤ T đều đã đến rồi. Hãy chốt kết quả tính toán cho các thời điểm ≤ T đi!"_

```
Events trong Stream:  [t=1]  [t=3]  [t=2]  [t=5]  [W(t=3)]  [t=4] ...
                                                    │
                                                    └───> "Tất cả event <= 3 đã đến đủ!"
```

Khi một sự kiện có t=2 xuất hiện sau khi Watermark W(t=3) đã đi qua (**late data**), Flink có các chính sách xử lý nâng cao:

- Bỏ qua và đẩy ra **Side Output**
- Mở lại cửa sổ để tính toán cập nhật kết quả

### 3.5. Windowing

Vì stream là vô hạn, không thể tính SUM hay AVG trên toàn bộ dòng dữ liệu, Flink cắt dòng dữ liệu thành các _"Cửa sổ thời gian"_ (Windows) để thực hiện tính toán:

#### 1. Tumbling Window (Cửa sổ cố định, không chồng lấp)

```
|-- 00:00-00:05 --|-- 00:05-00:10 --|-- 00:10-00:15 --|
```

- Các khoảng thời gian bằng nhau, không đè lên nhau.
- _Ví dụ_: Tính tổng doanh thu mỗi 5 phút một lần.

#### 2. Sliding Window (Cửa sổ trượt, có chồng lấp)

```
|------ Window 5 phút (Slide 1 phút) ------|
   |------ Window 5 phút (Slide 1 phút) ------|
```

- Các khoảng thời gian bằng nhau nhưng trượt sau mỗi bước ngắn, có khoảng chồng lấp.
- _Ví dụ_: Tính lượng xe lưu thông trong 5 phút gần nhất, cập nhật mỗi 10 giây.

#### 3. Session Window (Cửa sổ theo phiên hoạt động)

```
[Event--Event--Event] <--- Inactivity Gap ---> [Event--Event]
```

- Cửa sổ nhóm các sự kiện theo hành vi người dùng, tự động đóng lại khi không có hoạt động nào sau một khoảng thời gian chờ (Inactivity Gap).
- _Ví dụ_: Theo dõi 1 phiên truy cập ứng dụng, đóng phiên nếu người dùng không thao tác trong 15 phút.

### 3.6. Stateful Processing và State Management

| Thuật ngữ                 | Ý nghĩa                                                                                                                                      |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| **Stateless Computation** | Tính toán không trạng thái: Mỗi bản ghi xử lý hoàn toàn độc lập, không nhớ gì về quá khứ (ví dụ: đổi chữ thường thành chữ hoa).              |
| **Stateful Computation**  | Tính toán có trạng thái: Hệ thống phải ghi nhớ các bản ghi trước đó để tính toán bản ghi hiện tại (ví dụ: tính tổng tiền giỏ hàng hiện tại). |

Flink quản lý State phân tán theo hai loại:

| Loại State         | Mô tả                                                                                                                                                                                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Keyed State**    | Trạng thái được tự động phân chia (partition) theo khóa (Key). Ví dụ: khóa theo user_id, mọi giao dịch của User A được gom về đúng State của User A. Các kiểu dữ liệu State: ValueState (lưu 1 giá trị), ListState (lưu danh sách), MapState (lưu key-value). |
| **Operator State** | Trạng thái gắn liền với một luồng xử lý song song (parallel instance), không chia theo key. Thường dùng để lưu offset vị trí đọc dữ liệu từ Kafka.                                                                                                            |

#### State Backends (Nơi lưu trữ State):

| State Backend                   | Mô tả                                                                                                                                    |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **HashMapStateBackend**         | Lưu state trong bộ nhớ Heap RAM của Java. Truy xuất rất nhanh nhưng giới hạn dung lượng RAM.                                             |
| **EmbeddedRocksDBStateBackend** | Lưu state vào CSDL nhúng RocksDB trên ổ đĩa SSD của máy worker. Cho phép lưu trữ State khổng lồ (hàng Terabyte) vượt quá dung lượng RAM. |

### 3.7. Fault Tolerance và Exactly-Once Semantics

| Thuật ngữ                  | Ý nghĩa                                                                                                                                              |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Exactly-Once Semantics** | Đảm bảo mỗi sự kiện chỉ ảnh hưởng tới State nội bộ đúng một lần duy nhất. Dù sập máy, không bị tính thiếu (At-least-once) hay bỏ sót (At-most-once). |
| **Checkpoint**             | Cơ chế tự động chụp ảnh nhanh (snapshot) State của hệ thống theo chu kỳ và ghi xuống ổ đĩa bền vững (HDFS, S3).                                      |
| **Savepoint**              | Bản Checkpoint do người dùng chủ động kích hoạt trước khi dừng job để nâng cấp code hoặc migration.                                                  |

Để chụp ảnh nhanh toàn bộ hệ thống đang chạy phân tán mà không cần dừng luồng xử lý, Flink sử dụng thuật toán **Chandy-Lamport** biến thể bằng cách tiêm các **Checkpoint Barriers** vào dòng dữ liệu:

```
Stream: [Bản ghi 3] [Bản ghi 2]  ───>  [Barrier N]  ───>  [Bản ghi 1]
                                           │
                                           └───> Khi Barrier tới Operator nào,
                                                 Operator đó tự snapshot State
                                                 của mình xuống S3/HDFS.
```

> Khi một TaskManager bị sập, JobManager chỉ đạo toàn bộ cụm rollback về bản Checkpoint gần nhất và tiếp tục đọc lại dữ liệu từ offset tương ứng trong Kafka. Kết quả đảm bảo chuẩn xác tuyệt đối **Exactly-Once**. Đây là nền tảng cho các ứng dụng quan trọng như giao dịch tài chính hoặc hệ thống cảnh báo real-time.

### 3.8. Mối quan hệ với Apache Kafka

Trong các hệ thống Big Data thực tế, **Flink + Kafka** là bộ đôi tiêu chuẩn:

```
┌──────────────┐    Streaming Data    ┌──────────────┐    Alerts/Output    ┌──────────────┐
│ Apache Kafka │ ───────────────────> │ Apache Flink │ ───────────────────>│ Apache Kafka │
│ (Data Source)│  Offsets managed     │  (Engine)    │  Two-Phase Commit   │ (Data Sink)  │
└──────────────┘                      └──────────────┘                     └──────────────┘
```

| Thành phần                    | Vai trò                                                                            |
| ----------------------------- | ---------------------------------------------------------------------------------- |
| **Kafka** (Message Broker)    | Lưu trữ đệm dòng dữ liệu đầu vào với khả năng chịu lỗi cao và throughput khổng lồ. |
| **Flink** (Processing Engine) | Đọc dữ liệu từ Kafka Topic, tính toán state phức tạp với latency thấp.             |

#### End-to-End Exactly-Once:

Để đạt **End-to-End Exactly-Once**, Flink sử dụng cơ chế **Two-Phase Commit Protocol (2PC)** khi ghi dữ liệu ra Kafka Sink: chỉ khi Checkpoint thành công, Flink mới chính thức Commit giao dịch ra Kafka Sink, đảm bảo toàn bộ đường ống đạt mức chính xác tuyệt đối.

---

## 4. So sánh với các công cụ tương đương

| Tiêu chí So sánh                  | Apache Flink                                         | Apache Spark (Structured Streaming)              | Apache Kafka Streams                              |
| --------------------------------- | ---------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------- |
| **Mô hình kiến trúc**             | True Streaming (Event-by-Event)                      | Micro-batching / Continuous Processing (hạn chế) | True Streaming (Event-by-Event)                   |
| **Độ trễ (Latency)**              | Siêu thấp (mili-giây)                                | Trung bình (vài trăm mili-giây đến vài giây)     | Siêu thấp (mili-giây)                             |
| **Thông lượng (Throughput)**      | Rất cao                                              | Rất cao (nhờ gom batch)                          | Cao                                               |
| **Xử lý Event Time & Watermark**  | Xuất sắc, tích hợp sâu ở tầng lõi engine             | Tốt (mới được hỗ trợ ở các phiên bản gần đây)    | Cơ bản                                            |
| **Quản lý Trạng thái (State)**    | Rất mạnh (lưu RocksDB, scale hàng Terabyte)          | Có hỗ trợ nhưng tốn tài nguyên khi State lớn     | Tốt (dùng RocksDB nhúng)                          |
| **Batch Processing**              | Hỗ trợ tốt (Batch là trường hợp đặc biệt của Stream) | Rất mạnh (vua của mảng Batch Processing)         | ❌ Không hỗ trợ                                   |
| **Hệ sinh thái Machine Learning** | Trung bình (Flink ML đang phát triển)                | Rất xuất sắc (Spark MLlib khổng lồ)              | ❌ Không có                                       |
| **Yêu cầu hạ tầng**               | Độc lập (tự quản lý cluster hoặc YARN/K8s)           | Độc lập (tự quản lý cluster hoặc YARN/K8s)       | Bắt buộc phải chạy cùng Apache Kafka              |
| **Cách thức triển khai**          | Cụm phân tán (Cluster Application)                   | Cụm phân tán (Cluster Application)               | Nhúng trực tiếp như một Library Java vào ứng dụng |

### Nhận định khi chọn công cụ

| Công cụ                  | Khi nào nên chọn                                                                                                                                                                                                                                                                              |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Apache Flink**         | Bài toán yêu cầu **độ trễ siêu thấp (mili-giây)** và **độ chính xác tuyệt đối** (tài chính, ngân hàng, cảnh báo gian lận, game thời gian thực).<br>Cần xử lý dòng dữ liệu phức tạp có **State lớn hàng Terabyte** và tính toán theo **Event Time phức tạp** với dữ liệu đến trễ nghiêm trọng. |
| **Apache Spark**         | Doanh nghiệp đã xây dựng hạ tầng **Lakehouse/Data Warehouse** trên Spark.<br>Bài toán **kết hợp nặng giữa Batch Processing, Analytics và Machine Learning**, chấp nhận độ trễ vài giây.                                                                                                       |
| **Apache Kafka Streams** | Hạ tầng hiện tại đã có sẵn Apache Kafka.<br>Muốn xây dựng các **microservices nhỏ gọn**, chỉ cần include thư viện Java mà không muốn quản lý thêm một cụm phân tán phức tạp như Flink/Spark.                                                                                                  |

---

## 5. Kịch bản demo

### 5.1. Kịch bản: Phát hiện giao dịch thẻ tín dụng bất thường

```
┌────────────────────────────────────────────────────────────────────────┐
│                    KIẾN TRÚC DEMO PHÁT HIỆN GIAN LẬN                  │
│                                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐     │
│  │Data Generator│───>│ Kafka Topic  │───>│  Flink Job (CEP)     │     │
│  │(Python Script)│    │"transactions"│    │  - Pattern Detection │     │
│  └──────────────┘    └──────────────┘    │  - 5-min Sliding Win │     │
│                                           │  - Stateful Aggregation│   │
│                                           └──────────┬───────────┘     │
│                                                      │                  │
│                                                      ▼                  │
│                              ┌──────────────┐    ┌──────────────┐     │
│                              │  Alert Sink  │<───│   Output     │     │
│                              │(Console / DB)│    │(High-risk tx)│     │
│                              └──────────────┘    └──────────────┘     │
└────────────────────────────────────────────────────────────────────────┘
```

### 5.2. Logic xử lý

#### Input — Dữ liệu sinh ngẫu nhiên qua Python script, đẩy vào Kafka topic `transactions`:

```json
{"account_id": "A001", "amount":
```
