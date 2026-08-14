import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

USERS = ['user_001', 'user_002', 'user_003', 'user_004', 'user_005']
MERCHANTS = ['Shopee', 'Lazada', 'Grab', 'VinMart', 'ATM']

print('🚀 Bắt đầu gửi dữ liệu liên tục vào Kafka...')

count = 0
while True:
  user = random.choice(USERS)
  # Cứ mỗi 10 giao dịch thì tạo 1 giao dịch gian lận (> 10 triệu)
  if count % 10 == 0:
    amount = random.randint(15_000_000, 50_000_000)
  else:
    amount = random.randint(100_000, 2_000_000)

  data = {
      'user_id': user,
      'amount': amount,
      'ts': int(time.time() * 1000),
      'merchant': random.choice(MERCHANTS),
  }

  producer.send('transactions', data)
  producer.flush()

  count += 1
  print(f'[{count}] Đã gửi: {data}')
  time.sleep(0.2)  # Bắn 5 tin nhắn/giây