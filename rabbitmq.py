import pika
import sys


class RabbitMQ:
    def __init__(self, host: str):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()

    def __del__(self):
        self.connection.close()

    def fanout_enqueue(self):
        channel = self.channel
        channel.exchange_declare(
            exchange='logs',
            exchange_type='fanout'
        )

        message: bytes = (' '.join(sys.argv[1:]) or "info: Hello World!").encode("utf-8")

        channel.basic_publish(
            exchange='logs',
            routing_key='',
            body=message
        )
        print(" [x] Sent %r" % (message,))

    def fanout_dequeue(self):
        channel = self.channel
        channel.exchange_declare(
            exchange='logs',
            exchange_type='fanout'
        )

        queue_name = channel.queue_declare(queue="adhoc", exclusive=True).method.queue

        channel.queue_bind(
            exchange='logs',
            queue=queue_name  # 取得臨時隊列名稱
        )

        channel.basic_consume(
            on_message_callback=lambda ch, method, properties, body: print(" [x] %r" % (body,)),
            queue=queue_name,
            auto_ack=False
        )
        channel.start_consuming()

    # 使用
    # 如果你想保存error與warning兩個層級的log的話，那就可以這樣執行
    # python receive_logs_direct.py warning error > logs_from_rabbit.log

    # 如果你希望所有層級的log都顯示在console上，就可以這樣執行
    # python receive_logs_direct.py info warning error

    # 開啟兩個Consumer之後，就可以來發送訊息了
    # python emit_log_direct.py error "Run. Run. Or it will explode."
    def direct_enqueue(self):
        channel = self.channel
        channel.exchange_declare(
            exchange='direct_logs',
            exchange_type='direct'
        )

        severity = sys.argv[1] if len(sys.argv) > 2 else 'info'  # 取得外部參數第一索引當log等級
        message: bytes = (' '.join(sys.argv[2:]) or 'Hello World!').encode("utf-8")

        channel.basic_publish(
            exchange='direct_logs',
            routing_key=severity,
            body=message
        )
        print(" [x] Sent %r:%r" % (severity, message))

    def direct_dequeue(self):
        channel = self.channel
        channel.exchange_declare(
            exchange='direct_logs',
            exchange_type='direct'
        )
        #  設置關閉之後刪除匿名隊列
        queue_name = channel.queue_declare(queue="adhoc", exclusive=True).method.queue
        severities: list = sys.argv[1:] if not sys.argv[1:] else []  # 取得log層級設置

        for severity in severities:
            channel.queue_bind(
                exchange='direct_logs',
                queue=queue_name,
                routing_key=severity
            )

        channel.basic_consume(
            on_message_callback=lambda ch, method, properties, body: print(" [x] %r:%r" % (method.routing_key, body)),
            queue=queue_name,
            auto_ack=False
        )
        channel.start_consuming()

    # 使用
    # 接收所有訊息
    # python receive_logs_topic.py "#"

    # 接收來自設置kern的訊息
    # python receive_logs_topic.py "kern.*"

    # 接收層級為critical的訊息
    # python receive_logs_topic.py "*.critical"

    # 接收設備為kerns以及層級為critical的訊息
    # python receive_logs_topic.py "kern.*" "*.critical"

    # 發送kern設備並且層級為critical的訊息
    # python emit_log_topic.py "kern.critical" "A critical kernel error"
    def topic_enqueue(self):
        channel = self.channel

        channel.exchange_declare(
            exchange='topic_logs',
            exchange_type='topic'
        )
        routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
        message: bytes = (' '.join(sys.argv[2:]) or 'Hello World!').encode("utf-8")

        channel.basic_publish(
            exchange='topic_logs',
            routing_key=routing_key,
            body=message
        )
        print(" [x] Sent %r:%r" % (routing_key, message))

    def topic_dequeue(self):
        channel = self.channel
        channel.exchange_declare(
            exchange='topic_logs',
            exchange_type='topic'
        )

        queue_name = channel.queue_declare(queue="adhoc", exclusive=True).method.queue
        binding_keys: list = sys.argv[1:]

        for binding_key in binding_keys:
            channel.queue_bind(
                exchange='topic_logs',
                queue=queue_name,
                routing_key=binding_key
            )

        channel.basic_consume(
            on_message_callback=lambda ch, method, properties, body: print(" [x] %r:%r" % (method.routing_key, body)),
            queue=queue_name,
            auto_ack=False
        )
        channel.start_consuming()
