from types import SimpleNamespace


class FakeRabbit:
    def __init__(self):
        self.queues = {}
        self.next_delivery_tag = 1

    def connection(self):
        return FakeConnection(self)


class FakeConnection:
    def __init__(self, broker):
        self.broker = broker
        self.is_open = True

    def channel(self):
        return FakeChannel(self.broker)

    def close(self):
        self.is_open = False


class FakeChannel:
    def __init__(self, broker):
        self.broker = broker
        self.inflight = {}

    def queue_declare(self, queue, **_):
        self.broker.queues.setdefault(queue, [])

    def confirm_delivery(self):
        return None

    def basic_publish(self, routing_key, body, **_):
        self.broker.queues.setdefault(routing_key, []).append(body)
        return True

    def basic_qos(self, **_):
        return None

    def basic_get(self, queue, **_):
        messages = self.broker.queues.setdefault(queue, [])
        if not messages:
            return None, None, None
        tag = self.broker.next_delivery_tag
        self.broker.next_delivery_tag += 1
        body = messages[0]
        self.inflight[tag] = (queue, body)
        return SimpleNamespace(delivery_tag=tag), None, body

    def basic_ack(self, delivery_tag):
        queue, body = self.inflight.pop(delivery_tag)
        self.broker.queues[queue].remove(body)

    def basic_nack(self, delivery_tag, requeue):
        queue, body = self.inflight.pop(delivery_tag)
        self.broker.queues[queue].remove(body)
        if requeue:
            self.broker.queues[queue].append(body)
        else:
            self.broker.queues.setdefault(f"{queue}.dlq", []).append(body)
