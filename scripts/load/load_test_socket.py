import argparse
import random
import socket

example_metrics = [
    "airflow.scheduler.critical_section_duration:8.494474|ms",
    "airflow.executor.open_slots:32|g",
    "airflow.executor.queued_tasks:0|g",
    "airflow.executor.running_tasks:0|g",
    "airflow.scheduler_heartbeat:1|c",
    "airflow.pool.open_slots.default_pool:128|g",
    "airflow.pool.queued_slots.default_pool:0|g",
    "airflow.scheduler.critical_section_duration:8.162927|ms",
    "airflow.dag_processing.total_parse_time:11.112728387117386|g",
    "airflow.dagbag_size:22|g",
    "airflow.dag_processing.processes:1|c",
]


def send_metrics(host: str, port: int, num_messages: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    messages = [random.choice(example_metrics) for _ in range(num_messages)]
    for message in messages:
        print("sending ", message)
        sock.sendto(message.encode(), (host, port))

    sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send UDP messages to the host,port")
    parser.add_argument("-i", "--host", type=str, required=False, default="127.0.0.1", help="Host address")
    parser.add_argument("-p", "--port", type=int, required=False, default=8125, help="Host port")
    parser.add_argument("-n", "--num-messages", type=int, required=True, help="Number of messages to send")
    args = parser.parse_args()

    send_metrics(args.host, args.port, args.num_messages)
