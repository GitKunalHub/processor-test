import subprocess
import sys
import time
import signal
import json
import redis
from urllib.parse import quote_plus
import os
import pika  # For waiting on the interactions message
from producer import (
    wait_for_rabbitmq,
    declare_queues,
    wait_for_main_queue_empty
)

class Config:
    # Azure File Storage
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER")
    RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
    RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
    REDIS_PORT = os.getenv("REDIS_PORT")
    REDIS_HOST = os.getenv("REDIS_HOST")
    MONGO_USER = quote_plus(os.getenv("MONGODB_USER"))
    MONGO_PASS = quote_plus(os.getenv("MONGODB_PASS"))
    MONGO_HOST = os.getenv("MONGODB_HOST")
    MONGO_PORT = os.getenv("MONGODB_PORT")
    MONGO_AUTH_SOURCE = quote_plus(os.getenv("MONGODB_AUTH_SOURCE"))

    MONGODB_URI = (
        f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    )
    MONGODB_DB = os.getenv("MONGODB_DB")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

def set_processor_availability(available):
    r = redis.Redis(host='redis', port=6379)
    r.set('processor_available', '1' if available else '0')

def wait_for_redis():
    """Wait for Redis to become available."""
    print("🌟 Waiting for Redis to be ready...")
    r = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT)
    start = time.time()
    timeout = 30
    while True:
        try:
            if r.ping():
                print("Redis is up!")
                return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Timeout waiting for Redis") from e
            print("Waiting for Redis...")
            time.sleep(2)

def wait_for_interactions_ready():
    """Wait for the 'Interactions ready' message containing agent_id."""
    print("🌟 Waiting for interactions-ready message...")
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=Config.RABBITMQ_HOST, port=Config.RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    start = time.time()
    agent_id = None
    
    while True:
        method_frame, _, body = channel.basic_get(queue='processor_queue', auto_ack=True)
        if method_frame:
            try:
                message = json.loads(body.decode('utf-8'))
                agent_id = message.get('agent_id')
                if agent_id:
                    print(f"✅ Received interactions ready for agent {agent_id}")
                    connection.close()
                    return agent_id
                else:
                    print("⚠️ Message missing agent_id")
            except json.JSONDecodeError:
                print(f"⚠️ Invalid message format: {body}")
        # if time.time() - start > timeout:
            # connection.close()
            # raise Exception("Timeout waiting for interactions-ready message.")
        time.sleep(1)

def main():
    processes = []
    try:
        set_processor_availability(True)
        print("Initial: Processor Availability = True")
        

        print("🌟 Waiting for infrastructure...")
        wait_for_rabbitmq(Config.RABBITMQ_HOST, Config.RABBITMQ_PORT)
        wait_for_redis()

        # Start Celery workers once (persistent)
        print("🚀 Starting Celery workers...")
        main_worker = subprocess.Popen([
            "celery", "-A", "tasks", "worker",
            "--loglevel=info", "-Q", "main_queue", "--concurrency=4"
        ])
        processes.append(main_worker)

        dlq_worker = subprocess.Popen([
            "celery", "-A", "tasks", "worker",
            "--loglevel=info", "-Q", "custom_dlq", "--concurrency=1"
        ])
        processes.append(dlq_worker)

        while True:  # Continuous processing loop
            agent_id = wait_for_interactions_ready()
            set_processor_availability(False)

            # Reset queues for new batch
            declare_queues()  

            print("📤 Starting producer for new batch...")
            subprocess.run([sys.executable, "-m", "producer", agent_id], check=True)
            
            print("⏳ Waiting for queue drain...")
            wait_for_main_queue_empty()
            set_processor_availability(True)

    except Exception as e:
        print(f"Error: {e}")
        set_processor_availability(False)
        sys.exit(1)
    finally:
        for p in processes: p.terminate()

if __name__ == "__main__":
    main()
