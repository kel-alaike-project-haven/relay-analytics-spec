from app.subscriber import PubSubSubscriber

if __name__ == "__main__":
    subscriber = PubSubSubscriber()
    subscriber.listen()
