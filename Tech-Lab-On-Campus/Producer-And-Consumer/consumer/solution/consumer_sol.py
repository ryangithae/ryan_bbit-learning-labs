from consumer_interface import mqConsumerInterface
import pika 
import os

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ Service
        conParams = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection  = pika.BlockingConnection(parameters=conParams)

        # Establish Channel
        self.channel = self.connection.channel()
        
        #Create Queue
        self.channel.queue_declare(queue= self.queue_name)

        #Create an exchange
        exchange = self.channel.exchange_declare(exchange= self.exchange_name)

        #Bind Binding key to Queue on the exchange
        self.channel.queue_bind(queue= self.queue_name, routing_key= self.binding_key, exchange= self.exchange_name)

        #Callback function for receiving messages
        self.channel.basic_consume(self.queue_name, self.on_message_callback, auto_ack= False)

    def on_message_callback(self, channel, method_frame, header_frame, body):
        #Acknowledge the message
        self.channel.basic_ack(method_frame.delivery_tag, False)

        #Print Message
        print(body)

    def startConsuming(self):
        print("[*] Waiting for messges. To exit press CTRL+C")

        #Start Consuming Messges
        self.channel.start_consuming()
        
    def __del__(self):
        print("Closing RMQ connection on destruction")

        #Close channel and connection
        self.channel.close()
        self.connection.close()

