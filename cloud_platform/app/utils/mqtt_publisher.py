import paho.mqtt.client as mqtt # type: ignore

BROKER_ADDRESS = "mosquitto"
BROKER_PORT = 8883
TOPIC_EV = "dataset/ev/stats"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe(TOPIC_EV)
        print(f"Subscribed to topic: {TOPIC_EV}")
    else:
        print(f"Failed to connect, return code {rc}")

class MqttPublisher:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = on_connect

    def connect(self):
        self.client.tls_set(ca_certs="certs/ca.crt")
        self.client.username_pw_set("username", "senha123")
        self.client.connect(BROKER_ADDRESS, BROKER_PORT, 60)

    def start(self):
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("Stopping subscriber...")
            self.client.disconnect()

    def publish(self, payload):
        self.client.publish(TOPIC_EV, payload)