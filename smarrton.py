import paho.mqtt.client as mqtt
import json
import csv
import time
from datetime import datetime
5
# MQTT configuration
MQTT_BROKER = "messages.nectarit.com"
MQTT_PORT = 1883
MQTT_TOPIC = "nectar/smarrton/A2526774A001/#"  #2454

MQTT_USERNAME = "rabbitadmin"
MQTT_PASSWORD = "rabbitadmin"

# CSV file setup
CSV_FILE = "mqtt_data.csv"

# Write CSV header if file is empty
def initialize_csv():
    try:
        with open(CSV_FILE, "x", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["equipment_name", "point", "value", "time"])
    except FileExistsError:
        pass  # File already exists, skip writing header

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        for key, entries in payload.items():
            parts = key.strip("/").split("/")
            if len(parts) >= 3:
                equipment_name = parts[1]
                point = parts[2]

                data = entries[0]  # Take first object
                value = data.get("v")
                timestamp_ms = data.get("ts")
                readable_time = datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

                # Write to CSV
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([equipment_name, point, value, readable_time])
                
                print(f"Written: {equipment_name}, {point}, {value}, {readable_time}")
    except Exception as e:
        print(f"Failed to process message: {e}")

def main():
    initialize_csv()
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()