import traceback
import asyncio
import os
import omni.client
from pxr import Usd, Sdf
from pathlib import Path
from paho.mqtt import client as mqtt_client
from omni.live import LiveEditSession, LiveCube, getUserNameFromToken
import random
import json
from datetime import datetime, timedelta
import re

OMNI_HOST = os.environ.get("OMNI_HOST", "localhost")
OMNI_USER = os.environ.get("OMNI_USER", "$omni-api-token")
OMNI_USER = getUserNameFromToken(os.environ.get("OMNI_PASS"))

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "azure-iot-operations/data/")
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
MQTT_PORT = os.environ.get("MQTT_PORT", "1883")
MQTT_CERT = os.environ.get("MQTT_CERT", "")
MQTT_KEY = os.environ.get("MQTT_KEY", "")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")

BASE_FOLDER = "omniverse://" + OMNI_HOST + "/Users/" + OMNI_USER + "/iot-samples"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONTENT_DIR = Path(SCRIPT_DIR).resolve().parents[1].joinpath("content")

messages = []

def log_handler(thread, component, level, message):
    print(message)
    messages.append((thread, component, level, message))

def sanitize_name(name):
    name = name.strip().strip('/')
    parts = name.split('/')
    sanitized_parts = [re.sub(r'[^a-zA-Z0-9_]', '_', part) for part in parts]
    sanitized_name = '/'.join(sanitized_parts)
    sanitized_name = re.sub(r'/(\d)', r'/_\1', sanitized_name)
    sanitized_name = sanitized_name.replace("-", "_")

    if not sanitized_name:
        raise ValueError("Invalid name: " + name)
    return sanitized_name.lower()

def ensure_prim_exists(stage, path, prim_type):
    if not path.startswith('/'):
        path = '/' + path
    print(f"Ensuring {path} exists")
    prim = stage.GetPrimAtPath(path)
    if not prim:
        print(f"Creating {path}")
        prim = stage.DefinePrim(path, prim_type)
    return prim

async def initialize_async(iot_topic):

    iot_topic = sanitize_name(iot_topic)
    stage_name = f"ConveyorBelt-{iot_topic}"
    local_folder = f"file:{CONTENT_DIR}/{stage_name}"
    stage_folder = f"{BASE_FOLDER}/{stage_name}"
    stage_url = f"{stage_folder}/{stage_name}.usd"
    result = await omni.client.copy_async(
        local_folder,
        stage_folder,
        behavior=omni.client.CopyBehavior.ERROR_IF_EXISTS,
        message="Copy Conveyor Belt",
    )


    print(f"Using stage URL: {stage_url}")

    stage_result, entry = omni.client.stat(stage_url)
    if stage_result == omni.client.Result.OK:
        print("USD stage found.")
        stage = Usd.Stage.Open(stage_url)
    else:
        print(f"USD stage at {stage_url} not found. Creating a new stage.")
        stage = Usd.Stage.CreateNew(stage_url)
        stage.Save()

    live_session = LiveEditSession(stage_url)
    live_layer = await live_session.ensure_exists()
    session_layer = stage.GetSessionLayer()
    session_layer.subLayerPaths.append(live_layer.identifier)
    stage.SetEditTarget(live_layer)

    return stage, live_layer

class OpcDeltaUsdWriter:

    def __init__(self, live_layer):
        self.live_layer = live_layer

    def open_stage(self):
        print("Opening stage")
        self.stage = Usd.Stage.Open(self.live_layer)

    def is_timestamp_recent(self, timestamp):
        try:
            timestamp_datetime = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
            current_time = datetime.now()
            time_difference = current_time - timestamp_datetime
            return time_difference <= timedelta(minutes=10)
        except Exception as e:
            print(f"Error parsing the timestamp: {e}")
            return True
        finally:
            print(f"Timestamp: {timestamp}")

    def write_to_opc_semantics(self, opc_topic, msg_content):
        print(f"Received message: {msg_content}")
        try:
            payload = json.loads(msg_content)
        except Exception as e:
            print(f"Error parsing the message content: {e}")
            return

        if not payload:
            raise Exception("Payload is missing from the message.")

        writer_name = payload.get("dataSetWriterName", "").replace(":", "_").replace(" ", "_")
        if not writer_name:
            raise Exception("dataSetWriterName is missing from the payload.")

        base_opc_path = f"iot/opc_delta/{opc_topic}/{writer_name}/"

        if not self.is_timestamp_recent(payload["TimeStamp"]):
            print(f"Timestamp is older than 10 minutes. Skipping the message.")
            return

        if not self.stage:
            self.open_stage()

        for property_id, property_data in payload["payload"].items():
            property_name = property_id.split(':')[-1].replace(';', '_').replace('.', '_')
            semantic_label = f"{property_name}"
            semantic_path = f"{base_opc_path}/{semantic_label}"
            semantic_path = sanitize_name(semantic_path)

            prim = ensure_prim_exists(self.stage, semantic_path, "Scope")

            for key, value in property_data.items():
                item_attr = prim.GetAttribute(key)
                if not item_attr:
                    item_attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.String, True)
                print(f"Setting {key} to {value}")
                item_attr.Set(str(value))

        omni.client.live_process()

def connect_mqtt(stage, iot_topic, live_layer):
    opc_delta_usd_writer = OpcDeltaUsdWriter(live_layer)
    topic = MQTT_TOPIC

    def on_message(client, userdata, msg):
        try:
            msg_content = msg.payload.decode()
            opc_delta_usd_writer.write_to_opc_semantics(iot_topic, msg_content)
            print(f"Received `{msg_content}` from `{msg.topic}` topic")
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_connect(client, userdata, flags, rc):
        print(f"Connected to MQTT broker {MQTT_HOST} on port {MQTT_PORT} as {MQTT_USER}")
        if rc == 0:
            print(f"Subscribing to topic: {topic}")
            client.subscribe(topic)
        else:
            print(f"Failed to connect, return code {rc}")

    def on_subscribe(client, userdata, mid, granted_qos):
        print(f"Subscribed {mid} {granted_qos}")

    def on_connect_fail(client, userdata, rc):
        print(f"Failed to connect to MQTT broker: {rc}")

    def on_log(client, userdata, level, buf):
        print(f"MQTT log: {buf}")

    client_id = f"ov-opc-ua-connector-{random.randint(0, 1000)}"
    if MQTT_USER:
        client_id = MQTT_USER

    client = mqtt_client.Client(client_id)
    client.enable_logger()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_connect_fail = on_connect_fail
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    client.on_log = on_log

    try:
        if MQTT_USER and MQTT_PASS:
            print(f"Using MQTT_USER {MQTT_USER} and password")
            client.username_pw_set(MQTT_USER, MQTT_PASS)
        elif MQTT_USER:
            print(f"Using MQTT_USER  without password {MQTT_USER}")
            client.username_pw_set(MQTT_USER)

        if os.path.isfile(MQTT_CERT) and os.path.isfile(MQTT_KEY):
            print(f"Using TLS certificate {MQTT_CERT} and key {MQTT_KEY}")
            client.tls_set(certfile=MQTT_CERT, keyfile=MQTT_KEY)
        else:
            print(f"TLS certificate or key not found. Using unencrypted connection. MQTT_CERT={MQTT_CERT} MQTT_KEY={MQTT_KEY}")

        if MQTT_USER:
            client.client_id = MQTT_USER + "-session"

        result = client.connect(MQTT_HOST, int(MQTT_PORT))
        if result != 0:
            print(f"Failed to connect to MQTT broker: {result}")
            return None

        client.loop_start()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")

    return client

def run(stage, live_layer, iot_topic):
    print(f"Connecting to {MQTT_HOST}....")
    mqtt_client = connect_mqtt(stage, iot_topic, live_layer)

async def main(iot_topic):
    omni.client.initialize()
    omni.client.set_log_level(omni.client.LogLevel.DEBUG)
    omni.client.set_log_callback(log_handler)

    try:
        stage, live_layer = await initialize_async(iot_topic)
        run(stage, live_layer, iot_topic)
        while True:
            await asyncio.sleep(1)  # A non-blocking wait. The magic number 1 can be adjusted as needed.
    except Exception as e:
        print('An mqtt exception occurred: ', str(e))
        traceback.print_exc()
        print('---- LOG MESSAGES ----')
        print(*messages, sep='\n')
        print('----')
    finally:
        omni.client.shutdown()

if __name__ == "__main__":
    # clear out the messages
    messages = []
    IOT_TOPIC = MQTT_TOPIC
    omni.client.initialize()
    omni.client.set_log_level(omni.client.LogLevel.DEBUG)
    omni.client.set_log_callback(log_handler)
    try:
        asyncio.run(main(IOT_TOPIC))
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        print('---- LOG MESSAGES ----')
        print(*messages, sep='\n')
        print('----')
        omni.client.shutdown()