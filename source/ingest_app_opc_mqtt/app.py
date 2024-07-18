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
    # Strip leading/trailing whitespace and slashes, replace non-alphanumeric characters, and replace hyphens
    sanitized_name = re.sub(r'[^a-zA-Z0-9/_-]', '_', name.strip().strip('/')).replace('-', '_')
    # Ensure slashes are followed by underscores before digits
    sanitized_name = re.sub(r'/(\d)', r'/_\1', sanitized_name)
    # remove any empty levels e.g. /_/ or /_
    sanitized_name = re.sub(r'/(_)+', '/', sanitized_name)
    # Replace double slashes with single slashes
    sanitized_name = sanitized_name.replace("//", "/")
    # Raise an error if the sanitized name is empty
    if not sanitized_name:
        raise ValueError("Invalid name: " + name)

    # Return the sanitized name in lowercase
    return sanitized_name.lower()


def ensure_prim_exists(stage, path, prim_type):
    # Ensure the path starts with a '/'x1
    if not path.startswith('/'):
        path = '/' + path
    print(f"Ensuring {path} exists")
    # Try to get the prim at the specified path
    prim = stage.GetPrimAtPath(path)
    # If the prim does not exist, create it with the specified type
    if not prim:
        print(f"Creating {path}")
        prim = stage.DefinePrim(path, prim_type)
    # Return the prim (existing or newly created)
    return prim


async def initialize_async(iot_topic):
    original_usd = f"ConveyorBelt_A08_PR_NVD_01"
    iot_topic = sanitize_name(iot_topic)
    stage_name = f"{iot_topic.split('/')[-1]}_ConveyorBelt"
    # hardcoding local dir for now
    local_folder = f"file:{CONTENT_DIR}/{original_usd}"
    stage_folder = f"{BASE_FOLDER}/{stage_name}"
    stage_url = f"{stage_folder}/{stage_name}.usd"
    source_usd = f"{stage_folder}/{original_usd}.usd"
    stage_url = source_usd.replace(f"{original_usd}.usd", stage_name+".usd")

    dest_result, entry = omni.client.stat(stage_url)
    if dest_result != omni.client.Result.OK:
        print(f"Copying {local_folder} to {stage_folder}")
        result = await omni.client.copy_async(
            local_folder,
            stage_folder,
            behavior=omni.client.CopyBehavior.OVERWRITE,
            message="Copy Conveyor Belt",
        )
        if(result != omni.client.Result.OK):
            raise Exception(f"Failed to copy {local_folder} to {stage_folder}")

        print(f" copying usd {source_usd} to {stage_url}")
        await omni.client.copy_async(source_usd, stage_url, behavior=omni.client.CopyBehavior.OVERWRITE, message="Rename USD")
        # remove old usd
        await omni.client.delete_async(source_usd)

    print(f"Using stage URL: {stage_url}")

    try:

        stage_result, entry = omni.client.stat(stage_url)
        if stage_result == omni.client.Result.OK:
            print("USD stage found.")
            stage = Usd.Stage.Open(stage_url, load=Usd.Stage.LoadNone)
        else:
            print(f"USD stage at {stage_url} not found. Creating a new stage.")
            stage = Usd.Stage.CreateNew(stage_url)
            stage.Save()
    except Exception as e:
        print(f"Error opening USD stage: {e}")
        raise

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

    def write_to_opc_semantics(self, opc_topic, msg_content):
        # Print the received message content
        print(f"Received message: {msg_content}")

        try:
            # Attempt to parse the message content as JSON
            payload = json.loads(msg_content)
        except Exception as e:
            # Print an error message if JSON parsing fails
            print(f"Error parsing the message content: {e}")
            return

        # Check if the payload is missing
        if not payload:
            raise Exception("Payload is missing from the message.")

        # Extract and sanitize the DataSetWriterName from the payload
        writer_name = payload.get("DataSetWriterName", "").replace(":", "_").replace(" ", "_")
        if not writer_name:
            raise Exception("DataSetWriterName is missing from the payload.")

        # Construct the base OPC path using the topic and writer name
        base_opc_path = f"iot/opc_delta/{opc_topic}/{writer_name}/"

        # Open the stage if it is not already open
        if not self.stage:
            self.open_stage()

        # Iterate over each property in the payload
        for property_id, property_data in payload["Payload"].items():
            # Sanitize and construct the semantic path for the property
            property_name = property_id.split(':')[-1].replace(';', '_').replace('.', '_')
            semantic_label = f"{property_name}"
            semantic_path = f"{base_opc_path}/{semantic_label}"
            semantic_path = sanitize_name(semantic_path)

            # Ensure the prim exists at the semantic path, create if necessary
            prim = ensure_prim_exists(self.stage, semantic_path, "Scope")

            # Iterate over each key-value pair in the property data
            for key, value in property_data.items():
                # Get or create the attribute on the prim
                item_attr = prim.GetAttribute(key)
                if not item_attr:
                    item_attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.String, True)
                # Print the key-value pair being set
                print(f"Setting {key} to {value}")
                # Set the attribute value
                item_attr.Set(str(value))

        # Process live updates with the omni client
        omni.client.live_process()


def connect_mqtt(stage, iot_topic, live_layer):
    opc_delta_usd_writer = OpcDeltaUsdWriter(live_layer)
    opc_delta_usd_writer.open_stage()
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

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
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

        if MQTT_CERT and MQTT_KEY:
            # Ensure the paths use forward slashes
            cert = MQTT_CERT
            key = MQTT_KEY

            cert = cert.replace('\\', '/')
            key = key.replace('\\', '/')

            if not os.path.isfile(cert):
                print(f"Certificate file not found: {cert}")
            if not os.path.isfile(key):
                print(f"Key file not found: {key}")

            print(f"Using TLS certificate {cert} and key {key}")
            client.tls_set(certfile=cert, keyfile=key)
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
        print("Reconnecting...")
        # wait a little bit before trying to reconnect
        asyncio.sleep(30)
        return connect_mqtt(stage, iot_topic, live_layer)

    return client


def run(stage, live_layer, iot_topic):
    print(f"Connecting to {MQTT_HOST}....")
    connect_mqtt(stage, iot_topic, live_layer)


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
        print('---- LOG MESSAGES ----')
        print(*messages, sep='\n')
        print('----')
    finally:
        omni.client.shutdown()
