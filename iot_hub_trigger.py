import logging
import azure.functions as func

import os
import json
from azure.identity import DefaultAzureCredential
from azure.digitaltwins.core import DigitalTwinsClient

iot_hub_trigger = func.Blueprint()

@iot_hub_trigger.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="iothub-ns-ccaziothub-22219968-5cc2da6979", connection="event_hub_conn_str", consumer_group="functions") 
def iothubUpdateTwins(azeventhub: func.EventHubEvent):
    adt_service_url = os.getenv("adt_service_url")

    # Get the body of the event hub message and the system properties
    try:
        message = json.loads(azeventhub.get_body().decode('utf-8'))
        message_type = message["type"]
        system_properties = azeventhub.metadata.get("SystemProperties")
        device_id = system_properties["iothub-connection-device-id"]
    except Exception as e:
        logging.error('Failed to decode message. Error: %s', e)
        return 1

    # Create the digital twins client
    try:
        cred = DefaultAzureCredential()
        client = DigitalTwinsClient(credential=cred, endpoint=adt_service_url)
    except Exception as e:
        logging.error('Could not create digital twin client. Error: %s', e)
        return 1
    
    # Update the twin
    try:
        match message_type:
            case "accelerometer":
                update_twin_data = [{"op": "replace", "path": "/accelerometerX", "value": message["accelerometerX"]},
                                    {"op": "replace", "path": "/accelerometerY", "value": message["accelerometerY"]},
                                    {"op": "replace", "path": "/accelerometerZ", "value": message["accelerometerZ"]}]
                client.update_digital_twin("accelerometer-" + device_id, update_twin_data)
                logging.info('Accelerometer twin updated: %s', message)
            case "gyroscope":
                update_twin_data = [{"op": "replace", "path": "/gyroscopeX", "value": message["gyroscopeX"]},
                                    {"op": "replace", "path": "/gyroscopeY", "value": message["gyroscopeY"]},
                                    {"op": "replace", "path": "/gyroscopeZ", "value": message["gyroscopeZ"]}]
                client.update_digital_twin("gyroscope-" + device_id, update_twin_data)
                logging.info('Gyroscope twin updated: %s', message)
            case "magnetometer":
                update_twin_data = [{"op": "replace", "path": "/magnetometerX", "value": message["magnetometerX"]},
                                    {"op": "replace", "path": "/magnetometerY", "value": message["magnetometerY"]},
                                    {"op": "replace", "path": "/magnetometerZ", "value": message["magnetometerZ"]}]
                client.update_digital_twin("magnetometer-" + device_id, update_twin_data)
                logging.info('Magnetometer twin updated: %s', message)
            case "environment":
                update_twin_data = [{"op": "replace", "path": "/humidity", "value": message["humidity"]},
                                    {"op": "replace", "path": "/pressure", "value": message["pressure"]},
                                    {"op": "replace", "path": "/temperature", "value": message["temperature"]}]
                client.update_digital_twin("environment-" + device_id, update_twin_data)
                logging.info('Environment twin updated: %s', message)
    except Exception as e:
        logging.error('Failed to update digital twin. Error: %s', e)
        return 1
