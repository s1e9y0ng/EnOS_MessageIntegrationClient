import paho.mqtt.client as mqtt
import json
import sys
import time
import csv
from datetime import datetime 

# Initializing global variables 
mqtt_endpoint = ""
channelid = ""
clientid = ""
pw = ""
pub_topic = ""
sub_topic = ""
devicekey = ""
filename = ""
assetid = ""
validatemode = False

def read_config_from_json(file_path):
    try:
        with open(file_path, 'r') as config_file:
            config_data = json.load(config_file)
        return config_data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Unable to parse JSON in '{file_path}': {e}")
        return None

def timestamp_to_datetime(timestamp_str):
    try:
        # Assuming the timestamp format is "YYYY-MM-DD HH:MM:SS" (you can adjust the format as needed)
        datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        #time_obj = datetime_obj.time()
        return datetime_obj
    except ValueError:
        print("Error: Invalid timestamp format")
        return None
    
def time_to_unix_milliseconds(time_obj):
    try:
        # Assuming the time object is in the same day (you can adjust as needed)
        unix_time_milliseconds = int(time_obj.timestamp() * 1000)
        return unix_time_milliseconds
    except AttributeError:
        print("Error: Invalid time object")
        return None
    
def read_csv_file(file_path):
    with open(file_path, 'r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        headers = next(csv_reader)  # Extract the headers as the first row

        data = []
        for row in csv_reader:
            data.append(row)

    return headers, data

def form_mqtt_message(datarow, headers):
    # form data message in the form of
    # {
    #    "id":"xxxxxxxxxx",
    #    "version":"1.0",
    #    "method":"integration.method.post",
    #    "params": {
    #        "deviceKey":"zF5eQpUgmT",
    #        "time": xxxxxxxxxxxxx,
    #        "measurepoints":{
    #            "AHU_raw_status":0,
    #            "AHU_raw_sp_supply_air_temp":21,
    #            "AHU_low_sat_sp_energy_waste_5min":0.09,
    #            "AHU_non_oper_hours_energy_waste_5min":0,
    #            "AHU_raw_supply_air_temp":25.3,
    #            "AHU_schedule_output":0, 
    #        },
    #        "":"",
    #    },
    # }
    
    # convert timestamp to unixtimestamp in millisec
    timeobj = timestamp_to_datetime(datarow[0])
    # if (timeobj.hour >= 10):
    #   return None
    unixts = time_to_unix_milliseconds(timeobj)

    message = {
        "deviceKey": devicekey,
        "time": unixts,
        "measurepoints":{
        }
    }

    for index, header in enumerate(headers):
        # skip the first column which is the timestamp field processed above
        if index == 0:
            continue
        # update the dictionary of measurepoints based on header and datarow
        message["measurepoints"].update({header: datarow[index]})

    return message
   

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    print("Subscribing to " + sub_topic)
    client.subscribe(sub_topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    
if __name__ == '__main__':
    config_file_path = 'config.json'
    config = read_config_from_json(config_file_path)

    # if no config.json file found then exit program
    if config is None:
        print("Config.json not found, exiting program")
        quit()

    mqtt_endpoint = config["mqtt_endpoint"]
    channelid = config["channelid"]
    clientid = config["clientid"]
    pw = config["pw"]
    pub_topic = config["pub_topic"]
    sub_topic = config["sub_topic"]
    devicekey = config["devicekey"]
    filename = config["filename"]
    validatemode = config["validatemode"]

    headers, data = read_csv_file(filename)
    unixtime = int(time.time()*1000)
    
    dictionary = { 
        "id": unixtime,
        "version":"1.0",
        "method":"integration.measurepoint.post",
        "params":[]
    }
    for row in data:
        row_message = form_mqtt_message(row, headers)
        if row_message:
            dictionary["params"].append(row_message)

    print (str(dictionary))
    print (str(len(dictionary["params"])))
    
    # configure client to connect to MQTT Broker
    # MQTT Broker is IoT Hub Message Integration Channel
    if validatemode:
        # in validate mode, stop when messages are processed.
        quit()

    print("Initializing - Connect to Message Integration MQTT Broker")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client._client_id = clientid
    client.username_pw_set(channelid, password=pw)
    client.connect(mqtt_endpoint, 11883, 60)
    client.loop_start()
    
    # Send data to IoT Hub
    if dictionary is not None:
        data_message = json.dumps(dictionary, indent = 4)
        client.publish(pub_topic, payload=data_message)

    # Termination
    time.sleep(30)
    client.loop_stop()
    client.disconnect()