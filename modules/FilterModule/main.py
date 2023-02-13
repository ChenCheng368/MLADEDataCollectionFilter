# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.
# Migrated with IoTHub Python SDK v2

import asyncio
import random
import time
import sys
import json
import requests
import os
from threading import Thread
import logging
from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import DetectRequest, TimeSeriesPoint, TimeGranularity, \
    AnomalyDetectorError
from azure.core.credentials import AzureKeyCredential
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message, MethodResponse
from datetime import datetime

SUBSCRIPTION_KEY = os.environ["ANOMALY_DETECTOR_KEY"]
ANOMALY_DETECTOR_ENDPOINT = os.environ["ANOMALY_DETECTOR_ENDPOINT"]
#TIME_SERIES_DATA_PATH = os.path.join("./sample_data", "request-data.csv")
AZURE_LOCATION = "southeastasia"

AD_client = AnomalyDetectorClient(AzureKeyCredential(SUBSCRIPTION_KEY), ANOMALY_DETECTOR_ENDPOINT)
logging.basicConfig(level=logging.DEBUG)

# global counters
TEMP_THRESHOLD_LOW_PROPERTY_NAME = "TemperatureThresholdLow"
TEMP_THRESHOLD_HIGH_PROPERTY_NAME = "TemperatureThresholdHigh"
TIME_PERIOD_PROPERTY_NAME = "TimePeriod"
INPUT_SET_LEN_PROPERTY_NAME = "ADInputLength"
DUTY_CYCLE_PROPERTY_NAME = "DutyCycle"
HEART_BEAT = "heartbeat"
DESIRED_PROPERTY_KEY = "desired"

TEMPERATURE_THRESHOLD_LOW = 25
TEMPERATURE_THRESHOLD_HIGH = 100
INPUT_SET_LEN=13 #range = [12,8640]
TIME_PERIOD = [] #["2021-11-01T14:15:00Z", "2021-11-01T15:46:00Z"]
DUTY_CYCLE = 0.2 #0.2 = 20% = range(1,60,5)

async def filter_infer_results(message):
    message_str = json.dumps(message)
    print("filtered inference results to be send to iothub: ", message_str)
    filtered_message = Message(message_str)
    return filtered_message

async def filter_telemetry(message, **kwargs):
    '''
    :param message:
    :param kwargs: inference_output_constraint, time_period, telemetry_threshold_low, telemetry_threshold_high
    :return:
    '''
    filtered_flag_cnt = 0
    arg_len = len(kwargs)
    print("arg_len", arg_len)
    # call telemetry filter module container, to get the filtered telemetry.
    # sample to filter telemetry based on response.is_negative_anomaly. select all telemetries with response.is_negative_anomaly==true
    # convert filtered msg data type for sending to cloud
    for key, value in kwargs.items():
        # filter criteria == inference output
        if key =="inference_output_constraint":
            if value == False:                     # condition editable for each ML scenario
                filtered_flag_cnt +=1
        # filter criteria == time period
        if key == "time_period":
            print("time_period constraint ", value)
            print("message ts ", message["ts"], type(message["ts"]))
            if len(value) != 0:
                if datetime.strptime(message["ts"][:26].strip(), "%Y-%m-%dT%H:%M:%S.%f") > datetime.strptime(value[0], "%Y-%m-%dT%H:%M:%SZ")  and datetime.strptime(message["ts"][:26].strip(), "%Y-%m-%dT%H:%M:%S.%f") < datetime.strptime(value[1], "%Y-%m-%dT%H:%M:%SZ"):#message["ts"]=str "2021-11-01T14:20:50.030756Z"
                    filtered_flag_cnt += 1
            else:
                filtered_flag_cnt += 1
        # filter criteria == time duty cycle
        if key == "duty_cycle":
            print("duty_cycle ", value)
            print("message ts ", message["ts"], type(message["ts"]))
            for iter_min in range(1, 60, int(1/value)):
                print("iter_min", iter_min)
                print('message["ts"][14:16].strip()', message["ts"][14:16].strip(), type(message["ts"][14:16].strip()))
                if int(message["ts"][14:16].strip()) == iter_min:
                    filtered_flag_cnt += 1

        # filter criteria == threshold
        if key == "telemetry_threshold_low":
            print("telemetry_threshold_low", value)
            if message["telemetry"] > value:
                filtered_flag_cnt +=1
        if key == "telemetry_threshold_high":
            print("telemetry_threshold_high", value)
            if message["telemetry"] < value:
                filtered_flag_cnt +=1

    # pre-processing if any filtered data
    print("filtered_flag_cnt", filtered_flag_cnt)

    if filtered_flag_cnt == arg_len:
        # data = {
        #     "timestamp": message["ts"],
        #     "telemetry": message["telemetry"]
        # }
        # convert infer result msg to json for sending out via mqtt
        message_str = json.dumps(message)  
        filtered_message = Message(message_str)
        print("filtered telemetry to be send to iothub: ", message_str)
        return filtered_message
    else:
        print("NOT filtered")
        return None


async def anomaly_detection_SDK_module(det_input):
    series = []
    for i in range(len(det_input)):  
        #print(type(row[0]), type(row[1]))
        series.append(TimeSeriesPoint(timestamp=det_input[i][0], value=det_input[i][1])) 

    #series.append(TimeSeriesPoint(timestamp=det_input[:][0], value=det_input[:][1]))
    last_ts = det_input[-1][0]
    last_data = det_input[-1][1]
    print("last ts is: ", last_ts, " and last data is : ", det_input[-1][1])
    #request = DetectRequest(series=series,  
    #                        granularity=TimeGranularity.daily)  # granularity=TimeGranularity.per_minute
    request = DetectRequest(series=series)

    print('Detecting the anomaly status of the latest data point.')
    try:
        response = AD_client.detect_last_point(request)
        print("finish detection, response is:", response)
        response_str=str(response)
        print("finish detection, response str is:", type(response_str),response_str )
        response_dict = json.loads(response_str.lower().replace("'", "\""))
        print(type(response_dict), response_dict)
        #response format: {'additional_properties': {}, 'period': 7, 'suggested_window': 29, 'expected_value': 35253918.97570676, 'upper_margin': 352539.189757064, 'lower_margin': 352539.189757064, 'is_anomaly': False, 'is_negative_anomaly': False, 'is_positive_anomaly': False}

    except AnomalyDetectorError as e:
        print('Error code: {}'.format(e.error.code), 'Error message: {}'.format(e.error.message))
    except Exception as e:
        print(e)

    results = dict(zip(["ts", "telemetry"], [last_ts, last_data]))
    print("results:",type(results),results)
    for key in response_dict:
        print(key)
        results[key] = response_dict[key]
    return results



class HubManager(object):
    def __init__(self):
        # Inputs/Outputs are only supported in the context of Azure IoT Edge and module client
        # The module client object acts as an Azure IoT Edge module and interacts with an Azure IoT Edge hub
        self.module_client = IoTHubModuleClient.create_from_edge_environment()
        self.input_set = []

    async def start(self):
        await self.module_client.connect()
        self.module_client.on_message_received = self.message_handler
        self.module_client.on_twin_desired_properties_patch_received = self.twin_patch_handler
        self.module_client.on_method_request_received = self.method_handler

    async def prepare_infer_input(self, message):
        print("in prepare_input_set function!")
        message_str = message.data
        if not message_str:
            return None
        message_obj = json.loads(message_str)
        print("module receives a msg with temp: {}".format(message_obj["machine"]["temperature"]))
        input_data = message_obj["machine"]["temperature"]
        time_stamp = message_obj["timeCreated"]  # UTC ISO 8601

        self.input_set.append([time_stamp, input_data])

        if len(self.input_set) > INPUT_SET_LEN:
            while len(self.input_set) > INPUT_SET_LEN:
                self.input_set.pop(0)
            return self.input_set
        else:
            return None

    async def filter_infer(self, infer_result):
        # thread for filtering infer results and sending to cloud
        print("in filter_infer")
        filtered_infer_result = await filter_infer_results(infer_result)
        if filtered_infer_result:
            print("filtered_infer_result! Sending...")
            await self.forward_event_to_output(filtered_infer_result,
                                         "output1")  

    async def filter_tele(self, infer_result):
        # thread for filtering telemetry and send to cloud for training
        # Define the inference output metric as filter constraint. editable for each ML scenario
        print("in filter_tele")
        infer_constraint = infer_result["is_negative_anomaly"]
        print("infer_constraint: ", infer_constraint)
        filtered_telem = await filter_telemetry(infer_result, inference_output_constraint=infer_constraint, telemetry_threshold_low=TEMPERATURE_THRESHOLD_LOW, telemetry_threshold_high=TEMPERATURE_THRESHOLD_HIGH, time_period=TIME_PERIOD, duty_cycle= DUTY_CYCLE)
        if filtered_telem:
            print("filtered_telem! Sending...")
            await self.forward_event_to_output(filtered_telem,
                                         "output2")  

    async def message_handler(self, message):
        if message.input_name == "input1":
            try:
                #send input telemetry to Anomaly Detector by module client call
                det_input = await self.prepare_infer_input(message) # at least 13 data points
                if det_input!= None:
                    print("start inferencing...len of det input: ", len(det_input))
                    #infer_result = anomaly_detection_HTTP(det_input) # test HTTP call
                    infer_result = await anomaly_detection_SDK_module(det_input)
                    print("infer_result is: ", infer_result)

                    #running 2 filter threads concurrently
                    results = await asyncio.gather(
                        self.filter_tele(infer_result),
                        self.filter_infer(infer_result),
                    )
                    print(len(results))

            except Exception as e:
                print("Error when filter message: %s" % e)
        else:
            print("message received on unknown input")

    # Define behavior for receiving a twin desired properties patch
    def twin_patch_handler(self, patch):
        print("twin_patch_handler is triggered!")
        global TEMPERATURE_THRESHOLD_LOW,TEMPERATURE_THRESHOLD_HIGH,TIME_PERIOD, INPUT_SET_LEN
        if DESIRED_PROPERTY_KEY in patch:#DESIRED_PROPERTY_KEY = "desired"
            patch = patch[DESIRED_PROPERTY_KEY]

        if TEMP_THRESHOLD_LOW_PROPERTY_NAME in patch:#TEMP_THRESHOLD_PROPERTY_NAME = "TemperatureThresholdLow"
            TEMPERATURE_THRESHOLD_LOW = patch[TEMP_THRESHOLD_LOW_PROPERTY_NAME]
        if TEMP_THRESHOLD_HIGH_PROPERTY_NAME in patch:#TEMP_THRESHOLD_PROPERTY_NAME = "TemperatureThresholdHigh"
            TEMPERATURE_THRESHOLD_HIGH = patch[TEMP_THRESHOLD_HIGH_PROPERTY_NAME]
        if INPUT_SET_LEN_PROPERTY_NAME in patch: #INPUT_SET_LEN_PROPERTY_NAME = "ADInputLength"
            INPUT_SET_LEN = patch[INPUT_SET_LEN_PROPERTY_NAME]
        if TIME_PERIOD_PROPERTY_NAME in patch:#TIME_PERIOD_PROPERTY_NAME = "TimePeriod"
            TIME_PERIOD =patch[TIME_PERIOD_PROPERTY_NAME]
        if DUTY_CYCLE_PROPERTY_NAME in patch:#DUTY_CYCLE_PROPERTY_NAME = "DutyCycle"
            DUTY_CYCLE =patch[DUTY_CYCLE_PROPERTY_NAME]


    # Define behavior for receiving methods
    async def method_handler(self, method_request):
        print("Received method [%s]" % (method_request.name))
        message_str = "Module [FilterModule] is Running"
        heart_beat_messsage = Message(message_str)
        heart_beat_messsage.custom_properties["MessageType"] = HEART_BEAT
        await self.forward_event_to_output(heart_beat_messsage, HEART_BEAT)
        print("Sent method response to module output via event [%s]" % HEART_BEAT)

        method_response = MethodResponse.create_from_method_request(
            method_request, 200, "{ \"Response\": \"This is the response from the device. Sent method response to module output via event heartbeat. \" }"
        )
        await self.module_client.send_method_response(method_response)

    async def forward_event_to_output(self, event, moduleOutputName):
        await self.module_client.send_message_to_output(event, moduleOutputName)

async def main():
    try:
        print("\nPython %s\n" % sys.version)
        print("IoT Hub Client for MLADE with Temp Sensor")
        print(SUBSCRIPTION_KEY)
        print(ANOMALY_DETECTOR_ENDPOINT)

        hub_manager = HubManager()
        await hub_manager.start()
        print("The sample is now waiting for messages and will indefinitely.  Press Ctrl-C to exit. ")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        await hub_manager.module_client.shutdown()
        print("IoTHubModuleClient sample stopped")


if __name__ == '__main__':
    asyncio.run(main())
