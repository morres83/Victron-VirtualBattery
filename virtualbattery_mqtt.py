from gi.repository import GLib
import logging
import sys
import os
import dbus
# from settings import *
from datetime import datetime as dt         # for UTC time stamps for logging
import time as tt                           # for charge measurement
import paho.mqtt.client as mqtt
import json
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

sys.path.append('/opt/victronenergy/dbus-systemcalc-py/ext/velib_python')
from vedbus import VeDbusService #, VeDbusItemImport

path_UpdateIndex = '/UpdateIndex' #not sure if needed

# MQTT Setup
broker_address = "localhost"
MQTTNAME = "venusMQTT"
virtualBatteryMQTTPath = "virtualbattery/data"

# Variblen setzen
mqttConnected = 0
voltage, current, power = 0, 0, 0
soc, maxCellTemperature, minCellTemperature = 0, 0, 0
maxCellVoltage, minCellVoltage = 0, 0
modulesBlockingCharge = 0
maxChargeCurrent, maxDischargeCurrent, maxChargeVoltage = 0, 0, 0
internalFailure = 0

def setFailsafeSettings():
    global maxChargeCurrent, maxDischargeCurrent, maxChargeVoltage, internalFailure
    maxChargeCurrent = 0
    maxDischargeCurrent = 0
    maxChargeVoltage = 53.6
    internalFailure = 2

def on_disconnect(client, userdata, rc):
    global mqttConnected
    print("Client Got Disconnected")
    if rc != 0:
        print('Unexpected MQTT disconnection. Will auto-reconnect')

    else:
        print('rc value:' + str(rc))

    try:
        print("Trying to Reconnect")
        client.connect(broker_address)
        mqttConnected = 1
    except Exception as e:
        logging.exception("Error in Retrying to Connect with Broker")
        print("Error in Retrying to Connect with Broker")
        mqttConnected = 0
        print(e)

def on_connect(client, userdata, flags, rc):
        global mqttConnected
        if rc == 0:
            print("Connected to MQTT Broker!")
            mqttConnected = 1
            client.subscribe(virtualBatteryMQTTPath)
        else:
            print("Failed to connect, return code %d\n", rc)


def on_message(client, userdata, msg):

    try:

        global voltage, current, power, soc #mandatory
        global maxCellTemperature, minCellTemperature, internalFailure
        global maxCellVoltage, minCellVoltage, modulesBlockingCharge
        global maxChargeCurrent, maxDischargeCurrent, maxChargeVoltage #mandatory
        if msg.topic == virtualBatteryMQTTPath:   # JSON String vom Broker
            if msg.payload != '{"value": null}' and msg.payload != b'{"value": null}':
                jsonpayload = json.loads(msg.payload)
                voltage = float(jsonpayload["Voltage"])
                current = float(jsonpayload["Current"])
                power = float(jsonpayload["Power"])
                soc = float(jsonpayload["Soc"])
                
                maxCellTemperature = float(jsonpayload.get("MaxCellTemperature") or 0)
                minCellTemperature = float(jsonpayload.get("MinCellTemperature") or 0)
                maxCellVoltage = float(jsonpayload.get("MaxCellVoltage") or 0)
                minCellVoltage = float(jsonpayload.get("MinCellVoltage") or 0)
                modulesBlockingCharge = int(jsonpayload.get("ModulesBlockingCharge") or 0)

                maxChargeCurrent = float(jsonpayload["MaxChargeCurrent"])
                maxDischargeCurrent = float(jsonpayload["MaxDischargeCurrent"])
                maxChargeVoltage = float(jsonpayload["MaxChargeVoltage"])
                internalFailure = 0

            else:
                print("Answer from MQTT was null and was ignored")
    except KeyError:
        logging.exception("Not all mandatory variables in JSON string")
        setFailsafeSettings()
    except Exception as e:
        logging.exception("Exception in onmessage function")
        print(e)
        print("Exception in onmessage function")
        setFailsafeSettings()


class DbusVirtualBatService(object):
    
    def __init__(self, servicename='com.victronenergy.battery.virtual'):
        self._dbusservice = VeDbusService(servicename)
        self._dbusConn = dbus.SessionBus()  if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else dbus.SystemBus()
        
        # Create the mandatory objects
        self._dbusservice.add_mandatory_paths(processname = __file__, processversion = '0.0', connection = 'Virtual',
			deviceinstance = 14, productid = 0, productname = 'VirtualBattery', firmwareversion = 0.2, 
            hardwareversion = '0.0', connected = 1)

        # Create DC paths        
        self._dbusservice.add_path('/Dc/0/Voltage', None, writeable=True, gettextcallback=lambda a, x: "{:.2f}V".format(x))
        self._dbusservice.add_path('/Dc/0/Current', None, writeable=True, gettextcallback=lambda a, x: "{:.1f}A".format(x))
        self._dbusservice.add_path('/Dc/0/Power', None, writeable=True, gettextcallback=lambda a, x: "{:.0f}W".format(x))
        
        # Create capacity paths
        self._dbusservice.add_path('/Soc', None, writeable=True)
        # self._dbusservice.add_path('/Capacity', None, writeable=True, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        # self._dbusservice.add_path('/InstalledCapacity', None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        # self._dbusservice.add_path('/ConsumedAmphours', None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        
        # Create temperature paths
        # self._dbusservice.add_path('/Dc/0/Temperature', None, writeable=True)       
        self._dbusservice.add_path('/System/MinCellTemperature', None, writeable=True)
        self._dbusservice.add_path('/System/MaxCellTemperature', None, writeable=True)
        # self._dbusservice.add_path('/System/MaxTemperatureCellId', None, writeable=True)       
        # self._dbusservice.add_path('/System/MinTemperatureCellId', None, writeable=True)
        
        # Create extras paths
        self._dbusservice.add_path('/System/MinCellVoltage', None, writeable=True)
        # self._dbusservice.add_path('/System/MinVoltageCellId', None, writeable=True)
        self._dbusservice.add_path('/System/MaxCellVoltage', None, writeable=True)
        # self._dbusservice.add_path('/System/MaxVoltageCellId', None, writeable=True)
        # self._dbusservice.add_path('/System/NrOfCellsPerBattery', None, writeable=True)
        # self._dbusservice.add_path('/System/NrOfModulesOnline', None, writeable=True)
        # self._dbusservice.add_path('/System/NrOfModulesOffline', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfModulesBlockingCharge', None, writeable=True)
        # self._dbusservice.add_path('/System/NrOfModulesBlockingDischarge', None, writeable=True)         
        
        # Create alarm paths
        # self._dbusservice.add_path('/Alarms/LowVoltage', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighVoltage', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/LowCellVoltage', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/LowSoc', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighChargeCurrent', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighDischargeCurrent', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/CellImbalance', None, writeable=True)
        self._dbusservice.add_path('/Alarms/InternalFailure', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighChargeTemperature', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/LowChargeTemperature', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/HighTemperature', None, writeable=True)
        # self._dbusservice.add_path('/Alarms/LowTemperature', None, writeable=True)
        #self._dbusservice.add_path('/Alarms/HighCellVoltage', None, writeable=True)
        
        # Create control paths
        self._dbusservice.add_path('/Info/MaxChargeCurrent', None, writeable=True, gettextcallback=lambda a, x: "{:.1f}A".format(x))
        self._dbusservice.add_path('/Info/MaxDischargeCurrent', None, writeable=True, gettextcallback=lambda a, x: "{:.0f}A".format(x))
        self._dbusservice.add_path('/Info/MaxChargeVoltage', None, writeable=True, gettextcallback=lambda a, x: "{:.1f}V".format(x))

        GLib.timeout_add(1000, self._update)    
    
    
    def _update(self):  

        with self._dbusservice as bus:
        
            bus['/Dc/0/Voltage'] = voltage
            bus['/Dc/0/Current'] = current
            bus['/Dc/0/Power'] = power
        
            bus['/Soc'] = round(soc, 0)
            # bus['/Capacity'] = Capacity
            # bus['/InstalledCapacity'] = InstalledCapacity
            # bus['/ConsumedAmphours'] = ConsumedAmphours
        
            # bus['/Dc/0/Temperature'] = Temperature
            bus['/System/MaxCellTemperature'] = round(maxCellTemperature, 1)
            bus['/System/MinCellTemperature'] = round(minCellTemperature, 1)
        
            bus['/System/MaxCellVoltage'] = round(maxCellVoltage, 3)
            # bus['/System/MaxVoltageCellId'] = MaxVoltageCellId
            bus['/System/MinCellVoltage'] = round(minCellVoltage, 3)
            # bus['/System/MinVoltageCellId'] = MinVoltageCellId
        
            # bus['/System/NrOfCellsPerBattery'] = NrOfCellsPerBattery
            # bus['/System/NrOfModulesOnline'] = NrOfModulesOnline
            # bus['/System/NrOfModulesOffline'] = NrOfModulesOffline
            bus['/System/NrOfModulesBlockingCharge'] = modulesBlockingCharge
            # bus['/System/NrOfModulesBlockingDischarge'] = NrOfModulesBlockingDischarge
        
            # bus['/Alarms/LowVoltage'] = LowVoltage_alarm
            # bus['/Alarms/HighVoltage'] = HighVoltage_alarm
            # bus['/Alarms/LowCellVoltage'] = LowCellVoltage_alarm
            # bus['/Alarms/LowSoc'] = LowSoc_alarm
            # bus['/Alarms/HighChargeCurrent'] = HighChargeCurrent_alarm
            # bus['/Alarms/HighDischargeCurrent'] = HighDischargeCurrent_alarm
            # bus['/Alarms/CellImbalance'] = CellImbalance_alarm
            bus['/Alarms/InternalFailure'] = internalFailure
            
            # bus['/Alarms/HighChargeTemperature'] = HighChargeTemperature_alarm
            # bus['/Alarms/LowChargeTemperature'] = LowChargeTemperature_alarm
            # bus['/Alarms/HighTemperature'] = HighChargeTemperature_alarm
            # bus['/Alarms/LowTemperature'] = LowChargeTemperature_alarm
        
            bus['/Info/MaxChargeCurrent'] = maxChargeCurrent
            bus['/Info/MaxDischargeCurrent'] = maxDischargeCurrent
            bus['/Info/MaxChargeVoltage'] = maxChargeVoltage
            index = bus[path_UpdateIndex] + 1  # increment index
            if index > 255:   # maximum value of the index
                index = 0       # overflow from 255 to 0
            bus[path_UpdateIndex] = index

        return True
    
def main():
    logging.basicConfig(filename = 'virtualbattery.log', level=logging.INFO)

    from dbus.mainloop.glib import DBusGMainLoop
    # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
    DBusGMainLoop(set_as_default=True)
    DbusVirtualBatService()

    logging.info(f'{dt.now()} Connected to dbus')

    # Configuration MQTT
    client = mqtt.Client(MQTTNAME) # create new instance
    client.on_disconnect = on_disconnect
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_address)  # connect to broker

    client.loop_start()

    mainloop = GLib.MainLoop()
    mainloop.run()

if __name__ == "__main__":
    main()

