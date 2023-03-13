#!/usr/bin/python3
import datetime
import json
import threading
import time
import sys
import traceback
import socket

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

TOKEN = "zkLYpm0binNquuWIBH_ndVpG0q3DkWk1Ak1I9gejhrxhJD4kDYsvPfRGaJOsiFNqd61iUg0HREqHyrcUcxwu3w=="


class SocketClient(threading.Thread):
    def __init__(self, address, port, callbackFn = None):
        super().__init__()
        self.address = address
        self.port = port
        self.callbackFn = callbackFn
        self.daemon = True
        self.connected = False
        self.connect()

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while not self.connected:
            print("connecting")
            try:
                self.s.connect((self.address, self.port))
                self.connected = True
                print("connected")
            except:
                time.sleep(1)

    def run(self) -> None:
        while True:
            while self.connected:
                try:
                    data = self.s.recv(1024).decode("utf-8")
                    if not data:
                        break
                    print("%s\n" % data)
                    if "\n" in data:
                        dataToProcess, data = data.split("\n")
                        if self.callbackFn:
                            self.callbackFn(dataToProcess)
                except KeyboardInterrupt:
                    sys.exit(-1)
                except Exception as exc:
                    print("%s" % exc)
                    break
            print("lost connection")
            self.connected = False
            self.connect()


class DataProcessor(object):
    def __init__(self, influxLink):
        self.influxLink = influxLink

    def processData(self, dataStr):
        data = json.JSONDecoder().decode(dataStr)
        self.influxLink.sendDataPoint(data, data["room"])


class InfluxConfig(object):
    def __init__(self, configFilename):
        self.configFilename = configFilename
        self.bucket = None
        self.url = None
        self.toket = None
        self.org = None
        self.measurement = None
        self.tagname = None
        self.sources = None

    def read(self):
        f = open(self.configFilename, "r")
        lines = f.readlines()
        f.close()
        for line in lines:
            k, v = line.split(":", 1)
            k = k.strip()
            v = v.strip()
            if k == "bucket":
                self.bucket = v
            elif k == "url":
                self.url = v
            elif k == "token":
                self.token = v
            elif k == "org":
                self.org = v
            elif k == "measurement":
                self.measurement = v
            elif k == "tagname":
                self.tagname = v
            elif k == "sources":
                self.sources = v

    def validate(self):
        if not self.bucket:
            sys.stderr.write("config file must contain a 'bucket'\n")
        if not self.url:
            sys.stderr.write("config file must contain a 'url'\n")
        if not self.token:
            sys.stderr.write("config file must contain a 'token'\n")
        if not self.org:
            sys.stderr.write("config file must contain a 'org'\n")
        if not self.measurement:
            sys.stderr.write("config file must contain a 'measurement'\n")
        if not self.tagname:
            sys.stderr.write("config file must contain a 'tagname'\n")
        if not self.sources:
            sys.stderr.write("config file must contain 'sources'\n")

    def __str__(self):
        return "bucket: %s\nurl: %s\ntoken: %s\norg: %s\nmeasurement: %s\ntagname: %s\nsources: %s\n" % (self.bucket, self.url, self.token, self.org, self.measurement, self.tagname, self.sources)

class InfluxLink(object):
    def __init__(self, configObj):
        self.config = configObj
        self.client = InfluxDBClient(url=self.config.url, token=self.config.token, org=self.config.org)

    def sendDataPoint(self, dataPoint, tag):
        # dataPoint is a dict containing a timestamp and other values
        if not self.client:
            self.client = InfluxDBClient(url=self.config.url, token=self.config.token, org=self.config.org)
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        p = Point(self.config.measurement)
        t = datetime.datetime.fromisoformat(dataPoint["time"]).astimezone()
        p.time(t, WritePrecision.S)
        p.tag(self.config.tagname, tag)
        for k, v in dataPoint.items():
            if k == "time":
                pass
            elif k == "room":
                p.field(k.strip(), v)
            else:
                p.field(k.strip(), float(v))
        try:
            print(p)
            write_api.write(self.config.bucket, self.config.org, p)
            print("ok")
            return True
        except (Exception, KeyboardInterrupt) as exc:
            self.client = None
            sys.stderr.write("Exception in sendDataPoint: %s\n" % exc)
            traceback.print_stack()
            return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: %s <configFile>" % sys.argv[0])
        print("   ex: %s fs20-config.influxdb" % sys.argv[0])
        print("")
        print("<configFile> must contain: bucket, url, token, org, measurement, tagname, sources")
        print("sources is a JSON encoded dict of <tagname>:<ipaddress:port>")
        sys.exit(-1)
    configObj = InfluxConfig(sys.argv[1])
    configObj.read()
    configObj.validate()
    influxLink = InfluxLink(configObj)
    sources = json.JSONDecoder().decode(configObj.sources)
    clientThreads = []
    for tagName, addr in sources.items():
        dataProcessor = DataProcessor(influxLink)
        ipAddress = addr.split(":")[0]
        port = int(addr.split(":")[1])
        client = SocketClient(ipAddress, port, dataProcessor.processData)
        client.start()
        clientThreads.append(client)
    for thread in clientThreads:
        thread.join()
