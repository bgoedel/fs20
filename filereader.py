#!/usr/bin/python3
import datetime
import time
import os
import sys
import traceback

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

TOKEN = "zkLYpm0binNquuWIBH_ndVpG0q3DkWk1Ak1I9gejhrxhJD4kDYsvPfRGaJOsiFNqd61iUg0HREqHyrcUcxwu3w=="


class FileReader(object):
    def __init__(self, filenamePrefix, influxLink):
        self.influxLink = influxLink
        self.fileNames = [fileName for fileName in os.listdir(".") if ("%s-" % filenamePrefix in fileName) and (".csv" in fileName)]
        self.fileNames.sort()
        self.posFilename = "%s.pos" % filenamePrefix
        if os.path.exists(self.posFilename):
            #print("continue")
            f = open(self.posFilename, "r")
            self.lastFile, self.lastPos = f.readline().split(";")
            self.lastPos = int(self.lastPos)
            f.close()
            #print("starting to read from %s;%d" % (self.lastFile, self.lastPos))
        else:
            self.lastFile = self.fileNames[0]
            self.lastPos = 0
        file = open(self.lastFile)
        self.headers = file.readline().split(";")
        file.close()
        self.filenamePrefix = filenamePrefix

    def storeLastPos(self):
        f = open(self.posFilename, "w")
        #print("lastpos: %s;%d" % (self.lastFile, self.lastPos))
        f.write("%s;%d" % (self.lastFile, self.lastPos))
        f.close()

    def read(self):
        lastPos = self.lastPos
        for filename in self.fileNames[self.fileNames.index(self.lastFile):]:
            #print("working on file %s" % filename)
            file = open(filename)
            self.lastFile = filename
            self.lastPos = file.tell()
            file.seek(lastPos)
            line = file.readline()
            if "time;" in line:
                line = file.readline()
            while line:
                #print("line: %s" % line)
                entries = line.split(";")
                dataPoint = {}
                for index in range(0, len(self.headers)):
                    dataPoint[self.headers[index]] = entries[index]
                if self.influxLink.sendDataPoint(dataPoint, self.filenamePrefix):
                    self.lastPos = file.tell()
                    self.lastFile = filename
                    line = file.readline()
                else:
                    #self.lastPos = file.tell()
                    #self.lastFile = filename
                    self.storeLastPos()
                    return
            file.close()
            self.storeLastPos()
            lastPos = 0


class InfluxConfig(object):
    def __init__(self, configFilename):
        self.configFilename = configFilename
        self.bucket = None
        self.url = None
        self.toket = None
        self.org = None
        self.measurement = None
        self.tagname = None

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
            if k != "time":
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
    if len(sys.argv) < 4:
        print("usage: %s <configFile> <filenamePrefix> <interval>" % sys.argv[0])
        print("   ex: %s config.influxdb charger 60" % sys.argv[0])
        print("   ex: %s config.influxdb inverter 60" % sys.argv[0])
        sys.exit(-1)
    configObj = InfluxConfig(sys.argv[1])
    configObj.read()
    configObj.validate()
    influxLink = InfluxLink(configObj)
    while True:
        #print("reading new values")
        FileReader(sys.argv[2], influxLink).read()
        time.sleep(int(sys.argv[3]))
