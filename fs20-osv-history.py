#!/usr/bin/python3
# -*- coding: iso-8859-1 -*-

import socket
import threading
import time
import os
import sys


class Logger(object):
    def __init__(self, logPrefix):
        self.logPrefix = logPrefix
        self.date = time.strftime("%Y-%m-%d")
        filename = "%s.%s" % (self.logPrefix, self.date)
        if os.path.exists(filename):
            self.logFile = open(filename, "a")
        else:
            self.logFile = open(filename, "w")

    def log(self, str):
        date = time.strftime("%Y-%m-%d")
        if date != self.date:
            self.logFile.write("%s starting new logfile\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
            self.logFile.close()
            self.date = date
            self.logFile = open("%s.%s" % (self.logPrefix, self.date), "w")
            self.logFile.write("%s starting new logfile\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
            oldLogFileName = self.logPrefix + "." + time.strftime("%Y-%m-%d", time.localtime(time.time() - 2 * 86400))
            try:
                os.remove(oldLogFileName)
            except Exception as exc:
                self.logFile.write("%s cannot delete old log file %s: %s\n" % (
                    time.strftime('%Y-%m-%d %H:%M:%S'), oldLogFileName, exc))
        self.logFile.write("%s %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S'), str))
        self.logFile.flush()

    def stop(self):
        self.logFile.close()


class Fs20Receiver(threading.Thread):
    def __init__(self, serverName, logger):
        super(Fs20Receiver, self).__init__()
        self.daemon = True
        self.logger = logger
        self.serverIp = serverName.split(':')[0]
        self.serverPort = int(serverName.split(':')[1])
        self.connected = False
        self.fs20handler = {}

    def connect(self):
        while not self.connected:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.serverIp, self.serverPort))
                self.connected = True
            except Exception as exc:
                print(exc)
                self.log("failed to connect to FS20 server: %s" % exc)
                time.sleep(5)

    def send(self, telegram):
        if self.connected:
            self.log("sending %s" % telegram)
            self.sock.send(telegram + '\n')
        else:
            self.log("not sending %s, no connection (yet)" % telegram)

    def sendList(self, telegramList):
        for telegram in telegramList:
            self.send(telegram)

    def run(self):
        self.log("started")
        while True:
            self.connect()
            buffer = ''
            telegram = ''
            while self.connected:
                try:
                    buffer += self.sock.recv(16).decode()
                    if len(buffer) == 0:
                        self.connected = False
                        break
                    if '\n' in buffer:
                        telegram += buffer.split('\n')[0]
                        telegram = telegram.replace('\r', '')
                        self.notifyHandlers(telegram)
                        telegram = ''
                    if len(buffer.split('\n')) > 1:
                        buffer = buffer.split('\n')[1]
                    else:
                        buffer = ''
                except KeyboardInterrupt:
                    self.log("caught KeyboardInterrupt, exiting")
                    self.closeHandlers()
                    sys.exit(0)

    #				except Exception as exc:
    #					self.log("caught exception " + exc.message)
    #					self.connected = False
    #					break

    def notifyHandlers(self, telegram):
        for t in self.fs20handler.keys():
            if telegram.startswith(t):
                self.fs20handler[t].handle(telegram)
                return True
        self.log("no handler for telegram '%s'" % telegram)
        return False

    def closeHandlers(self):
        for handler in self.fs20handler.values():
            handler.stop()

    def register(self, telegrams, handler):
        if type(telegrams) == tuple:
            for telegram in telegrams:
                self.fs20handler[telegram] = handler
        else:
            self.fs20handler[telegrams] = handler

    def log(self, logStr):
        print(logStr)
        self.logger.log("Fs20Receiver: " + logStr)


class FileWriter(object):
    def __init__(self, targetDir, prefix):
        self.targetDir = targetDir
        self.prefix = prefix

    def write(self, data):
        filename = os.path.join(self.targetDir, "%s-%s.csv" % (self.prefix, time.strftime("%Y%m%d")))
        dataKeys = list(data.keys())
        dataKeys.sort()
        if os.path.exists(filename):
            f = open(filename, "a")
        else:
            f = open(filename, "w")
            f.write("time;%s\n" % ";".join(k for k in dataKeys))
        f.write("%s;" % time.strftime("%Y-%m-%d %H:%M:%S"))
        f.write("%s\n" % ";".join("%0.2f" % data[k] for k in dataKeys))
        f.close()


class DataObject(object):
    def __init__(self, name, logger):
        self.name = name
        self.logger = logger
        self.fileWriter = FileWriter("/tmp/fs20", name)

    def log(self, logStr):
        print(logStr)
        self.logger.log("%s: " % self.name + logStr)

    def stop(self):
        pass


class S300TH(DataObject):
    def __init__(self, name, logger):
        super(S300TH, self).__init__(name, logger)

    def handle(self, telegram):
        if (len(telegram) != 9):
            self.log("wrong length: '%s'" % telegram)
            return
        try:
            temp, hum = self.convert(telegram)
            self.fileWriter.write({"temperature": temp, "humidity": hum})
            self.log("%4.1f°C, %4.1f%%" % (temp, hum))
        except ConvertException as exc:
            self.log("cannot convert K-telegram '%s': %s" % (telegram, exc))

    def convert(self, telegram):
        if len(telegram) == 9:
            sign = 1 if (int(telegram[1], 16) & 8 == 0) else -1
            temperature = sign * float("%c%c.%c" % (telegram[6], telegram[3], telegram[4]))  # temp in Â°C
            humidity = float("%c%c.%c" % (telegram[7], telegram[8], telegram[5]))  # hum  in %
            return temperature, humidity
        else:
            self.log("cannot convert, len=%d, telegram='%s'" % (len(telegram), telegram))
            raise ConvertException("cannot convert, len=%d, telegram='%s'" % (len(telegram), telegram))


class HMS100T(DataObject):
    def __init__(self, name, logger):
        super(HMS100T, self).__init__(name, logger)

    def handle(self, telegram):
        try:
            temp, hum, status = self.convert(telegram)
            if (hum == 0):
                self.log("%4.1f°C, 0x%02X" % (temp, status))
                self.fileWriter.write({"temperature": temp})
            else:
                self.log("%4.1f°C, %4.1f%%, 0x%02X" % (temp, hum, status))
                self.fileWriter.write({"temperature": temp, "humidity": hum})
        except ConvertException as exc:
            self.log("cannot convert H-telegram '%s': %s" % (telegram, exc))

    def convert(self, telegram):
        # "H155601150100"
        if len(telegram) == 13:
            sign = 1 if (int(telegram[5], 16) & 8 == 0) else -1
            temperature = sign * float("%c%c.%c" % (telegram[10], telegram[7], telegram[8]))  # temp in Â°C
            humidity = float("%c%c.%c" % (telegram[11], telegram[12], telegram[9]))  # hum in %
            status = int("%c%c" % (telegram[5], telegram[6]))  # status
            return temperature, humidity, status
        else:
            self.log("cannot convert, len=%d, telegram='%s'" % (len(telegram), telegram))
            raise ConvertException("cannot convert, len=%d, telegram='%s'" % (len(telegram), telegram))


class ConvertException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: %s <fs20-server>" % sys.argv[0])
        print("ex:    %s localhost:7890" % sys.argv[0])
        sys.exit(-1)

for arg in sys.argv:
    print(arg)

startTime = time.ctime()

logger = Logger("/var/log/fs20/fs20-osv-history")
if not os.path.exists("/tmp/fs20"):
    os.makedirs("/tmp/fs20")

fs20Receiver = Fs20Receiver(sys.argv[1], logger)

fs20Receiver.register(("K31", "KB1"), S300TH("Aussen", logger))  # telegram = "K31" -> "K31" and "KB1" is ok
fs20Receiver.register("H9E34", HMS100T("Wohnzimmer", logger))
fs20Receiver.register("H39BA", HMS100T("Badezimmer", logger))
fs20Receiver.register("H1556", HMS100T("Schlafzimmer", logger))
fs20Receiver.register("HF3BF", HMS100T("Florian", logger))
fs20Receiver.register("H312A", HMS100T("Benjamin", logger))
fs20Receiver.register("H70A2", HMS100T("Stiegenhaus", logger))
fs20Receiver.register("HBE63", HMS100T("Dusche", logger))

fs20Receiver.run()
