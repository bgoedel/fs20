#!/usr/bin/python3

from threading import Thread
from threading import Event
from threading import Lock
import queue
import socket
import sys
import time
import serial
import os


class Logger(object):
    def __init__(self, logPrefix):
        self.logPrefix = logPrefix
        self.date = time.strftime("%Y-%m-%d")
        self.logFile = open(self.logPrefix + "." + self.date, "a")

    def log(self, str):
        date = time.strftime("%Y-%m-%d")
        if date != self.date:
            self.logFile.write("%s starting new logfile" % time.strftime('%Y-%m-%d %H:%M:%S'))
            self.logFile.close()
            self.date = date
            self.logFile = open(self.logPrefix + "." + self.date, "w")
            self.logFile.write("%s starting new logfile" % time.strftime('%Y-%m-%d %H:%M:%S'))
            oldLogFileName = self.logPrefix + "." + time.strftime("%Y-%m-%d", time.localtime(time.time() - 2 * 86400))
            try:
                os.remove(oldLogFileName)
            except Exception as exc:
                self.logFile.write("%s cannot delete old log file %s: %s" % (
                    time.strftime('%Y-%m-%d %H:%M:%S'), oldLogFileName, exc))
        self.logFile.write("%s %s\n" % (time.strftime('%Y-%m-%d %H:%M:%S'), str))
        self.logFile.flush()

    def stop(self):
        self.logFile.close()


class Fs20(Thread):
    def __init__(self, devices, logger):
        Thread.__init__(self)
        self.cul = None
        self.clients = []
        self.logger = logger
        self.devices = devices
        self.connectedDevice = ""
        self.lock = Lock()
        self.log("starting Sender Thread")
        self.fs20Sender = Fs20Sender(self, logger)
        self.fs20Sender.daemon = True
        self.fs20Sender.start()
        self.log("Sender Thread started")

    def connectDevice(self):
        while self.connectedDevice == "":
            for device in self.devices:
                try:
                    self.cul = serial.Serial(device, 9600)
                    self.connectedDevice = device
                    self.writeToCul("X01")
                    self.log("connected to device " + device)
                    return
                except serial.SerialException as exc:
                    self.log("problem with the CUL device: %s" % exc)
            time.sleep(60)

    def addClient(self, client):
        self.lock.acquire()
        try:
            self.clients.append(client)
            self.log("add client " + client.id())
        finally:
            self.lock.release()

    def delClient(self, client):
        self.lock.acquire()
        try:
            self.log("remove client " + client.id())
            self.clients.remove(client)
        finally:
            self.lock.release()

    def run(self):
        self.log("started")
        self.connectDevice()
        while True:
            c = ''
            telegram = ''
            while c != '\n':
                try:
                    c = self.cul.read(1).decode()
                except Exception as exc:
                    self.log("Device %s stopped working: %s" % (self.connectedDevice, exc))
                    self.connectedDevice = ""
                    self.connectDevice()
                    c = ''
                    telegram = ''
                telegram += c
            telegram = telegram[:-2]
            self.log("received FS20 telegram %s" % telegram)
            self.distribute(telegram)

    def distribute(self, telegram):
        for client in self.clients:
            client.received(telegram)

    def send(self, telegram):
        self.fs20Sender.send(telegram)
        self.distribute(telegram)

    def writeToCul(self, telegram):
        telegram = telegram.replace('\r', '')
        self.lock.acquire()
        try:
            self.log("writing %s to CUL" % telegram)
            self.cul.write((telegram + '\r\n').encode())
        finally:
            self.lock.release()

    def log(self, logStr):
        self.logger.log("Fs20: %s" % logStr)


class Fs20Sender(Thread):
    def __init__(self, fs20, logger):
        Thread.__init__(self)
        self.sendQueue = queue.Queue()
        self.recvQueue = queue.Queue()
        self.listenEvents = Event()
        self.listenEvents.clear()
        self.logger = logger
        self.running = True
        self.fs20 = fs20
        fs20.addClient(self)

    def send(self, telegram):
        self.listenEvents.set()
        self.sendQueue.put(telegram)

    def run(self):
        while self.running:
            telegram = self.sendQueue.get()
            self.__transmit(telegram)
        self.log("exit")

    def __transmit(self, telegram):
        sent = False
        self.log("sending %s" % telegram)
        while not sent:
            self.fs20.writeToCul(telegram)
            ack = False
            while (not ack):
                try:
                    reply = self.recvQueue.get(True, 1)
                    self.log("got %s" % reply)
                    if "LOVF" in reply:
                        self.log("received LOVF, trying again in 22 seconds")
                        ack = True
                        time.sleep(22)
                except queue.Empty:
                    sent = True
                    ack = True
        self.listenEvents.clear()
        self.log("successfully sent %s" % telegram)

    def received(self, telegram):
        if self.listenEvents.isSet():
            self.recvQueue.put(telegram)

    def id(self):
        return "Fs20Sender"

    def log(self, logStr):
        self.logger.log("Fs20Sender: " + logStr)


class SocketServer(object):
    def __init__(self, host, port, fs20, logger):
        self.host = host
        self.port = port
        self.fs20 = fs20
        self.logger = logger
        self.socket = None

    def log(self, logStr):
        self.logger.log("SocketServer: " + logStr)

    def prepare(self):
        self.log("started")
        self.fs20.daemon = True
        self.fs20.start()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.bind((self.host, self.port))
        except socket.error as exc:
            self.log("Socket bind failed: %s" % exc)
            self.logger.stop()
            sys.exit(0)

    def run(self):
        self.prepare()
        self.socket.listen(10)
        while True:
            try:
                conn, addr = self.socket.accept()
                self.log(addr[0] + ':' + str(addr[1]) + " connected")
                reqHandler = RequestHandler((conn, "%s:%d" % addr), self.fs20, self.logger)
                reqHandler.daemon = True
                reqHandler.start()
            except Exception as exc:
                self.log("exception %s" % exc)
                self.socket.close()
                sys.exit(1)
            except KeyboardInterrupt:
                self.log("keyboard interrupt")
                self.socket.close()
                self.logger.stop()
                sys.exit(1)


class RequestHandler(Thread):
    def __init__(self, connection, fs20, logger):
        Thread.__init__(self)
        self.connection = connection
        self.fs20 = fs20
        self.logger = logger

    def log(self, logStr):
        self.logger.log("RequestHandler %s: %s" % (self.connection[1], logStr))

    def received(self, telegram):
        self.connection[0].sendall(("%s\n" % telegram).encode())

    def id(self):
        return self.connection[1]

    def run(self):
        self.fs20.addClient(self)
        recvBuffer = ""
        while True:
            try:
                recvStr = self.connection[0].recv(20)
            except Exception as exc:
                self.log("Exception %s while receiving from connection %s" % (exc, self.connection[1]))
                self.fs20.delClient(self)
                break
            if not recvStr:
                self.log("Received nil data from connection " + self.connection[1])
                self.fs20.delClient(self)
                break
            recvBuffer += recvStr.decode()
            telegrams = recvBuffer.split("\n")
            for telegram in telegrams[:-1]:
                self.fs20.send(telegram)
            recvBuffer = telegrams[-1]
        self.log("Closed connection " + self.connection[1])


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: %s <port> <devices>" % sys.argv[0])
        print("ex:    %s 7890 /dev/ttyACM0 /dev/ttyACM1 /dev/ttyACM2" % sys.argv[0])
        sys.exit(-1)
    logger = Logger("/var/log/fs20/fs20")
    fs20 = Fs20(sys.argv[2:], logger)
    SocketServer(host="", port=int(sys.argv[1]), fs20=fs20, logger=logger).run()
