#!/usr/bin/python3
# -*- coding: iso-8859-1 -*-

import socket
import threading
import time
import os
import sys
import ephem
import datetime

try:
    import RPi.GPIO as GPIO
except:
    GPIO = None


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
            self.sock.send((telegram + '\n').encode())
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
                    sys.exit(0)

    #				except Exception as exc:
    #					self.log("caught exception " + exc.message)
    #					self.connected = False
    #					break

    def notifyHandlers(self, telegram):
        for telegramPrefix in self.fs20handler:
            if telegramPrefix in telegram:
                self.fs20handler[telegramPrefix].handle(telegram)
                return

    def register(self, telegram, handler):
        self.fs20handler[telegram] = handler

    def log(self, logStr):
        self.logger.log("Fs20Receiver: " + logStr)


class SunEvents(object):
    def __init__(self, logger):
        self.logger = logger
        self.obs = ephem.Observer()
        self.obs.lat = '48.14'
        self.obs.long = '16.20'
        self.sun = ephem.Sun()
        self.initToday()

    def getSunrise(self):
        return self.sunrise

    def getSunZenith(self):
        return self.zenith

    def getSunset(self):
        return self.sunset

    def initToday(self):
        self.obs.date = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        self.sunrise = self.obs.next_rising(self.sun).datetime()
        self.sunset = self.obs.next_setting(self.sun).datetime()
        self.zenith = self.sunrise + (self.sunset - self.sunrise) / 2
        self.log("Daten am %s" % self.obs.date)
        self.log("Sonnenaufgang:   %s UTC" % self.sunrise)
        self.log("Sonne im Zenith: %s UTC" % self.zenith)
        self.log("Sonnenuntergang: %s UTC" % self.sunset)

    def log(self, logStr):
        self.logger.log("SunEvents: " + logStr)


class TimeController(object):
    def __init__(self, logger):
        self.logger = logger
        self.lock = threading.Lock()
        self.events = []
        self.intervalStart = datetime.datetime.utcnow()

    def start(self):
        self.log("started")
        self.run()

    def addEvent(self, event):
        try:
            self.lock.acquire()
            self.events.append(event)
        finally:
            self.lock.release()

    def run(self):
        intervalEnd = datetime.datetime.utcnow()
        self.timer = threading.Timer(60, self.run)
        self.timer.daemon = True
        self.timer.start()
        self.executeEvents(self.intervalStart, intervalEnd)
        self.intervalStart = intervalEnd

    def executeEvents(self, start, end):
        try:
            self.lock.acquire()
            for event in self.events:
                eventTime = event.getTime()
                if (start < eventTime) and (eventTime <= end):
                    self.log("executing %s" % event.getName())
                    event.runEvent()
        except Exception as exc:
            self.log("Exception: %s" % exc)
        finally:
            self.lock.release()

    def log(self, logStr):
        self.logger.log("TimeController: " + logStr)


class Event(object):
    def __init__(self, name, occasionFn, timedelta, earliest, latest, execFn, parameter, logger):
        self.name = name
        self.occasionFn = occasionFn
        self.timedelta = timedelta
        self.earliest = earliest
        self.latest = latest
        self.execFn = execFn
        self.parameter = parameter
        self.logger = logger

    def getName(self):
        return self.name

    def getTime(self):
        earliest = datetime.datetime.combine(datetime.datetime.today(), self.earliest)
        latest = datetime.datetime.combine(datetime.datetime.today(), self.latest)
        occasion = self.occasionFn() + self.timedelta
        if (occasion < earliest):
            return earliest
        if (occasion > latest):
            return latest
        return occasion

    def runEvent(self):
        self.log("executing event")
        try:
            if (self.parameter == None):
                self.execFn()
            else:
                self.execFn(self.parameter)
        except Exception as exc:
            self.log("Exception when executing event: %s" % exc)

    def log(self, str):
        self.logger.log("Event (%s): %s" % (self.name, str))


def getMidnight():
    return datetime.datetime.combine(datetime.date.today(), datetime.time(hour=0))


class ModeMonitor(object):
    def __init__(self, logger):
        if GPIO:
            GPIO.setmode(GPIO.BOARD)
            GPIO.setup(3, GPIO.IN, pull_up_down=GPIO.PUD_UP)  # Alarm
            GPIO.setup(5, GPIO.IN, pull_up_down=GPIO.PUD_UP)  # vollscharf
            GPIO.setup(7, GPIO.IN, pull_up_down=GPIO.PUD_UP)  # anwesend scharf
        self.logger = logger
        self.sunnyDay = False
        self.log("started")

    def handle(self, telegram):
        if ("F5E7E133100" in telegram):
            self.log("Schatten")
        if ("F5E7E132000" in telegram):
            self.log("Sonne, sunnyDay = True")
            self.sunnyDay = True

    def reset(self):
        self.log("Tag init: sunnyDay = False")
        self.sunnyDay = False

    def isAlarm(self):
        if GPIO:
            pin = GPIO.input(3)
            self.log("pin 3 (alarm) = %d" % pin)
            return pin == 0
        else:
            return False

    def isFullyActive(self):
        if GPIO:
            pin = GPIO.input(5)
            self.log("pin 5 (fully active) = %d" % pin)
            return pin == 0
        else:
            return True

    def isNightActive(self):
        if GPIO:
            pin = GPIO.input(7)
            self.log("pin 7 (night active) = %d" % pin)
            return pin == 0
        else:
            return False

    def log(self, str):
        self.logger.log("ModeMonitor: %s" % str)


class Controller(object):
    def __init__(self, logger, fs20Receiver, modeMonitor):
        self.logger = logger
        self.fs20Receiver = fs20Receiver
        self.modeMonitor = modeMonitor

    def morning(self, param):
        if (self.modeMonitor.isFullyActive()):
            self.log("Morgens/Haus unbewohnt")
            # self.fs20Receiver.sendList(["F404B8111", "F404B8211", "F404B8311", "F404B9011", "F404B8511", "F404B8611", "F404B8711", "F404B8811", "F404B8911"])
            self.fs20Receiver.sendList(
                ["F404B8111", "F404B8211", "F404B8311", "F404B9011", "F404B8511", "F404B8611", "F404B8711",
                 "F404B8811"])
        else:
            self.log("Morgens/anwesend")
            self.fs20Receiver.sendList(["F404B8111", "F404B8211", "F404B8311", "F404B9011", "F404B8511"])

    def schoolday(self, param):
        if not (self.modeMonitor.isFullyActive()):
            if (datetime.datetime.today().weekday() <= 4):
                self.fs20Receiver.sendList("F404B8111", "F404B8211", "F404B8311", "F404B9011", "F404B8511")

    def suedSchatten(self, param):
        if ("start" in param):
            if (self.modeMonitor.sunnyDay):
                self.log("vormittag, sonnig, Beschattung Suedseite")
                self.fs20Receiver.sendList(["F404B81202D", "F404B82202D", "F404B872044"])
        # self.fs20Receiver.sendList(["F404B81202D", "F404B82202D", "F404B872044", "F404B892044"])
        if ("end" in param):
            if (self.modeMonitor.sunnyDay):
                self.log("Ende Beschattung Suedseite")
                self.fs20Receiver.sendList(["F404B8111", "F404B8211", "F404B8711"])

    # self.fs20Receiver.sendList(["F404B8111", "F404B8211", "F404B8711", "F404B8911"])

    def noon(self, param):
        self.log("Mittags")

    def afternoon(self, param):
        if (self.modeMonitor.isFullyActive()):
            self.log("Nachmittags/Haus unbewohnt")
            if (self.modeMonitor.sunnyDay):
                self.log("sonnig, Wohnzimmer verdunkeln")
                self.fs20Receiver.sendList(["F404B832045", "F404B902045", "F404B852045"])
            else:
                self.log("keine Sonne")
        else:
            self.log("Nachmittags/anwesend")
            if (self.modeMonitor.sunnyDay):
                self.log("sonnig, Wohnzimmer beschatten")
                self.fs20Receiver.sendList(["F404B832045", "F404B902045", "F404B852045"])
            # self.fs20Receiver.sendList(["F404B832074", "F404B852074"])
            else:
                self.log("keine Sonne")

    # 0 = endlos 0,25 s
    # 1 =  1     0,5 s
    # 2 =  2     1 s
    # 3 =  3     2 s
    # 4 =  4     4 s
    # 5 =  5     8 s
    # 6 =  6    16 s
    # 7 =  7    32 s
    # 8 =  8    64 s
    # 9 =  9   128 s
    # A = 10   256 s
    # B = 11   512 s
    # C = 12  1024 s
    # Beispiel: 54 =  8s * i 4 = 32s
    # Beispiel: 2D =  1s * 13 = 13s

    def evening(self, param):
        self.log("Abends")
        self.fs20Receiver.sendList(["F404B8311", "F404B9011", "F404B8511"])

    def night(self, param):
        if (self.modeMonitor.isFullyActive()):
            self.log("Nachts/Haus unbewohnt")
            # self.fs20Receiver.sendList(["F404B8100", "F404B8200", "F404B8300", "F404B9000", "F404B8500", "F404B8600", "F404B8700", "F404B8800", "F404B8900"])
            self.fs20Receiver.sendList(
                ["F404B8100", "F404B8200", "F404B8300", "F404B9000", "F404B8500", "F404B8600", "F404B8700",
                 "F404B8800"])
        else:
            self.log("Nachts/anwesend")
            self.fs20Receiver.sendList(["F404B8100", "F404B8200", "F404B8300", "F404B9000", "F404B8500"])

    # self.fs20Receiver.sendList(["F404B8100"])

    def dawn(self, param):
        self.log("Daemmerung")

    # self.fs20Receiver.sendList(["F404B8600", "F404B8700", "F404B8800", "F404B8900"])

    def log(self, logStr):
        self.logger.log("TimeController: %s" % logStr)


class LightsControl(object):
    # alle Lampen im Haus mit dem Schalter bei der Tür ein/ausschalten
    def __init__(self, fs20Receiver, logger):
        self.logger = logger
        self.fs20Receiver = fs20Receiver

    def handle(self, telegram):
        if "F404B0011" in telegram:
            self.fs20Receiver.sendList(["F404B0111", "F404B0211", "F404B0311",
                                        "F404B0411", "F404B0511", "F404B0611", "F404B0711", "F404B0811",
                                        "F404B0911", "F404B0A11", "F404B0B11", "F404B0C11", "F404B0D11",
                                        "F404B0E11", "F404B0F11"])
        elif "F404B0000" in telegram:
            self.fs20Receiver.sendList(["F404B0100", "F404B0200", "F404B0300",
                                        "F404B0400", "F404B0500", "F404B0600", "F404B0700", "F404B0800",
                                        "F404B0900", "F404B0A00", "F404B0B00", "F404B0C00", "F404B0D00",
                                        "F404B0E00", "F404B0F00"])


class BlindsControl(object):
    # alle Rolllaeden im Haus mit dem Schalter bei der Tuer rauf/runterfahren
    def __init__(self, fs20Receiver, logger):
        self.fs20Receiver = fs20Receiver

    def handle(self, telegram):
        if "F404B8011" in telegram:
            self.fs20Receiver.sendList(["F404B8111", "F404B8211", "F404B8311",
                                        "F404B9011", "F404B8511", "F404B8611", "F404B8711", "F404B8811",
                                        "F404B8911"])
        elif "F404B8000" in telegram:
            self.fs20Receiver.sendList(["F404B8100", "F404B8200", "F404B8300",
                                        "F404B9000", "F404B8500", "F404B8600", "F404B8700", "F404B8800",
                                        "F404B8900"])


if __name__ == '__main__':
    startTime = time.ctime()

    logger = Logger("/var/log/fs20/fs20-osv-ctrl")

    fs20Receiver = Fs20Receiver(sys.argv[1], logger)
    sunEvents = SunEvents(logger)
    modeMonitor = ModeMonitor(logger)
    fs20Receiver.register("F5E7E13", modeMonitor)
    fs20Receiver.register("F404B00", LightsControl(fs20Receiver, logger))
    fs20Receiver.register("F404B80", BlindsControl(fs20Receiver, logger))

    controller = Controller(logger, fs20Receiver, modeMonitor)

    timeController = TimeController(logger)

    timeController.addEvent(
        Event(
            name="Sonne berechnen",
            occasionFn=getMidnight,
            timedelta=datetime.timedelta(hours=0),
            earliest=datetime.time(hour=0),
            latest=datetime.time(hour=0),
            execFn=sunEvents.initToday,
            parameter=None,
            logger=logger
        )
    )

    timeController.addEvent(
        Event(
            name="Tag initialisieren",
            occasionFn=getMidnight,
            timedelta=datetime.timedelta(hours=0),
            earliest=datetime.time(hour=0),
            latest=datetime.time(hour=0),
            execFn=modeMonitor.reset,
            parameter=None,
            logger=logger
        )
    )

    timeController.addEvent(
        Event(
            name="Sonnenaufgang",
            occasionFn=sunEvents.getSunrise,
            timedelta=datetime.timedelta(hours=1, minutes=45),
            earliest=datetime.time(hour=5, minute=15),  # Sommerzeit: 7:15, Normalzeit: 6:15
            latest=datetime.time(hour=7),  # Sommerzeit: 9:00, Normalzeit: 8:00
            execFn=controller.morning,
            parameter=("F404B8111", "F404B8211", "F404B8611", "F404B8711", "F404B8811", "F404B8911"),
            logger=logger
        )
    )

    timeController.addEvent(
        Event(
            name="Schultag",
            occasionFn=getMidnight,
            timedelta=datetime.timedelta(hours=3, minutes=10),
            earliest=datetime.time(hour=4, minute=10),  # Sommerzeit: 6:10
            latest=datetime.time(hour=4, minute=10),  # Sommerzeit: 6:10
            execFn=controller.schoolday,
            parameter=("F404B8111", "F404B8211", "F404B8611", "F404B8711", "F404B8811", "F404B8911"),
            logger=logger
        )
    )

    #	timeController.addEvent(
    #		Event(
    #			name       = "Suedschatten start",
    #			occasionFn = sunEvents.getSunrise,
    #			timedelta  = datetime.timedelta(hours=2, minutes=45),
    #			earliest   = datetime.time(hour = 7),  # Sommerzeit:  9:00
    #			latest     = datetime.time(hour = 9),  # Sommerzeit: 11:00
    #			execFn     = controller.suedSchatten,
    #			parameter  = ("start"),
    #			logger     = logger
    #		)
    #	)

    timeController.addEvent(
        Event(
            name="Suedschatten ende",
            occasionFn=sunEvents.getSunZenith,
            timedelta=datetime.timedelta(hours=3),
            earliest=datetime.time(hour=11),  # Sommerzeit: 13:00
            latest=datetime.time(hour=13),  # Sommerzeit: 15:00
            execFn=controller.suedSchatten,
            parameter=("end"),
            logger=logger
        )
    )

    # Mittags (Sonne im Zenith): derzeit ohne Funktion
    timeController.addEvent(
        Event(
            name="Mittags",
            occasionFn=sunEvents.getSunZenith,
            timedelta=datetime.timedelta(hours=-1),
            earliest=datetime.time(hour=10),  # Sommerzeit: 12:00
            latest=datetime.time(hour=13),  # Sommerzeit: 15:00
            execFn=controller.noon,
            parameter=("F404B8311", "F404B9011", "F404B8511"),
            logger=logger
        )
    )

    # Nachmittags, wenn die Sonne ins Wohnzimmer scheint: Die FRolläden zur Terrasse runterfahren damit sich das Haus nicht so aufheizt (nicht sinnvoll im Winter - im Sommer wichtig auch für den Kühlschrank)
    #	timeController.addEvent(
    #		Event(
    #			name       = "Nachmittags",
    #			occasionFn = sunEvents.getSunZenith,
    #			timedelta  = datetime.timedelta(hours=0, minutes=-30),
    #			earliest   = datetime.time(hour = 10), # Sommerzeit: 12:00
    #			latest     = datetime.time(hour = 12), # Sommerzeit: 14:00
    #			execFn     = controller.afternoon,
    #			parameter  = ("F404B852034"),
    #			logger     = logger
    #		)
    #	)

    # Am Abend, wenn die Sonne nicht mehr stark reinscheint, die Rollläden zur Terrasse wieder rauffahren
    timeController.addEvent(
        Event(
            name="Abends",
            occasionFn=sunEvents.getSunset,
            timedelta=datetime.timedelta(hours=-2),
            earliest=datetime.time(hour=13),  # Sommerzeit: 15:00
            latest=datetime.time(hour=16),  # Sommerzeit: 18:00
            execFn=controller.evening,
            parameter=("F404B852034"),
            logger=logger
        )
    )

    # Über Nacht alle Rollläden runterfahren
    timeController.addEvent(
        Event(
            name="Nachts",
            occasionFn=sunEvents.getSunset,
            timedelta=datetime.timedelta(minutes=45),
            earliest=datetime.time(hour=16),  # Sommerzeit: 18:00
            latest=datetime.time(hour=20),  # Sommerzeit: 22:00
            execFn=controller.night,
            parameter=("F404B8511"),
            logger=logger
        )
    )

    # Morgendämmerung: die Rolläden am Dachboden runterfahren bevor die Vödel zwitschern und die Sonne reinscheint
    #	timeController.addEvent(
    #		Event(
    #			name       = "Daemmerung",
    #			occasionFn = sunEvents.getSunrise,
    #			timedelta  = datetime.timedelta(hours=-1),
    #			earliest   = datetime.time(hour = 2), # Sommerzeit: 4:00
    #			latest     = datetime.time(hour = 4), # Sommerzeit: 6:00
    #			execFn     = controller.dawn,
    #			parameter  = ("F404B8511"),
    #			logger     = logger
    #		)
    #	)

    timeController.start()

    fs20Receiver.run()

# F5E7E133100 = Sonne
# F5E7E120000 = Schatten
#
# 13:03:53 F5E7E133100 Schatten
# 13:03:58 F5E7E132000 Sonne
# 13:13:53 F5E7E133100 Schatten
# 18:13:53 F5E7E133100 Schatten
# 18:13:58 F5E7E132000 Sonne
# 18:33:53 F5E7E133100 Schatten
