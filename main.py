import pathlib
import time
import serial
from threading import Thread, Lock
from queue import Queue, Empty
import sqlite3
from dataclasses import dataclass
from datetime import datetime as dt
from typing import List

from loguru import logger


@dataclass()
class Package(object):
    at: dt
    type: str
    body: bytes


class NMEASaver(Thread):
    def __init__(self, *, dbFile: str, bufferSize: int=1024, timeoutSecs=5):
        super().__init__()
        self.daemon = True

        self.locker = Lock()
        self.entryQueue = Queue()
        self.buffer: List[Package] = []

        self.timeoutSecs = timeoutSecs  # Queue.getの際のタイムアウト値
        self.bufferSize = bufferSize  # buffer full
        self.counter = 0

        self.dateFormat = f'%Y-%m-%d %H:%M:%S.%f'

        self.isReady = True
        self.dbFile = pathlib.Path(dbFile)
        if self.dbFile.exists() is False:
            logger.warning(f'preparing [{dbFile}]')
            if self.prepare() is False:
                self.isReady = False
        else:
            logger.info(f'using [{dbFile}]')

    def __del__(self):
        logger.info(f'Bye bye!')

    def prepare(self) -> bool:
        success = True
        initScript = """
            create table if not exists nmea
            (
                id INTEGER default 0 not null primary key autoincrement unique,
                at TEXT default '' not null,
                type TEXT default '' not null,
                body blob default '' not null
            );
        """

        try:
            with sqlite3.connect(self.dbFile) as db:
                cursor = db.cursor()
                cursor.execute(initScript)
                db.commit()
        except (sqlite3.Error) as e:
            logger.error(e)
            success = False
        return success

    def append(self):
        try:
            ts = dt.now()
            volume = len(self.buffer)
            with sqlite3.connect(self.dbFile) as db:
                cursor = db.cursor()
                query = f"insert into nmea(at,type,body) values(?,?,?)"
                params = []
                for package in self.buffer:
                    at = package.at.strftime(self.dateFormat)
                    thisType = package.type
                    body = package.body
                    params.append([at, thisType, body])
                cursor.executemany(query, params)
                db.commit()
        except (sqlite3.Error) as e:
            logger.error(e)
        else:
            te = dt.now()
            secs = (te-ts).total_seconds()
            logger.info(f'append {volume} records in {secs} secs')
        self.counter = 0
        self.buffer.clear()

    def run(self) -> None:
        while self.isReady:
            try:
                package: Package = self.entryQueue.get(timeout=self.timeoutSecs)
            except (Empty) as e:
                if self.buffer:
                    self.append()
                    logger.warning(e)
            except (KeyboardInterrupt) as e:
                logger.info(f'Quit')
                break
            else:
                logger.debug(package)
                self.buffer.append(package)
                self.counter += 1
                if self.counter == self.bufferSize:
                    self.append()
                else:
                    pass


class NMEALogger(Thread):
    def __init__(self, *, port: str, baudrate: int, qp: Queue, name: str):
        super().__init__()
        self.daemon = True
        self.name = name

        self.port = port
        self.baudrate = baudrate
        self.qp = qp

        self.counter = 0
        self.isReady = True
        try:
            self.sp = serial.Serial(port=self.port, baudrate=self.baudrate)
        except (serial.SerialException) as e:
            self.isReady = False
            logger.error(e)

    def __del__(self):
        if self.isReady:
            self.sp.close()
        logger.info(f'See you again')

    def run(self) -> None:
        if self.isReady:
            while self.isReady:
                try:
                    line = self.sp.readline()
                    now = dt.now()
                except (serial.SerialException) as e:
                    logger.error(e)
                    break
                except (KeyboardInterrupt) as e:
                    logger.info(f'Quit')
                    break
                else:
                    package = Package(type=self.name, body=line, at=now)
                    self.qp.put(package)


class Main(object):
    def __init__(self):
        name = dt.now().strftime('%Y-%m-%d')
        filename = f'{name}.db'
        self.saver = NMEASaver(dbFile=filename, bufferSize=64)
        if self.saver.isReady:
            self.saver.start()
            self.collector = NMEALogger(port='COM4', baudrate=9600, qp=self.saver.entryQueue, name='GPS')
            if self.collector.isReady:
                self.collector.start()
                try:
                    time.sleep(5)
                except (KeyboardInterrupt) as e:
                    logger.error(e)

                self.collector.isReady = False
                self.collector.join()

                self.saver.isReady = False
                self.saver.join()


if __name__ == '__main__':
    def main():
        MT = Main()


    main()
