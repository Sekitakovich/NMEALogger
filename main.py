import time
import serial
from threading import Thread
import sqlite3
from loguru import logger


class NMEASaver(object):
    def __init__(self, *, file: str):
        self.isReady = True
        initScript = """
            create table if not exists  nmea
            (
                id   INTEGER default 0 not null
                    primary key autoincrement
                    unique,
                at   TEXT    default '' not null,
                body TEXT    default '' not null
            );
        """

        try:
            self.db = sqlite3.connect(file)
        except (sqlite3.Error) as e:
            self.isReady = False
            logger.error(e)
        else:
            self.exec(query=initScript)
            pass

    def exec(self, *, query: str) -> bool:
        success = True
        cursor = self.db.cursor()
        try:
            cursor.execute(query)
        except (sqlite3.Error) as e:
            success = False
            logger.error(e)
        else:
            self.db.commit()
        return success


class NMEALogger(Thread):
    def __init__(self, *, port: str, baudrate: int, nickname: str):
        super().__init__()
        self.daemon = True

        self.port = port
        self.baudrate = baudrate
        self.nickname = nickname

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
        logger.info(f'Bye!')

    def run(self) -> None:
        if self.isReady:
            loop = True
            while loop:
                try:
                    line = self.sp.readline()
                except (serial.SerialException) as e:
                    logger.error(e)
                except (KeyboardInterrupt) as e:
                    logger.info(f'Quit')
                    break
                else:
                    logger.debug(line)


class Main(object):
    def __init__(self):
        self.collector = NMEALogger(port='COM4', baudrate=9600, nickname='GPS')
        if self.collector.isReady:
            self.collector.start()
            time.sleep(10)


if __name__ == '__main__':
    def main():
        # MT = Main()

        ss = NMEASaver(file='sample.db')


    main()
