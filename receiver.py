import serial
from loguru import logger


class Receiver(object):
    def __init__(self, *, port: str, baud: int = 0):
        with serial.Serial(port=port, baudrate=baud) as sp:
            while True:
                try:
                    data = sp.readline()
                except (serial.SerialException) as e:
                    logger.error(e)
                else:
                    print(data)


if __name__ == '__main__':
    def main():
        R = Receiver(port='/dev/ttyACM0', baud=9600)
        pass


    main()
