import serial


class NMEALogger(object):
    def __init__(self, *, port: str, baud: int):
        self.port = port
        self.baud = baud

    def exec(self):
        with serial.Serial(port=self.port, baudrate=self.baud) as sp:
            loop = True
            while loop:
                line = sp.readline()


if __name__ == '__main__':
    def main():
        pass

    main()

