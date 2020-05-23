from contextlib import AbstractContextManager
from socket import socket
from typing import Tuple

HEADER_SIZE = 4  # number of characters in all headers
DESTINATION_LENGTH_SIZE = 4
PAYLOAD_LENGTH_SIZE = 8  # number of bytes used to encode payload length


class Connection(AbstractContextManager):
    __slots__ = ('conn', 'addr')

    def __init__(self, conn: socket, addr: Tuple[str, int]):
        self.conn, self.addr = conn, addr

    @staticmethod
    def create(host: str, port: int):
        sock = socket()
        addr = (host, port)
        sock.connect(addr)
        return Connection(sock, addr)

    def send_raw(self, header: str, destination: str, content: bytes):
        assert len(header) == HEADER_SIZE
        self.conn.sendall(header.encode())
        self.conn.sendall(len(destination).to_bytes(DESTINATION_LENGTH_SIZE, byteorder='big'))
        self.conn.sendall(destination.encode())
        self.conn.sendall(len(content).to_bytes(PAYLOAD_LENGTH_SIZE, byteorder='big'))
        self.conn.sendall(content)

    def recv_header(self) -> str:
        return self.conn.recv(HEADER_SIZE).decode()

    def recv_destination(self) -> str:
        length = int.from_bytes(self.conn.recv(DESTINATION_LENGTH_SIZE), byteorder='big')
        return self.conn.recv(length).decode()

    def recv_raw(self, max_package: int = 2048) -> bytes:
        length = int.from_bytes(self.conn.recv(PAYLOAD_LENGTH_SIZE), byteorder='big')
        chunks = []
        bytes_recd = 0
        while bytes_recd < length:
            chunk = self.conn.recv(min(length - bytes_recd, max_package))
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        ret = b''.join(chunks)
        assert len(ret) == length
        return ret

    def recv_message(self) -> Tuple[str, str, bytes]:
        return self.recv_header(), self.recv_destination(), self.recv_raw()

    def __exit__(self, *exc_info):
        self.conn.close()


def find_open_port():
    try:
        sock = socket()
        sock.bind(('', 0))
        port = sock.getsockname()[1]
        sock.close()
        return port
    except:
        raise ValueError("Could not find open port")
