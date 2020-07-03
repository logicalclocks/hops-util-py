from . import mirrored_reservation
import sys

if __name__ == "__main__":
  host = sys.argv[1]
  port = int(sys.argv[2])
  addr = (host, port)
  client = mirrored_reservation.Client(addr)
  client.request_stop()
  client.close()
