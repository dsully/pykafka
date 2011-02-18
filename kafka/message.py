import struct
import zlib

def parse_from(binary):
  """ Turn a packed binary message as recieved from a Kafka broker into a :class:`Message`. """

  # A message. The format of an N byte message is the following:
  # 1 byte "magic" identifier to allow format changes
  # 4 byte CRC32 of the payload
  # N - 5 byte payload
  size     = struct.unpack('>i', binary[0:4])[0]
  magic    = struct.unpack('>B', binary[4:5])[0]
  checksum = struct.unpack('>i', binary[5:9])[0]
  payload  = binary[9:9+size]

  return Message(payload, magic, checksum)

class Message(object):
  """ A Kafka Message object. """

  MAGIC_IDENTIFIER_DEFAULT = 0

  def __init__(self, payload=None, magic=MAGIC_IDENTIFIER_DEFAULT, checksum=None):
    self.magic    = magic
    self.checksum = checksum
    self.payload  = None

    if payload is not None:
      self.payload = str(payload)

    if self.payload is not None and self.checksum is None:
      self.checksum = self.calculate_checksum()

  def __str__(self):
    return self.payload

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self.magic == other.magic and self.payload == other.payload and self.checksum == other.checksum

    return False

  def __ne__(self, other):
    return not self.__eq__(other)

  def calculate_checksum(self):
    """ Returns the checksum for the payload. """

    return zlib.crc32(self.payload)

  def is_valid(self):
    """ Returns true if the checksum for this message is valid. """
    return self.checksum == self.calculate_checksum()

  def encode(self):
    """ Encode a :class:`Message` to binary form. """

    # <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>
    return struct.pack('>Bi%ds' % len(self.payload), self.magic, self.calculate_checksum(), self.payload)

