import array

class WrongPartitionException(Exception):
  pass

class OffsetOutOfRangeException(Exception):
  pass

class InvalidMessageException(Exception):
  pass

class InvalidMessageSizeException(Exception):
  pass

class InvalidTopicException(Exception):
  pass

class UnknownException(Exception):
  pass

class ErrorMapping(object):
  """ A bi-directional mapping between error codes and exceptions x """

  EMPTY_BYTEBUFFER = array.array('c', ' ')

  UNKNOWN_CODE             = -1
  NO_ERROR                 = 0
  OFFSET_OUT_OF_RANGE_CODE = 1
  INVALID_MESSAGE_CODE     = 2
  WRONG_PARTITION_CODE     = 3
  INVALID_RETCH_SIZE_CODE  = 4
  INVALID_TOPIC_CODE       = 5

  EXCEPTION_TO_CODE = {
    'OffsetOutOfRangeException'  : OFFSET_OUT_OF_RANGE_CODE,
    'InvalidMessageException'    : INVALID_MESSAGE_CODE,
    'WrongPartitionException'    : WRONG_PARTITION_CODE,
    'InvalidMessageSizeException': INVALID_RETCH_SIZE_CODE,
    'InvalidTopicException'      : INVALID_TOPIC_CODE,
  }

  CODE_TO_EXCEPTION = dict([[v, k] for k, v in EXCEPTION_TO_CODE.iteritems()])

  def code_for(self, exception):
    try:
      return self.EXCEPTION_TO_CODE[exception.__class__]
    except KeyError:
      return self.UNKNOWN_CODE

  def maybe_throw_exception(self, code):
    if code:

      try:
        class_name = self.CODE_TO_EXCEPTION[code]
      except KeyError:
        class_name = 'UnknownException'

      raise type(class_name, (object,), {})
