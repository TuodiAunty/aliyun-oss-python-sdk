#!/usr/bin/env python
#coding=utf-8

import struct

"""
The adapter class for BatchGet object's response.
The response consists of frames. Each frame has the following format:

// |version| Frame-Type | channel-id | Payload Length|Header Checksum|Payload|Payload Checksum|
// |<1---->| <--3 bytes>|<--4 bytes->| <--4 bytes--->|<--4 bytes-----><-n/a--><--4 bytes----->|

"""

class BatchGetObjectStruct:
    type = 0
    data = ""
    ref_id = 0

class ObjectFrame:
    header = {}
    data = ""

class BatchGetResponseAdapter(object):

    _CHUNK_SIZE = 8 * 1024
    _CONTINIOUS_FRAME_TYPE= 0xFF3
    _META_FRAME_TYPE = 0xFF1
    _DATA_FRAME_TYPE = 0xFF2
    _END_FRAME_TYPE = 0xFF4
    _INVALIDATE_FRAME_TYPE = 0xFF

    def __init__(self, response, progress_callback = None):
        self.response = response
        self.frame_off_set = 0
        self.frame_length = 0
        self.frame_data = b''
        self.check_sum_flag = 0
        self.file_offset = 0
        self.finished = 0
        self.raw_buffer = b''
        self.raw_buffer_offset = 0
        self.final_status = 0
        self.error = b''
        self.resp_success_count = 0
        self.total_count = 0
        self.frame_ref_id = 0
        self.frame_type = BatchGetResponseAdapter._INVALIDATE_FRAME_TYPE
        self.resp_content_iter = response.__iter__()
        self.callback = progress_callback

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if  self.response.status/100 != 2:
            data = self.response.read(1024 * 64)
            if len(data) != 0:
                return data
            else: raise StopIteration

        while self.finished == 0:
            if self.frame_off_set < self.frame_length:
                frame_data = self.read_raw(self.frame_length - self.frame_off_set)
                print("Reading FrameData:" + str(len(frame_data)) + " buffer size" + str(self.frame_length - self.frame_off_set))
                self.frame_length = self.frame_off_set = 0
                frame = BatchGetObjectStruct()
                frame.type = self.frame_type
                frame.data = frame_data
                frame.ref_id = self.frame_ref_id
                if (self.callback is not None):
                    self.callback(self.frame_ref_id, self.file_offset, self.content_length)
                return frame
            else:
                self.read_next_frame()

        raise StopIteration

    def read_raw(self, amt):
        ret = b''
        read_count = 0
        while amt > 0 and self.finished == 0:
            size = len(self.raw_buffer)
            if size == 0:
                self.raw_buffer =  next(self.resp_content_iter)
                self.raw_buffer_offset = 0
                size = len(self.raw_buffer)
                print "read_raw %s" % size
                if size == 0:
                    break

            if size - self.raw_buffer_offset >= amt:
                data = self.raw_buffer[self.raw_buffer_offset:self.raw_buffer_offset + amt]
                data_size = len(data)
                self.raw_buffer_offset += data_size
                ret += data
                read_count += data_size
                amt -= data_size
            else:
                data = self.raw_buffer[self.raw_buffer_offset:]
                data_len = len(data)
                ret += data
                read_count += data_len
                amt -= data_len
                self.raw_buffer = b''

        return ret

    def read_next_frame(self):
        if (self.check_sum_flag == 1):
            self.read_raw(4)

        frame_type = bytearray(self.read_raw(4))
        ref_id = bytearray(self.read_raw(4))
        ref_id.reverse()
        ref_id_val = struct.unpack("I", bytearray(ref_id))[0]
        self.frame_ref_id = ref_id_val

        payload_length = bytearray(self.read_raw(4))
        header_checksum = bytearray(self.read_raw(4))
        frame_type[0] = 0 #mask the version bit
        frame_type.reverse() # convert to little endian
        frame_type_val = struct.unpack("I", bytearray(frame_type))[0]
        self.frame_type = frame_type_val
    
        if frame_type_val == BatchGetResponseAdapter._DATA_FRAME_TYPE or frame_type_val == BatchGetResponseAdapter._META_FRAME_TYPE:
            payload_length.reverse() # convert to little endian
            payload_length_val = struct.unpack("I", bytearray(payload_length))[0]
            self.frame_length = payload_length_val
            self.frame_off_set = 0
            self.check_sum_flag= 1
            print("Get Data Frame:" + str(self.frame_length))
        elif frame_type_val == BatchGetResponseAdapter._CONTINIOUS_FRAME_TYPE:
            self.frame_length = self.frame_off_set = 0
            self.check_sum_flag=1
            print("GetContiniousFrame:" + str(self.frame_length))
        elif frame_type_val == BatchGetResponseAdapter._END_FRAME_TYPE:
            self.frame_off_set = 0
            payload_length.reverse()
            payload_length_val = struct.unpack("I", bytearray(payload_length))[0]
            request_object_count = bytearray(self.read_raw(4))
            request_object_count.reverse()
            request_object_count_val = struct.unpack("I", bytearray(request_object_count))[0]
            success_count = bytearray(self.read_raw(4))
            success_count.reverse()
            success_count_val = struct.unpack("I", bytearray(success_count))[0]

            status_bytes = bytearray(self.read_raw(4))
            status_bytes.reverse()
            status = struct.unpack("I", bytearray(status_bytes))[0]
            error_msg_size = payload_length_val - 12
            if error_msg_size > 0:
                self.error = self.read_raw(error_msg_size)
                print(self.error)
            self.read_raw(4) # read the payload checksum
            self.final_status = status
            self.total_count = request_object_count_val
            self.resp_success_count = success_count_val
            self.frame_length = 0
            self.finished = 1
            print "end of stream.done"
        else:
            print "error. invalid frame-type %s" % frame_type_val
            self.final_status = 400
            self.finished = 1
