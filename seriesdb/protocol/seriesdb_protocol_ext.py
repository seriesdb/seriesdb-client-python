import seriesdb.protocol.seriesdb_protocol_pb2 as seriesdb_protocol_pb2


def encode_msg(msg):
    if msg.__class__ == seriesdb_protocol_pb2.PingReq:
        return (1).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.PingRep:
        return (2).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.SetRowsReq:
        return (3).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.SetRowsRep:
        return (4).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.DeleteRowsSinceReq:
        return (15).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.DeleteRowsSinceRep:
        return (16).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetFirstRowReq:
        return (27).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetFirstRowRep:
        return (28).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetLastRowReq:
        return (29).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetLastRowRep:
        return (30).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetBoundaryRowsReq:
        return (35).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetBoundaryRowsRep:
        return (36).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsSinceReq:
        return (37).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsSinceRep:
        return (38).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsUntilReq:
        return (39).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsUntilRep:
        return (40).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsUntilLastReq:
        return (43).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsUntilLastRep:
        return (44).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsBetweenReq:
        return (45).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetRowsBetweenRep:
        return (46).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetFirstKeyReq:
        return (47).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetFirstKeyRep:
        return (48).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetLastKeyReq:
        return (49).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetLastKeyRep:
        return (50).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetBoundaryKeysReq:
        return (55).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetBoundaryKeysRep:
        return (56).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValueReq:
        return (57).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValueRep:
        return (58).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetNthLastValueReq:
        return (65).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetNthLastValueRep:
        return (66).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesSinceReq:
        return (67).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesSinceRep:
        return (68).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesUntilReq:
        return (69).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesUntilRep:
        return (70).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesUntilLastReq:
        return (73).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesUntilLastRep:
        return (74).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesBetweenReq:
        return (75).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetValuesBetweenRep:
        return (76).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.DestroyTableReq:
        return (79).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.DestroyTableRep:
        return (80).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.RenameTableReq:
        return (81).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.RenameTableRep:
        return (82).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetTablesReq:
        return (83).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.GetTablesRep:
        return (84).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.OkRep:
        return (99).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    elif msg.__class__ == seriesdb_protocol_pb2.ErrorRep:
        return (100).to_bytes(1, 'little', signed=False) + msg.SerializeToString()
    else:
      raise TypeError(f"Unknown msg type: {msg.__class__}")


def decode_msg(encoded_msg):
    msg_type_uint32 = int.from_bytes(encoded_msg[:1], byteorder='little')
    if msg_type_uint32 == 1:
        msg = seriesdb_protocol_pb2.PingReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 2:
        msg = seriesdb_protocol_pb2.PingRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 3:
        msg = seriesdb_protocol_pb2.SetRowsReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 4:
        msg = seriesdb_protocol_pb2.SetRowsRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 15:
        msg = seriesdb_protocol_pb2.DeleteRowsSinceReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 16:
        msg = seriesdb_protocol_pb2.DeleteRowsSinceRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 27:
        msg = seriesdb_protocol_pb2.GetFirstRowReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 28:
        msg = seriesdb_protocol_pb2.GetFirstRowRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 29:
        msg = seriesdb_protocol_pb2.GetLastRowReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 30:
        msg = seriesdb_protocol_pb2.GetLastRowRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 35:
        msg = seriesdb_protocol_pb2.GetBoundaryRowsReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 36:
        msg = seriesdb_protocol_pb2.GetBoundaryRowsRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 37:
        msg = seriesdb_protocol_pb2.GetRowsSinceReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 38:
        msg = seriesdb_protocol_pb2.GetRowsSinceRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 39:
        msg = seriesdb_protocol_pb2.GetRowsUntilReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 40:
        msg = seriesdb_protocol_pb2.GetRowsUntilRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 43:
        msg = seriesdb_protocol_pb2.GetRowsUntilLastReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 44:
        msg = seriesdb_protocol_pb2.GetRowsUntilLastRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 45:
        msg = seriesdb_protocol_pb2.GetRowsBetweenReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 46:
        msg = seriesdb_protocol_pb2.GetRowsBetweenRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 47:
        msg = seriesdb_protocol_pb2.GetFirstKeyReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 48:
        msg = seriesdb_protocol_pb2.GetFirstKeyRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 49:
        msg = seriesdb_protocol_pb2.GetLastKeyReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 50:
        msg = seriesdb_protocol_pb2.GetLastKeyRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 55:
        msg = seriesdb_protocol_pb2.GetBoundaryKeysReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 56:
        msg = seriesdb_protocol_pb2.GetBoundaryKeysRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 57:
        msg = seriesdb_protocol_pb2.GetValueReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 58:
        msg = seriesdb_protocol_pb2.GetValueRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 65:
        msg = seriesdb_protocol_pb2.GetNthLastValueReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 66:
        msg = seriesdb_protocol_pb2.GetNthLastValueRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 67:
        msg = seriesdb_protocol_pb2.GetValuesSinceReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 68:
        msg = seriesdb_protocol_pb2.GetValuesSinceRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 69:
        msg = seriesdb_protocol_pb2.GetValuesUntilReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 70:
        msg = seriesdb_protocol_pb2.GetValuesUntilRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 73:
        msg = seriesdb_protocol_pb2.GetValuesUntilLastReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 74:
        msg = seriesdb_protocol_pb2.GetValuesUntilLastRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 75:
        msg = seriesdb_protocol_pb2.GetValuesBetweenReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 76:
        msg = seriesdb_protocol_pb2.GetValuesBetweenRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 79:
        msg = seriesdb_protocol_pb2.DestroyTableReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 80:
        msg = seriesdb_protocol_pb2.DestroyTableRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 81:
        msg = seriesdb_protocol_pb2.RenameTableReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 82:
        msg = seriesdb_protocol_pb2.RenameTableRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 83:
        msg = seriesdb_protocol_pb2.GetTablesReq()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 84:
        msg = seriesdb_protocol_pb2.GetTablesRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 99:
        msg = seriesdb_protocol_pb2.OkRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    elif msg_type_uint32 == 100:
        msg = seriesdb_protocol_pb2.ErrorRep()
        msg.ParseFromString(encoded_msg[1:])
        return msg
    else:
      raise TypeError(f"Unknown msg type: {msg_type_uint32}")
