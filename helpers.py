import datetime
import pprint
import struct

import dateutil


def get_val(target_object, key, default_value=None):
    if isinstance(target_object, dict):
        retn = target_object.get(key, default_value)
    else:
        try:
            retn = getattr(target_object, key)
        except AttributeError:
            retn = default_value
        else:
            if retn is None:
                retn = default_value

    return retn


def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]

    retn = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

    return dateutil.parser.parse(retn)


def safe_json_serialize(input_dict):
    for k, v in input_dict.items():
        if isinstance(v, dict):
            input_dict[k] = safe_json_serialize(v)
        elif isinstance(v, datetime.datetime):
            input_dict[k] = v.isoformat()

    return input_dict


def print_vals(target_object):
    if not isinstance(target_object, dict):
        print_object = dict()

        for key in dir(target_object):
            print_object.update({key: get_val(target_object, key)})

        pprint.pprint(print_object)
    else:
        pprint.pprint(target_object)
