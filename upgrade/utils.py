import datetime


def convert_str_to_bool(string):
    if isinstance(string, bool):
        return string

    if string.lower() == "true":
        return True
    elif string.lower() == "false":
        return False

    raise ValueError("cannot convert to bool")


def date_suffix():
    return str(datetime.datetime.now()).replace(" ", "-").replace(":", "-")