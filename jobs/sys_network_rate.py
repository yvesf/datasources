from collections import namedtuple

Data = namedtuple('Data', ['hostname', 'device', 'entry', 'value'])
temp = {}

def job(device, entry, intervals):
    global temp
    f = open('/sys/class/net/' + device + '/statistics/' + entry, 'r')
    value = f.read()
    ivalue = int(value.replace("\n", ""))
    f.close()

    return_value = []
    if entry in temp:
        rate = (ivalue - temp[entry]) / intervals  # bytes/s
        if rate > 0:
            # prevent counter overflows
            return_value = [Data('laer.2.localnet.cc', device, entry, rate)]

    temp[entry] = ivalue

    return return_value
