import socket


def job(device, stat_type):
    f = open('/sys/class/net/' + device + '/statistics/' + stat_type, 'r')
    value = f.read()
    ivalue = int(value.replace("\n", ""))
    f.close()
    return {'hostname': socket.gethostname(), 'device': device, 'stat_type': stat_type, 'value': ivalue}


if __name__ == "__main__":
    from pprint import pprint

    pprint(job("eth1", "rx_bytes"))
