from __future__ import print_function
import os
import time
from multiprocessing import Process

import thriftpy2
from thriftpy2.rpc import make_client, make_server

cdir = os.path.dirname(os.path.realpath(__file__))
test_thrift = thriftpy2.load(cdir + "/spec.thrift")
Bar = test_thrift.Bar
Foo = test_thrift.Foo


def test_img():
    with open(cdir + "/test.png", 'rb') as infile:
        data = infile.read()
    obj = Bar(
        tbinary=data,
        tlist_of_binary=[b'a binary string']
    )

    class Handler(object):
        def test(self, t):
            return t

    def do_server():
        server = make_server(service=test_thrift.BarService, handler=Handler(), host='localhost', port=9090)
        server.serve()

    proc = Process(target=do_server)
    proc.start()
    time.sleep(0.1)

    client = make_client(test_thrift.BarService, host='localhost', port=9090)
    res = client.test(obj)
    print("Type of binary=", type(res.tbinary))
    print("Type of t_list_of_binary=", type(res.tlist_of_binary[0]))
    proc.terminate()


if __name__ == "__main__":
    test_img()
