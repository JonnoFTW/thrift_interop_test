import logging
import os
import socket
import time
from multiprocessing import Process

import pytest
import six

from thrift.protocol.TBinaryProtocol import TBinaryProtocolFactory as T_TBinaryProtocolFactory
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory as T_TCompactProtocolFactory
from thrift.protocol.TJSONProtocol import TJSONProtocolFactory as T_TJSONProtocolFactory
from thrift.transport import TSocket, TTransport
from thrift.server import TServer

import thriftpy2
from thriftpy2.protocol import TApacheJSONProtocolFactory, TCompactProtocolFactory
from thriftpy2.protocol import TCyBinaryProtocolFactory, TBinaryProtocolFactory
from thriftpy2.transport import TCyBufferedTransportFactory
from thriftpy2.transport.buffered import TBufferedTransportFactory
from thriftpy2.protocol.binary import TBinaryProtocolFactory

from spec import BarService
from spec.ttypes import Foo, Bar

logging.basicConfig(level=logging.DEBUG)

test_thrift = thriftpy2.load(os.path.dirname(os.path.realpath(__file__)) + "/spec.thrift")
tp2_Bar = test_thrift.Bar
tp2_Foo = test_thrift.Foo


def recursive_vars(obj):
    if isinstance(obj, six.string_types):
        return six.ensure_str(obj)
    if isinstance(obj, six.binary_type):
        return six.ensure_binary(obj)
    if isinstance(obj, (int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: recursive_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursive_vars(v) for v in obj]
    if isinstance(obj, set):
        return [recursive_vars(v) for v in obj]
    if hasattr(obj, '__dict__'):
        return recursive_vars(vars(obj))


def make_data(bar_cls, foo_cls):
    return bar_cls(
        tbool=False,
        tbyte=16,
        tdouble=1.234567,
        tlong=123123123,
        tshort=123,
        tint=12345678,
        tstr="Testing String",
        tsetofints={1, 2, 3, 4, 5},
        tmap_of_int2str={
            1: "one",
            2: "two",
            3: "three"
        },
        tlist_of_strings=["how", "do", "i", "test", "this?"],
        tmap_of_str2foo={'first': foo_cls("first"), "2nd": foo_cls("baz")},
        tmap_of_str2foolist={
            'test': [foo_cls("test list entry")]
        },
        tmap_of_str2mapofstring2foo={
            "first": {
                "second": foo_cls("testing")
            }
        },
        tmap_of_str2stringlist={
            "words": ["dog", "cat", "pie"],
            "other": ["test", "foo", "bar", "baz", "quux"]
        },
        tfoo=foo_cls("test food"),
        tlist_of_foo=[foo_cls("1"), foo_cls("2"), foo_cls("3")],
        tlist_of_maps2int=[
            {"one": 1, "two": 2, "three": 3}
        ],
        tmap_of_int2foo={
            1: foo_cls("One"),
            2: foo_cls("Two"),
            5: foo_cls("Five")
        },
        tbinary=b"\x01\x0fabc123\x00\x02",
        tmap_of_bool2str={True: "true string", False: "false string"},
        tmap_of_bool2int={True: 0, False: 1},
        tbin2bin={b'Binary': b'data'},
        tset_of_binary={b'bin one', b'bin two'},
        tlist_of_binary=[b'foo roo', b'baz boo'],
    )


tp2_object = make_data(tp2_Bar, tp2_Foo)
t_object = make_data(Bar, Foo)


class Handler:
    @staticmethod
    def test(t):
        return t


protocols = [
    (TApacheJSONProtocolFactory, T_TJSONProtocolFactory),
    (TBinaryProtocolFactory, T_TBinaryProtocolFactory),
    (TCyBinaryProtocolFactory, T_TBinaryProtocolFactory),
    (TCompactProtocolFactory, T_TCompactProtocolFactory),
]


def thrift_server(**kwargs):
    handler = Handler()
    processor = BarService.Processor(handler)
    transport = TSocket.TServerSocket(host='localhost', socket_family=socket.AF_INET)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = kwargs['th_prot']()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print("Starting thrift server with", kwargs['th_prot'])
    server.serve()


def thrift_client(**kwargs):
    transport = TSocket.TSocket('localhost', socket_family=socket.AF_INET)
    transport = TTransport.TBufferedTransport(transport)

    protocol = kwargs['th_prot']().getProtocol(transport)
    client = BarService.Client(protocol)
    transport.open()

    result = client.test(t_object)
    transport.close()
    print("\n", result)
    assert recursive_vars(result) == recursive_vars(t_object)
    assert result.tbinary is not None
    assert str(result.tbool) == "False"


def thriftpy2_server(**kwargs):
    from thriftpy2.rpc import make_server
    trans_factory = TBufferedTransportFactory if\
        kwargs['tp2_prot'] == TBinaryProtocolFactory else TCyBufferedTransportFactory
    server = make_server(
        service=test_thrift.BarService,
        handler=Handler(),
        host='localhost',
        proto_factory=kwargs['tp2_prot'](),
        trans_factory=trans_factory()
    )
    print("Starting thriftpy2 server with", kwargs['tp2_prot'])
    server.serve()


def thriftpy2_client(**kwargs):
    from thriftpy2.rpc import make_client
    trans_factory = TBufferedTransportFactory if \
        kwargs['tp2_prot'] == TBinaryProtocolFactory else TCyBufferedTransportFactory
    client = make_client(
        test_thrift.BarService,
        'localhost',
        proto_factory=kwargs['tp2_prot'](),
        trans_factory=trans_factory()
    )
    result = client.test(tp2_object)
    print("\n", result)
    assert recursive_vars(result) == recursive_vars(tp2_object)
    assert result.tbinary is not None
    assert str(result.tbool) == "False"


@pytest.mark.parametrize('protos', protocols)
@pytest.mark.parametrize('server_fn', [thrift_server, thriftpy2_server])
@pytest.mark.parametrize('client_fn', [thrift_client, thriftpy2_client])
def test_client_server(protos, server_fn, client_fn):
    kw = {
        'tp2_prot': protos[0],
        'th_prot': protos[1],
    }
    proc = Process(target=server_fn, kwargs=kw)
    proc.start()
    time.sleep(0.1)
    try:
        client_fn(**kw)
    finally:
        proc.kill()
    # sometimes a test fails because the proc
    # of a previous test takes a while to die
    time.sleep(0.05)


if __name__ == "__main__":
    import sys
    tp2_prot, th_prot = protocols[0]
    {
        'tp2': lambda: thriftpy2_server(tp2_prot=tp2_prot),
        'th': lambda: thrift_server(th_prot=th_prot),
        'tp2_c': lambda: thriftpy2_client(tp2_prot=tp2_prot),
        'th_c': lambda: thrift_client(th_prot=th_prot)
    }.get(sys.argv[1], lambda: print("Invalid arg"))()
