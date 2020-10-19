import logging
import os
import socket
import time
from multiprocessing import Process

import pytest
import thriftpy2
from thrift.protocol import TBinaryProtocol as T_TBinary_Protocol, TCompactProtocol as T_TCompactProtocol, \
    TJSONProtocol as T_TJSONProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
from thriftpy2.protocol import TBinaryProtocolFactory, TApacheJSONProtocolFactory, TCompactProtocolFactory

from spec import BarService
from spec.ttypes import Foo, Bar

logging.basicConfig(level=logging.DEBUG)

test_thrift = thriftpy2.load(os.path.dirname(os.path.realpath(__file__)) + "/spec.thrift")
tp2_Bar = test_thrift.Bar
tp2_Foo = test_thrift.Foo


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
        tmap_of_bool2int={True: 0, False: 1}
    )


tp2_object = make_data(tp2_Bar, tp2_Foo)
t_object = make_data(Bar, Foo)


class Handler:
    @staticmethod
    def test(t):
        return t


protocols = [
    (TApacheJSONProtocolFactory, T_TJSONProtocol.TJSONProtocolFactory),
    (TBinaryProtocolFactory, T_TBinary_Protocol.TBinaryProtocolFactory),
    (TCompactProtocolFactory, T_TCompactProtocol.TCompactProtocolFactory),
]


def thrift_server(**kwargs):
    handler = Handler()
    processor = BarService.Processor(handler)
    transport = TSocket.TServerSocket(socket_family=socket.AF_INET)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = kwargs['th_prot']()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()


def thrift_client(**kwargs):
    transport = TSocket.TSocket(socket_family=socket.AF_INET)
    transport = TTransport.TBufferedTransport(transport)

    protocol = kwargs['th_prot']().getProtocol(transport)
    client = BarService.Client(protocol)
    transport.open()

    result = client.test(t_object)
    transport.close()
    assert result.tbinary == t_object.tbinary


def thriftpy2_server(**kwargs):
    from thriftpy2.rpc import make_server
    server = make_server(
        service=test_thrift.BarService,
        handler=Handler(),
        proto_factory=kwargs['tp2_prot']()
    )
    server.serve()


def thriftpy2_client(**kwargs):
    from thriftpy2.rpc import make_client
    client = make_client(test_thrift.BarService, proto_factory=kwargs['tp2_prot']())
    result = client.test(tp2_object)
    assert result.tbinary == tp2_object.tbinary


@pytest.mark.parametrize('protos', protocols)
@pytest.mark.parametrize('server_fn', [thrift_server, thriftpy2_server])
@pytest.mark.parametrize('client_fn', [thrift_client, thriftpy2_client])
def test_client_server(protos, server_fn, client_fn):
    kw = {
        'tp2_prot': protos[0],
        'th_prot': protos[1]
    }
    proc = Process(target=server_fn, kwargs=kw)
    proc.start()
    time.sleep(0.1)
    try:
        client_fn(**kw)
    finally:
        pass
        proc.kill()
