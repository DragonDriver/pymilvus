import abc
import threading
import logging
import random
import functools
import time

import grpc
from grpc._cython import cygrpc

from ..grpc_gen import master_service_discovery_pb2_grpc
from ..grpc_gen import master_service_discovery_pb2

from .grpc_handler import set_uri, error_handler
from .types import Status
from .pool import ConnectionRecord
from .exceptions import (
    NotConnectError,
)

LOGGER = logging.getLogger(__name__)


class GrpcServiceHandler:
    def __init__(self, host=None, port=None, pre_ping=True, **kwargs):
        self._channel = None
        self._stub = None
        self._uri = None
        self.status = None
        self._connected = False
        self._pre_ping = pre_ping
        # if self._pre_ping:
        self._max_retry = kwargs.get("max_retry", 5)

        # record
        self._id = kwargs.get("conn_id", 0)

        # condition
        self._condition = threading.Condition()
        self._request_id = 0

        # set server uri if object is initialized with parameter
        _uri = kwargs.get("uri", None)
        self._setup(host, port, _uri, pre_ping)

    def __str__(self):
        attr_list = ['%s=%r' % (key, value)
                     for key, value in self.__dict__.items() if not key.startswith('_')]
        return attr_list

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def _setup(self, host, port, uri, pre_ping=False):
        """
        Create a grpc channel and a stub

        :raises: NotConnectError

        """
        raise NotImplementedError()

    def _pre_request(self):
        if self._pre_ping:
            self.ping()

    def _get_request_id(self):
        with self._condition:
            _id = self._request_id
            self._request_id += 1
            return _id

    def ping(self):
        begin_timeout = 1
        timeout = begin_timeout
        ft = grpc.channel_ready_future(self._channel)
        retry = self._max_retry
        try:
            while retry > 0:
                try:
                    ft.result(timeout=timeout)
                    return True
                except:
                    retry -= 1
                    LOGGER.debug("Retry connect addr <{}> {} times".format(self._uri, self._max_retry - retry))
                    if retry > 0:
                        timeout *= 2
                        continue
                    else:
                        LOGGER.error("Retry to connect server {} failed.".format(self._uri))
                        raise
        except grpc.FutureTimeoutError:
            raise NotConnectError('Fail connecting to server on {}. Timeout'.format(self._uri))
        except grpc.RpcError as e:
            raise NotConnectError("Connect error: <{}>".format(e))
        except Exception as e:
            raise NotConnectError("Error occurred when trying to connect server:\n"
                                  "\t<{}>".format(str(e)))

    @property
    def server_address(self):
        """
        Server network address
        """
        return self._uri


def retry_on_unexpected_error(retry_times=3):
    def wrapper(func):
        @functools.wraps(func)
        def handler(self, *args, **kwargs):
            wait = 1
            counter = 1
            while True:
                try:
                    resp = func(self, *args, **kwargs)
                    if resp.status.error_code == 0:
                        return resp
                    if resp.status.error_code != Status.UNEXPECTED_ERROR:
                        raise BaseException(resp.status.error_code, resp.status.reason)
                    if counter >= retry_times:
                        raise BaseException(resp.status.error_code, resp.status.reason)
                    time.sleep(wait)
                    wait *= 2
                except Exception as e:
                    raise e
                finally:
                    counter += 1

        return handler

    return wrapper


class MasterServiceHandler(GrpcServiceHandler):
    def _setup(self, host, port, uri, pre_ping=False):
        self._uri = set_uri(host, port, uri)
        self._channel = grpc.insecure_channel(
            self._uri,
            options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                     (cygrpc.ChannelArgKey.max_receive_message_length, -1),
                     ('grpc.enable_retries', 1),
                     ('grpc.keepalive_time_ms', 55000)]
        )
        # TODO(dragondriver): use MasterServiceStub
        self._stub = master_service_discovery_pb2_grpc.ServiceDiscoveryProviderStub(self._channel)
        self.status = Status()

    @error_handler(None)
    @retry_on_unexpected_error(retry_times=3)
    def get_info(self, timeout=None):
        req = master_service_discovery_pb2.GetInfoRequest()
        resp = self._stub.GetInfo.future(req, wait_for_ready=True, timeout=timeout)
        return resp.result()


class Connections:
    def __init__(self, master_uri=None, update_policy="lazy"):
        self._master_uri = master_uri
        self._update_policy = update_policy
        self._lock = threading.Lock()
        self._conns = dict()    # endpoint -> conn
        self._master_conn = MasterServiceHandler(uri=master_uri)

        self._master_conn.ping()
        self._update()

    def _update(self):
        nodes = self._master_conn.get_info().nodes
        with self._lock:
            for node in nodes:
                endpoint = f"{node.host}:{node.port}"
                conn = self._conns.get(endpoint, None)
                if conn:
                    try:
                        conn.ping()
                    except:
                        self._conns[endpoint] = ConnectionRecord(uri=f"tcp://{node.host}:{node.port}", handler="GRPC")
                else:
                    self._conns[endpoint] = ConnectionRecord(uri=f"tcp://{node.host}:{node.port}", handler="GRPC")

    def fetch(self):
        updated = False
        while True:
            with self._lock:
                while len(self._conns) > 0:
                    endpoint, conn = random.choice(list(self._conns.items()))
                    try:
                        conn.connection().ping()
                        return conn.connection()
                    except:
                        self._conns.pop(endpoint)
            if updated:
                raise Exception("no available server, please try again later")
            self._update()
            updated = True


# TODO(dragondriver)
class LoadBalancer:
    def __init__(self):
        pass

    @abc.abstractmethod
    def add_servers(self, server_list):
        raise NotImplementedError()

    @abc.abstractmethod
    def choose_server(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def mark_server_down(self, server):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_server_list(self, available=True):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_reachable_servers(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_all_servers(self):
        raise NotImplementedError()
