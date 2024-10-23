import io
import urllib.error

import pytest

from kafka_connect_doctor.client import ClientError, UrllibConnectClient, restart_path


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, status: int):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Recorder:
    def __init__(self, body=b"{}", status=200, exc=None):
        self.body, self.status, self.exc = body, status, exc
        self.requests = []

    def __call__(self, req, timeout):
        self.requests.append((req, timeout))
        if self.exc:
            raise self.exc
        return FakeResponse(self.body, self.status)


def test_get_json():
    rec = Recorder(b'{"a": {"status": {}}}')
    c = UrllibConnectClient("http://connect.example.com:8083/", timeout=3, opener=rec)
    assert c.get_json("/connectors?expand=status") == {"a": {"status": {}}}
    req, timeout = rec.requests[0]
    assert req.full_url == "http://connect.example.com:8083/connectors?expand=status"
    assert req.get_method() == "GET" and timeout == 3


def test_post_sends_json_content_type():
    rec = Recorder(b"", 204)
    c = UrllibConnectClient("http://c:8083", opener=rec)
    assert c.post(restart_path("x")) == (204, None)
    req, _ = rec.requests[0]
    assert req.get_method() == "POST"
    assert req.get_header("Content-type") == "application/json"
    assert req.full_url.endswith("/connectors/x/restart?includeTasks=true&onlyFailed=true")


def test_http_error_status_is_returned_for_post_and_raised_for_get():
    err = urllib.error.HTTPError("http://c:8083/x", 409, "Conflict", {}, io.BytesIO(b'{"error_code":409}'))
    c = UrllibConnectClient("http://c:8083", opener=Recorder(exc=err))
    assert c.post("/x") == (409, {"error_code": 409})
    err2 = urllib.error.HTTPError("http://c:8083/x", 404, "Not Found", {}, io.BytesIO(b"nope"))
    with pytest.raises(ClientError, match="HTTP 404"):
        UrllibConnectClient("http://c:8083", opener=Recorder(exc=err2)).get_json("/x")


def test_network_error_wrapped():
    c = UrllibConnectClient("http://c:8083", opener=Recorder(exc=urllib.error.URLError("refused")))
    with pytest.raises(ClientError, match="refused"):
        c.get_json("/connectors")


@pytest.mark.parametrize("url", ["connect:8083", "ftp://c", "http://"])
def test_invalid_url(url):
    with pytest.raises(ClientError):
        UrllibConnectClient(url)
