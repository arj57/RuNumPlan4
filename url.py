import urllib.parse as urlparse


class Url(object):
    """
    URL structure:
    <scheme>:[//[<login>[:<password>]@]<host>[:<port>]][/<URLpath>][?<parameters>][#<anchor>]
    Used structure:
    <scheme>://<login>:<password>@<host>[:<port>]][/<URLpath>]
    sheme: схема распознается и парсится по двоеточию
    //netloc - сетевое расположение (host, port) распознается и парсится по //
    Example of URL:
    ftp://login[:PASSWORD]@192.168.1.1[:port]/path
    Here path in the URL also may contain glob pattern symbols: '?', '*', '[', ']'
    """
    def __init__(self, url):
        url2 = urlparse.quote(url, safe=':/@!*')
        u = urlparse.urlparse(url2)
        self.scheme = u.scheme
        self.netloc = u.netloc
        self.path = urlparse.unquote(u.path)
        # self.path = (lambda x: r"/" if x == "" else x.lstrip("/"))(urlparse.unquote(u.path))
        self.query = u.query
        self.fragment = u.fragment
        self.username = u.username
        self.password = u.password
        self.hostname = u.hostname
        self.port = u.port
        self.string = urlparse.unquote(url2)

