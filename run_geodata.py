from http.server import ThreadingHTTPServer
from geodata import GeoHandler, seed

seed()
ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()

