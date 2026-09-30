from http.server import ThreadingHTTPServer
from geodata import GeoHandler

# Recover queued and interrupted imports only after durable state and the
# durable state has been hydrated. Importing geodata as a Python
# module must not unexpectedly start background jobs (it is also used by
# tests and by the NATS promotion worker).
GeoHandler.store.hydrate()
GeoHandler.recover_import_runs()
ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
