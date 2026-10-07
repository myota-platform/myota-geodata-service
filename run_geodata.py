from http.server import ThreadingHTTPServer
from geodata import GeoHandler
from jetstream_observability import start_jetstream_metrics

# API replicas hydrate read state only. Durable import work is dispatched and
# recovered by the separately scalable JetStream worker Deployment.
GeoHandler.store.hydrate()
start_jetstream_metrics()
ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
