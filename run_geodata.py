from http.server import ThreadingHTTPServer
import os
from geodata import GeoHandler
from jetstream_observability import start_jetstream_metrics

# API replicas hydrate read state only. Durable import work is dispatched and
# recovered by the separately scalable JetStream worker Deployment.
GeoHandler.store.wait_for_authority_schema()
GeoHandler.store.hydrate()
start_jetstream_metrics()
ThreadingHTTPServer(
    ("0.0.0.0", int(os.environ.get("GEODATA_HTTP_PORT", "8003"))), GeoHandler
).serve_forever()
