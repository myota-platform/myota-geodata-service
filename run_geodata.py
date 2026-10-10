import os
from common import BoundedThreadingHTTPServer
from geodata import GeoHandler

# API replicas hydrate read state only. Durable import work is dispatched and
# recovered by the separately scalable JetStream worker Deployment.
GeoHandler.store.wait_for_authority_schema()
GeoHandler.store.hydrate()
BoundedThreadingHTTPServer(
    ("0.0.0.0", int(os.environ.get("GEODATA_HTTP_PORT", "8003"))), GeoHandler
).serve_forever()
