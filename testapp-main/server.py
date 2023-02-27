# Serve app on a local port via waitress
from waitress import serve
import app

serve(app.server, port=80)
