import SimpleHTTPServer
import SocketServer
import logging
import cgi
import base64

import sys

if len(sys.argv) > 2:
    PORT = int(sys.argv[2])
    I = sys.argv[1]
elif len(sys.argv) > 1:
    PORT = int(sys.argv[1])
    I = ""
else:
    PORT = 8000
    I = ""

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
      logging.warning("======= GET STARTED =======")
      logging.warning(self.headers)
      SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
      logging.warning("======= POST STARTED =======")
      length = self.headers['content-length']
      data = self.rfile.read(int(length))

      with open("./" + self.path, 'w') as fh:
        fh.write(base64.b64decode(data.decode()))

      self.send_response(200)

Handler = ServerHandler

httpd = SocketServer.TCPServer(("", PORT), Handler)

print "Serving at: http://%(interface)s:%(port)s" % dict(interface=I or "localhost", port=PORT)
httpd.serve_forever()