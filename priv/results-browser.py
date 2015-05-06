 #!/usr/bin/env python

import SimpleHTTPServer
import SocketServer
import logging
import cgi
import base64
import argparse
import os

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
      logging.warning("======= GET STARTED =======")
      logging.warning(self.headers)
      SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
      logging.warning("======= POST STARTED =======")
      length = self.headers['content-length']
      data = self.rfile.read(int(length))

      with open(os.path.join("." , "summary.png"), 'w') as fh:
        fh.write(base64.b64decode(data.decode()))

      self.send_response(200)


def startServer(host, port):
  httpd = SocketServer.TCPServer((host, port), ServerHandler)
  print 'Serving at: http://{host}:{port}'.format(host=host, port=port)
  httpd.serve_forever()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Results generator')
  parser.add_argument('--port', '-p', type=int, help='Port for results generator to bind to', default=8080, required=False)
  parser.add_argument('--host', type=str, help='Host for results generator to bind to', default='localhost', required=False)
  args = parser.parse_args()
  startServer(args.host, args.port)