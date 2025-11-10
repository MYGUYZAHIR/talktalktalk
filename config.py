PORT = 12000                   # if you change this line, change the port as well in .htaccess
import os

# Bind host/port
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "9000"))

ADMINNAME = 'admin'            # this username will be available if *and only if* the following username is entered in the input field:
ADMINHIDDENNAME = 'adminxyz'
ALLOWEDTAGS = []               # tags allowed in messages, could be ['a', 'b', 'em', 'code'], etc.
