import json
import websocket
import tempfile

class RPCException(Exception):
  pass

class Pliant(object):
  def __init__(self, port):
    self.call_id = 0
    self.ws = websocket.WebSocket()
    self.ws.connect("ws://127.0.0.1:%s/conn"%port)
    
  def local(self, path):
    return self._call("AtomicClient.GetLocalPath", [path])
  def put(self, local, path):
    return self._call("AtomicClient.PutLocalPath", [{"LocalPath": local, "DestPath": path}])
  def mkdir(self, path):
    return self._call("AtomicClient.MakeDir", [path])
  def listdir(self, path):
    files = self._call("AtomicClient.ListFiles", [path])
    return files
  def stat(self, path):
    return self._call("AtomicClient.Stat", [path])
  def push(self, path, label):
    return self._call("AtomicClient.Push", [{"Source": path, "Tag": label}])
  def pull(self, label, path):
    return self._call("AtomicClient.Pull", [{"Destination": path, "Tag": label}])
  def _call(self, method, params):
    call_id = self.call_id 
    self.call_id += 1
    self.ws.send(json.dumps({"Method":method, "Params":params, "Id":call_id}))
    resp = self.ws.recv()
    #print("resp=%r" % resp)
    resp = json.loads(resp)
    if resp['error'] != None:
      raise RPCException(resp['error'])
    return resp['result']

class TransientFile(object):
  def __init__(self, filename, client):
    self.filename = filename
    self.client = client
    self.file = tempfile.NamedTemporaryFile()
  
  def read(size=-1):
    return self.read(size)
  def write(self, buffer):
    return self.file.write(buffer)
  def seek(self, offset, whence=0):
    return self.file.seek(offset, whence)
  def tell(self):
    return self.file.tell()
  def close(self):
    self.file.flush()
    self.client.put(self.file.name, self.filename)
    self.file.close()

class FS(object):
  def __init__(self, client):
    self.client = client
  def open(self, filename, mode="r"):
    if mode == "w":
      return TransientFile(filename, self.client)
    elif mode == "r":
      local_path = self.client.local(filename)
      return open(local_path, mode="r")
    else:
      raise Exception("invalid mode: %s" % mode)
  def mkdir(self, path):
    self.client.mkdir(path)
  def listdir(self, path):
    return self.client.ls(path)

client = Pliant(7788)
fs = FS(client)
fd = fs.open("foo/sample", "w")
fd.write("hello")
fd.close()
print( client.listdir("foo"))
fd = fs.open("foo/sample", "r")
print("file contents: %r" % fd.read())
