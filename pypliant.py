import json
import websocket
import tempfile
import collections
import os

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
    return [x['Name'] for x in self.client.listdir(path)]

FINISHED = "finished"
READY = "ready"
INCOMPLETE = "incomplete"

class Step:
  def __repr__(self):
    return "<inputs=%r, outputs=%r>" % (self.inputs, self.outputs)

  def __init__(self, inputs, outputs, callback):
    self.inputs = inputs
    self.outputs = outputs
    self.callback = callback
    
  def get_state(self):
    #print "get_state"
    has_all_inputs, inputs_min_ts, inputs_max_ts = check_set(self.inputs)
    has_all_outputs, outputs_min_ts, outputs_max_ts = check_set(self.outputs)
    
    #print locals()
    
    if has_all_inputs:
      if has_all_outputs and inputs_max_ts <= outputs_min_ts:
        return FINISHED
      else:
        return READY
    else:
      return INCOMPLETE

  def execute(self):
    self.callback(self.inputs, self.outputs)

def execute(steps):
  while True:
    m = collections.defaultdict(lambda: [])
    for step in steps:
      m[step.get_state()].append(step)

    #print m
    
    if len(m[READY]) == 0:
      break
    
    for step in m[READY]:
      #print "before execute"
      step.execute()
      #print "after execute"
      #print "state=%s (%s)" % (step.get_state(), step)
      import time
#      time.sleep(1)
      if step.get_state() != FINISHED:
        raise Exception("Failed:%s"%step)

  if len(m["INCOMPLETE"]) > 0:
    raise Exception("Could not execute: %s" % m["INCOMPLETE"])

def check_set(files):
  #print "check_set(%s)" % files
  mintimestamp = None
  maxtimestamp = None
  has_all = True
  
  for fn in files:
    if not os.path.exists(fn):
      #print "does not exist: %s" % fn
      has_all = False
    else:
      timestamp = os.stat(fn).st_mtime
      if mintimestamp == None or mintimestamp > timestamp:
        mintimestamp = timestamp
      if maxtimestamp == None or maxtimestamp < timestamp:
        maxtimestamp = timestamp
  
  #print has_all, mintimestamp, maxtimestamp
  return has_all, mintimestamp, maxtimestamp

def mkfn(txt):
  def fn(input, output):
    print txt
    for n in input:
      fd = open(n)
      fd.read()
      fd.close()
    for n in output:
      fd = open(n, "w")
      fd.write("out")
      fd.close()
      print "wrote %s" % n
      assert os.path.exists(n)
  return fn

execute([Step([], ["b"], mkfn("callback1")), Step(["b"], ["c"], mkfn("callback2"))])

####################
#client = Pliant(7788)
#fs = FS(client)
#
#fd = fs.open("foo/sample", "w")
#fd.write("hello")
#fd.close()
#print( fs.listdir("foo"))
#fd = fs.open("foo/sample", "r")
#print("file contents: %r" % fd.read())

