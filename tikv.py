from tikv_client import RawClient

client = RawClient.connect("127.0.0.1:2379")

# put
client.put(b"k1", b"Hello")
client.put(b"k2", b",")
client.put(b"k3", b"World")
client.put(b"k4", b"!")
client.put(b"k5", b"Raw KV")

# get
print(client.get(b"k1"))

# batch get
print(client.batch_get([b"k1", b"k3"]))

# scan
print(client.scan(b"k1", end=b"k5", limit=10, include_start=True, include_end=True))
