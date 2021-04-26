import hashlib
import uuid
#
# from BTrees.OIBTree import OIBTree
#
# newTree = OIBTree()
#
# newTree.insert("the", "hello")

# -------------------------------------
tuple = ("0001", "Brandon", 25)

hash = hashlib.md5()

for element in tuple:
    hash.update(str(element).encode())

# print(type(hash.digest().decode("utf-8")))

data_key = str(uuid.UUID(bytes=hash.digest()))
print(data_key)
# data_key = uuid.UUID(bytes=hash.digest())
data_key = hash.hexdigest()

print(data_key)
# -------------------------------------
#
# import shelve
#
# d = shelve.open("temp.db")
#
# # d["key"] = "hello"
#
# print(d["key"])
#
#
# from blist import sorteddict
#
# a = sorteddict()
#
# a[data_key] = "hello"
# a["key"] = "hello"
# a["akey"] = "hello"
#
# try:
#     for key in a.items():
#         print(key)
# except:
#     pass