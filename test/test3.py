

#
# from BTrees.OIBTree import OIBTree
#
# newTree = OIBTree()
#
# newTree.insert("the", "hello")


import shelve

d = shelve.open("temp.db")

# d["key"] = "hello"

print(d["key"])


from blist import sorteddict

a = sorteddict()

a["bcool"] = "hello"
a["key"] = "hello"
a["akey"] = "hello"

try:
    for key in a.items():
        print(key)
except:
    pass