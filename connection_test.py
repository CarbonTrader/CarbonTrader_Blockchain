import sys
from Node import *

node = Node([])
if(len(sys.argv) > 2 ):
    print("Please enter just one argument.")
if(sys.argv[1] == '1'):
    node.request()
elif(sys.argv[1] == '2'):
    print("Reply")
    node.reply()