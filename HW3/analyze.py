import sys

filename = ""
benchmark = ""

def test_msg(args):
    global filename
    global benchmark

    print "Hello"
    filename = args[0]
    benchmark = args[1]
    print filename
    print benchmark
    
if __name__ == '__main__':
    test_msg(sys.argv[1:])


