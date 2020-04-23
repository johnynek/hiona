import sys

if __name__ == "__main__":

    keys = {0: 'A', 1: 'B', 2: 'C', 3: 'D'}

    print('"key","ts"')
    for i in range(int(sys.argv[1])):
       key = keys[i % 4]
       print("%s,%i" % (key, i))
