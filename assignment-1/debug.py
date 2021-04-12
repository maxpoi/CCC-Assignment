def func():

    f = open("debug-smallTwitter.txt", "r")
    g = open("matches.txt", "r")

    i = 0
    while True:
        linef = f.readline().strip()
        lineg = g.readline().strip()
        if (linef != lineg):
            print(i, linef, lineg)
        i += 1

def visisual():
    f = open("debug-smallTwitter.txt", "r")
    g = open("smallTwitter.json", "r")
    g.readline()
    i=0
    while i < 9800:
        words = f.readline().strip()
        line = g.readline()
        if words != "[]":
            print(words)

            s = line.find("text")
            e = line.find("location")
            print(line[s:e])
        i += 1

if __name__ == "__main__":
    func()