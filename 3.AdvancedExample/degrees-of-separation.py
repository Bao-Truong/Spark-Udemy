from pyspark import SparkConf, SparkContext
import os

WORKDIR = os.path.dirname(os.getcwd())

# VISITED = 1: WHITE/ NOT VISITED
# VISITED = 2: GRAY/ TO BE VISITED
# VISITED = 3: BLACK/ VISITED

# normally, 2 person will has a low degree depart from each other, change this to change iterate
degreeToCheck = 10
startCharacterID = 5306  # SpiderMan
targetCharacterID = 123  # ADAM 3,031 (who?)


conf = SparkConf().setMaster("local").setAppName("DegreeofSeperation")
sc = SparkContext(conf=conf)
hitCounter = sc.accumulator(0)


def convertToNode(line):
    fs = line.split(" ")
    HeroID = int(fs[0])
    Connections = []
    for item in fs[1:]:
        if(item):
            Connections.append(int(item))
    Distance = 999
    Visited = 1
    if(HeroID == startCharacterID):
        Distance = 0
        Visited = 2

    return (HeroID, (Connections, Distance, Visited))


def init():
    lines = sc.textFile(f"file://{WORKDIR}/datasets/MarvelGraph")
    return lines.map(convertToNode)


def bfsMap(node):
    heroID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    visited = data[2]
    results = []
    if(visited == 2):  # Meet a GRAY node (parent)
        for connID in connections:
            # added the child node to queue, tobe checked next
            childNode = (connID, ([], distance+1, 2))
            if(connID == targetCharacterID):
                print(f"Middle man: {heroID}")
                hitCounter.add(1)
            results.append(childNode)
        visited = 3  # mark current parent to BLACK

    results.append((heroID, (connections, distance, visited)))
    return results


def bfsReduce(data1, data2):
    # return 1 line
    connection, distance, visited = data1[0] + data2[0], min(
        data1[1], data2[1]), max(data1[2], data2[2])
    return (connection, distance, visited)


if __name__ == "__main__":
    bfsNodes = init()  # Convert Text line to BFS Nodes
    print(bfsNodes.count())
    for i in range(degreeToCheck):
        print("Running BFS iteration# " + str(i+1))
        mapped = bfsNodes.flatMap(bfsMap)

        print("Processing " + str(mapped.count()) + " values.")
        if(hitCounter.value > 0):
            print("Hit the target character! From " + str(hitCounter.value)
                  + " different direction(s).")
            break

        bfsNodes = mapped.reduceByKey(bfsReduce)
