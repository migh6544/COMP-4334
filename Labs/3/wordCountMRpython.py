# Pure python wordCount using Python map/reduce
from functools import reduce

def wordcount(filename):

    # Load in all the words into a list from the file
    f = open(filename, "r")
    words = []
    for x in f:
        words.extend(x.rstrip().split(" "))
    print(words)

    # Perform the mapping
    m = map(lambda x: (x, 1), words)

    # Display the map
    print("After Map:")
    # Once you print it destroys the map iterator,
    #  so make a copy before printing
    from itertools import tee
    m, mcopy = tee(m)
    for x in mcopy:
        print(x)

    # Gather elements by key
    groups = {}
    for x in m:
        if x[0] in groups:
            groups[x[0]].append(x)
        else:
            groups[x[0]] = [x]

    print("After group by key")
    print(groups)

    # Reduce each key group
    print("After reduce")
    for g in groups.keys():
        r = reduce(lambda x,y: (x[0], x[1]+y[1]), groups[g])
        print(r) # Display
        
wordcount("wc.txt")