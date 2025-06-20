# Pure python wordCount using dictionary

def wordcount(filename):

    # Load in all the words into a list from the file
    f = open(filename, "r")
    words = []
    for x in f:
        words.extend(x.rstrip().split(" "))
    print(words)

    # Build the histogram
    hist = {}   # Dictionary
    for x in words:
        if x in hist:
            hist[x] = hist[x] + 1
        else:
            hist[x] = 1  # We have seen 1 of the new word

    print(hist)

wordcount("wc.txt")
