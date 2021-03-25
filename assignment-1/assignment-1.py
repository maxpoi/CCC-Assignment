import json
import math
from collections import defaultdict

AFINN = defaultdict(list)
word_score = []

# use this to record what is max. num of words exist in a phrase
# when later ietrating the twitter words, this is the max. threshold
# of creating sub phrases. Because if "not good" matches, "good" should
# be not counted. So we generate a sub phrases of size 2 first, and decrementin$
max_words = 0
with open("AFINN.txt", "r") as f:
    for line in f:
        phrase, score = line.strip().rsplit("\t", 1)
        words = phrase.lower().split()
        word_score.append([words, int(score)])

        max_words = max(max_words, len(words))

word_score.sort(key=lambda tup: tup[0])

for pair in word_score:
    words, score = pair
    AFINN[words[0][0]].append(pair)

# --------- finish reading AFINN --------- #

with open("melbGrid.json", "r") as f:
    data = f.read()
    data = data.replace("'", '"')
    data = data.replace("‘", '"')
    data = data.replace("’", '"')
    data = data.replace("“", '"')
    data = data.replace("”", '"')
melbGrid = json.loads(data)['features']
f.close()

# --------- finish reading melbGrid --------- #

def getGrid(x, y, grid=melbGrid):
    '''
    x, y, grid: float

    return:
        The grid num it is in: String
    '''
    x = float(x)
    y = float(y)
    for grid_info in grid:
        _id = grid_info['properties']['id']
        xmin = float(grid_info['properties']['xmin'])
        xmax = float(grid_info['properties']['xmax'])
        ymin = float(grid_info['properties']['ymin'])
        ymax = float(grid_info['properties']['ymax'])
        if (xmin < x and x <= xmax and ymin < y and y <= ymax):
            return _id

def getScore(filename):
    '''
    filename: must be a json file
    '''
    debug = []

    # valid chars which can appear at the end of words,
    punctuations = set(["!", ",", "?", ".", "'", '"', "‘", "’", "“", "”"])

    with open(filename, "r") as f:
        data = f.read()
    tweets = json.loads(data)
    f.close()

    scores = {}
    for grid_info in melbGrid:
        scores[grid_info['properties']['id']] = 0

    # ------- finish initializing ------- #

    # for each instance of tweeter's data (tweet)
    # get its text & location
    # compare each word in the text with AFINN
    # as AFINN is sorted, when word < target_word, stop searching
    # otherwise, keep searching until reaches the end of AFINN
    for row in range(len(tweets['rows'])):
        tweet = tweets['rows'][row]

        debug_inner = []

        x, y = tweet['value']['geometry']['coordinates']
        words = tweet['value']['properties']['text'].lower().split()
        loc = getGrid(x, y)

        len_words = len(words)

        # take each index of words as the starting point
        # and use max_words to create a slicing window
        # all three vars are array indexes.
        start = 0
        end = max_words-1
        last_end = -1
        while start < len_words:

            # skip obvious mismatches
            leading_char = words[start][0]
            if leading_char not in AFINN:
                last_end = start
                start += 1
                end = start + max_words-1

                continue

            # indicates whether there is a match with current window size
            found = False

            while end > last_end:
                # skip obvious problems
                while end + 1 > len_words:
                    end -= 1

                word = " ".join(words[start:end+1])

                # loop through AFINN to find a match
                for targets, score in AFINN.get(leading_char):
                    target = " ".join(targets)

                     # get the extra substring at the back of the word
                    # use set to examine if the substring only contains
                    # valid punctuations, if not, stop searching immediately.
                    _len = len(target)
                    front = word[:_len]
                    back = set(word[_len:])

                    if (back.issubset(punctuations)):
                        if front == target:
                            debug_inner.append(word)
                            scores[loc] += score
                            found = True
                            break

                    if front < target:
                        break

                if found:
                    break
                else:
                    end -= 1

            if end == last_end:
                    last_end = start
            else:
                last_end = end

            start += 1
            end = start + max_words - 1

        debug.append(str(debug_inner))

    return scores, debug   

import sys
import time

if __name__ == "__main__":
    start_time = time.time()

    filename = str(sys.argv[1])
    print(filename)

    score, debug = getScore(filename)
    print(score) 
    print("running time: %s " % (time.time() - start_time))

    with open("debug-" + filename, "w") as f:
        for instance in debug:
            f.write(instance + "\n")