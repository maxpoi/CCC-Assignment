import json
import math
import re
from collections import defaultdict
import sys
import time

from mpi4py import MPI

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
        word_score.append([phrase.lower(), int(score)])

        max_words = max(max_words, len(phrase.split()))

word_score.sort(key=lambda tup: tup[0])

for pair in word_score:
    word, score = pair
    AFINN[word[0]].append(pair)
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


def get_grid(x, y, grid=melbGrid):
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



def extract_field(line):
    """
    line: input string.

    return: 2 strings, both are enclosed by "{}"
            1st string is the location data in json format, e.g., {"location": [xx, yy]}
            2nd string is the tweet data in json format, e.g., {"text": "sdsd"}
    """

    # remove \n
    line = line.strip()

    # ------- find location first -------
    start = line.find("\"coordinates\":")
    if start == -1:
        return None, None

    end = line.find("]},", start)
    location = "{" + line[start:end+2]

    start = line.find("\"text\":", end)
    end = line.find("\", \"location\":", start)
    if end == -1:
        end = line.find("\",\"location\":", start)
    tweet = "{" + line[start:end+1] + "}"

    return location, tweet

def preprocess_word(word):
    punctuations = set(["!", ",", "?", ".", "'", '"', "‘", "’", "“", "”"])

    ret = ""

    start = 0
    if word[0] == '"' or word[0] == '"':
        start = 1
    for i in range(start, len(word)):
        ch = word[i:i+1]
        if ch in punctuations:
            if ch != "'" and ch != "’" and ch != "‘":
                ret += " "
            else:
                if i-3 > -1 and i+1 < len(word):
                    # only keep ',
                    # if ' is part of can't
                    if "can't" == word[i-3:i+2]:
                        # and can't is the last word, or
                        # the next character is in punctuations
                        if i+2 == len(word) or i+2 < len(word) and word[i+2:i+3] in punctuations:
                            ret += "'"
                        else:
                            ret += " "
                    else:
                        ret += " "
                else:
                    ret += " "

                # if i+1 == len(word):
                #     ret += " "
                # elif i+2 == len(word):
                #     if word[i+1:i+2].isalpha():
                #         ret += "'"
                #     else:
                #         ret += " "
                # else:
                #     ret += " "
        else:
            ret += ch
    # print(word, ret)
    return ret


def preprocess_tweet(scores, location, tweet):
    debug = []

    x, y = json.loads(location)["coordinates"]
    loc = get_grid(x, y)

    tweet = json.loads(tweet)["text"]
    pre_words = tweet.lower().split()

    # preprocessing words
    # remove " ' at the front
    # replace " ' , . with ' '
    words = []
    for word in pre_words:
        word = preprocess_word(word)
        new_list = word.split(" ")
        for new_word in new_list:
            if len(new_word) > 1:
                words.append(new_word)

    len_words = len(words)

    # take each index of words as the starting point
    # and use max_words to create a slicing window
    # start, end, last_end are array indexes.
    start = 0
    end = max_words
    while start < len_words:

        # skip obvious mismatches
        leading_char = words[start][0]
        if leading_char not in AFINN:
            start += 1
            end = start + max_words-1

            continue

        # indicates whether there is a match with current window size
        found = False

        while end > start:
            # skip obvious problems
            while end > len_words:
                end -= 1

            word = " ".join(words[start:end])

            # loop through AFINN to find a match
            for target, score in AFINN.get(leading_char):
                if target == word:
                    # debug.append(word)

                    scores[loc] += score
                    found = True
                    break

                if word < target:
                    break

            if found:
                break
            else:
                end -= 1

        # deal with situations like 
        # AFINN: word one; one two
        # words: word one two
        # match both
        # However, note:
        # if words are: cool nice; and AFINN are: cool nice, nice
        # then it matches both as well.
        start += 1
        end = start + max_words

    return scores, debug

def get_score_individual(filename, rank, num_cores):
    '''
    filename: must be a json file
    '''
    debug = []

    scores = defaultdict(int)
    # for grid_info in melbGrid:
    #     scores[grid_info['properties']['id']] = 0
    # ------- finish initializing ------- #

    counter = 0
    with open(filename, "r") as f:
        f.readline()
        for line in f:
            if counter % num_cores == rank:
                loc, twt = extract_field(line)
                if loc != None and twt != None:
                    # for each instance of tweeter's data (tweet)
                    # get its text & location
                    # compare each word in the text with AFINN
                    # as AFINN is sorted, when word < target_word, stop searching
                    # otherwise, keep searching until reaches the end of AFINN
                    scores, debug_inner = preprocess_tweet(scores, loc, twt)

                    debug.append(str(debug_inner))

            counter += 1
    f.close()

    return scores, debug   


def collect_scores(world):
    size = world.Get_size()

    score_list = []

    # receive and send must in the same order
    # the tag information is not that useful in our context

    # except master, everyelse is sending data to master
    # so master (rank_0) needs to send request to all of them
    for i in range(size-1):
        world.send("r", dest = (i+1), tag = (i+1))
    
    # then receive the returned data
    for i in range(size-1):
        score_list.append(world.recv(source=(i+1), tag=0))

    return score_list


def worker(world, filename):
    rank = world.Get_rank()
    size = world.Get_size()

    scores, debug = get_score_individual(filename, rank, size)

    # wait until data is send
    while True:
        in_message = world.recv(source=0, tag=rank)
        if isinstance(in_message, str):
            if in_message == "r":
                world.send(scores, dest=0, tag=0)
            elif in_message == "e":
                exit(0)


def master(world, filename):
    rank = world.Get_rank()
    size = world.Get_size()

    scores, debug = get_score_individual(filename, rank, size)
    if size > 1:
        score_list = collect_scores(world)
        for score in score_list:
            for k, v in score.items():
                scores[k] = scores[k] + v

        # close communication
        for i in range(size-1):
            world.send("e", dest=(i+1), tag=(i+1))

    return scores




if __name__ == "__main__":

    # ---------------- create MPI world ----------------
    world = MPI.COMM_WORLD
    rank = world.Get_rank()

    if rank == 0:
        start_time = time.time()
        print("------------      log       -------------")

    # --------------- read argv -------------------
    try:
        filename = str(sys.argv[1])
    except:
        print("Please provide a json file...")
        sys.exit(1)

    # if (len(sys.argv) > 2):
    #     n_nodes = int(sys.argv[2])
    #     n_cores = int(sys.argv[3])
    # else:
    #     n_nodes = 1
    #     n_cores = 1

    print("name: {}, rank: {}, size: {}".format(filename, rank, world.Get_size()))

    # initiate via rank
    if rank == 0:
        scores = master(world, filename)
    else:
        worker(world, filename)
    
    if rank == 0:
        print(scores)
        print("running time: %s " % (time.time() - start_time))
        print("------------ finish logging -------------")

    # with open("debug-" + filename[:-5] + ".txt", "w") as f:
    #     for instance in debug:
    #         f.write(instance + "\n")
