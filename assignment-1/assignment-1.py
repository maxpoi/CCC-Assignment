import json
import math
import re
from collections import defaultdict
import sys
import time

from mpi4py import MPI

outside = "Outside"

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


# -------------------------------------------- #
#              utility functions               #
# -------------------------------------------- #

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
    return None




def extract_field(line):
    '''
        line: input string.

        return: 2 strings, both are enclosed by "{}"
                1st string is the location data in json format, e.g., {"location": [xx, yy]}
                2nd string is the tweet data in json format, e.g., {"text": "sdsd"}
    '''

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





# -------------------------------------------- #
#                main functions                #
# -------------------------------------------- #
def preprocess_tweet(scores, loc, tweet):
    '''
        given a tweet,
        calculate its score and update to the 'scores'
        with key=loc
    '''

    if loc == None:
        loc = outside

    debug = []

    tweet = json.loads(tweet)["text"]

    # use re.split to split string by different eliminators. 
    # include http & https to remove hyperlinks
    # include filter to remove empty strings
    words = list(filter(None, re.split(r'(?:[!.?,\'\"\‘\’\“\”\s]|http://\S+|https://\S+)', tweet.lower())))
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
            end = start + max_words

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

                    scores[loc][1] += score
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




# -------------------------------------------- #
#                 mpi functions                #
# -------------------------------------------- #
def get_score_individual(filename, scores, rank, num_cores):
    '''
        filename: must be a json file
    '''
    debug = []

    counter = 0
    with open(filename, "r") as f:
        f.readline()
        for line in f:
            if counter % num_cores == rank:
                loc, twt = extract_field(line)
                if loc != None and twt != None:

                    x, y = json.loads(loc)["coordinates"]
                    loc = get_grid(x, y)

                    if loc == None:
                        scores[outside][0] += 1
                    else:
                        scores[loc][0] += 1

                    # for each instance of tweeter's data (tweet)
                    # get its text & location
                    # compare each word in the text with AFINN
                    # as AFINN is sorted, when word < target_word, stop searching
                    # otherwise, keep searching until reaches the end of AFINN
                    scores, debug_inner = preprocess_tweet(scores, loc, twt)

                    # if (len(debug_inner) > 0):
                    #     for w in debug_inner:
                    #         debug.append(w)
                            # debug.append(str(debug_inner))

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

    scores = {}
    for grid_info in melbGrid:
        scores[grid_info['properties']['id']] = [0, 0]
    scores[outside] = [0, 0]

    scores, debug = get_score_individual(filename, scores, rank, size)

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

    scores = {}
    for grid_info in melbGrid:
        scores[grid_info['properties']['id']] = [0, 0]
    scores[outside] = [0, 0]

    scores, debug = get_score_individual(filename, scores, rank, size)
    if size > 1:
        score_list = collect_scores(world)
        for score in score_list:
            for k, v in score.items():
                scores[k][0] = scores[k][0] + v[0]
                scores[k][1] = scores[k][1] + v[1]

        # close communication
        for i in range(size-1):
            world.send("e", dest=(i+1), tag=(i+1))

    return scores, debug




if __name__ == "__main__":

    # ---------------- create MPI world ----------------
    world = MPI.COMM_WORLD
    rank = world.Get_rank()


    # --------------- read argv -------------------
    try:
        filename = str(sys.argv[1])
    except:
        print("Please provide a json file...")
        sys.exit(1)

    # initiate via rank
    if rank == 0:
        scores, debug = master(world, filename)
    else:
        worker(world, filename)
    
    if rank == 0:
        print("{:<10}    {:<10}    {:<10}".format("Location", "#Tweet", "Score"))
        for key, value in scores.items():
            print("{:<10}    {:<10}    {:<10}".format(key, value[0], value[1]))

    # with open("debug-" + filename[:-5] + ".txt", "w") as f:
    #     for instance in debug:
    #         f.write(instance + "\n")
