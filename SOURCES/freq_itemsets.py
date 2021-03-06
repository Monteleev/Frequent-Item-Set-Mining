import pandas as pd
import sys
from collections import defaultdict
from itertools import islice
import matplotlib.pyplot as plt
import time


def ReadRatings(file):
    my_ratings_df = pd.read_csv(file)
    return (my_ratings_df)


def ReadMovies(file):
    my_movies_df = pd.read_csv(file)
    return (my_movies_df)


def CreateUserBasket(file):
    my_ratings_df = ReadRatings(file)
    my_userBaskets = dict((k, frozenset(v.values))
                          for k, v in my_ratings_df.groupby("userId")["movieId"])
    my_userBaskets_df = pd.DataFrame.from_dict(
        my_userBaskets, orient='index').transpose()
    my_userBaskets_df.to_csv('my_userBaskets.csv')
    return my_userBaskets


def CreateMovieBasket(file):
    my_ratings_df = ReadRatings(file)
    my_movieBaskets = dict((k, frozenset(v.values))
                           for k, v in my_ratings_df.groupby("movieId")["userId"])
    my_movieBaskets_df = pd.DataFrame.from_dict(
        my_movieBaskets, orient='index').transpose()
    my_movieBaskets_df.to_csv('my_movieBaskets.csv')
    return my_movieBaskets


def find_frequent_itemsets(itemBasket, k_1_itemsets, minSupport):
    counts = defaultdict(int)
    for user, reviews in itemBasket.items():
        for itemset in k_1_itemsets:
            if itemset.issubset(reviews):
                for other_movie in reviews - itemset:
                    current_superset = itemset | frozenset((other_movie,))
                    counts[current_superset] += 1
    return dict([(itemset, frequency) for itemset, frequency in counts.items() if frequency >= minSupport])


def getList(dict):

    return [*dict]


def getSet(set):
    l = []
    for x in set:
        lists = list(x)
        l.append(lists)
    return l


def ExactCounting(itemBasket, file, item, maxLength=5):
    frequent_itemsets = {}
    if(item == 1):
        frequent_itemsets[1] = countMovies(file, 1)
    elif(item == 0):
        frequent_itemsets[1] = countUsers(file, 1)

    print("There are {} items with more than {} support".format(
        len(frequent_itemsets[1]), 1))
    sys.stdout.flush()
    itemsets = []
    for k in range(2, maxLength+1):
        cur_frequent_itemsets = find_frequent_itemsets(
            itemBasket, frequent_itemsets[k-1], 1)
        if len(cur_frequent_itemsets) == 0:
            print("Did not find any frequent itemsets of length {}".format(k))
            sys.stdout.flush()
            break
        else:
            print("I found {} frequent itemsets of length {}".format(
                len(cur_frequent_itemsets), k))
            sys.stdout.flush()
            frequent_itemsets[k] = cur_frequent_itemsets

    return(frequent_itemsets)


def myApriori(itemBasket, minSupport, file, item, maxLength=5):
    frequent_itemsets = {}
    if(item == 1):
        frequent_itemsets[1] = countMovies(file, minSupport)
    elif(item == 0):
        frequent_itemsets[1] = countUsers(file, minSupport)

    print("There are {} items with more than {} support".format(
        len(frequent_itemsets[1]), minSupport))
    itemsets = []
    for k in range(2, maxLength+1):
        cur_frequent_itemsets = find_frequent_itemsets(
            itemBasket, frequent_itemsets[k-1], minSupport)
        if len(cur_frequent_itemsets) == 0:
            break
        else:
            print("------")
            sets = getList(cur_frequent_itemsets)
            lists = getSet(sets)
            itemsets.append(lists)
            frequent_itemsets[k] = cur_frequent_itemsets
    freq_set = getList(frequent_itemsets[1])
    freq_list = getSet(freq_set)
    frequentItemsets = [freq_list]+itemsets
    return(frequentItemsets)


def get_movie_name(movie_id, df):
    title_object = df.loc[df['movieId'] == int(movie_id)]
    return title_object


def get_user_name(user_id, df):
    title_object = df.loc[df['userId'] == int(user_id)]
    return title_object


def chunks(data, SIZE=100):
   it = iter(data)
   for i in range(0, len(data), SIZE):
       yield {k: data[k] for k in islice(it, SIZE)}


def SON(itemBasket, minSupport, file, item, maxLength=5, chunksize=100):
    frequent_itemsets = {}
    if(item == 1):
        frequent_itemsets[1] = countMovies(file, minSupport)
    elif(item == 0):
        frequent_itemsets[1] = countUsers(file, minSupport)

    items = []
    k_items = []
    for item in chunks(frequent_itemsets[1], chunksize):
        items.append(item)
    k_items.append(items)
    cur_frequent_itemsets = []
    chunklist = []
    print("There are {} items with more than {} support".format(
        len(frequent_itemsets[1]), minSupport))
    for i in range(2, maxLength+1):
        for k in range(len(k_items[i-2])):
            cur_frequent_itemsets = find_frequent_itemsets(
                itemBasket, k_items[i-2][k], minSupport)
            chunklist.append(cur_frequent_itemsets)
            if len(cur_frequent_itemsets) == 0:
                break
            else:
                k_items.append(chunklist)
    return(k_items)


def phase2(itemBasket, minSupport, file, item, maxLength=5, chunksize=100):
    basket = ExactCounting(itemBasket, file, item, maxLength)
    candidateSets = SON(itemBasket, minSupport, file,
                        item, maxLength, chunksize)

    for i in range(1, len(basket)+1):
        for j in range(0, len(candidateSets[i-1])):
            if not(all(basket[i].get(key, None) == val for key, val in candidateSets[i-1][j].items())):
                del candidateSets[i-1][j]
            else:
                continue
    print(candidateSets)


def countMovies(file, minSupport):
    all_ratings = ReadRatings(file)
    all_ratings["counter"] = 1
    counter = all_ratings[["movieId", "counter"]].groupby("movieId").sum()
    counter.sort_values(by=["counter"], ascending=False)
    frequent_itemsets = {}
    frequent_itemsets[1] = dict((frozenset((movie_id,)), row["counter"])
                                for movie_id, row in counter.iterrows()
                                if row["counter"] > minSupport)

    return frequent_itemsets[1]


def countUsers(file, minSupport):
    all_ratings = ReadRatings(file)
    all_ratings["counter"] = 1
    counter = all_ratings[["userId", "counter"]].groupby("userId").sum()
    counter.sort_values(by=["counter"], ascending=False)
    frequent_itemsets = {}
    frequent_itemsets[1] = dict((frozenset((movie_id,)), row["counter"])
                                for movie_id, row in counter.iterrows()
                                if row["counter"] > minSupport)

    return frequent_itemsets[1]


def presentResults():
    print("============================================================================================")
    print(
        "(C) Create baskets and dataframes from csv file [format: c , <u(sers) , m(ovies)>, <file>]")
    print("--------------------------------------------------------------------------------------------")
    print(
        "(A) List ALL frequent itemsets [format: a , < u(sers) , m(ovies) >]")
    print(
        "(B) List BEST (most frequent) itemset(s) [format: b , < u(sers) , m(ovies) >]")
    print(
        "(M) Show details of a particular MOVIE [format: m , <movieId >]")
    print(
        "(U) Show details of particular USERS [format: u , <userId>]")
    print(
        "(H) Print the HISTOGRAM frequent itemsets [format: h ]")
    print(
        "(O) ORDER frequent itemsets by increasing support [format: o , < u(sers),m(ovies) >]")
    print("--------------------------------------------------------------------------------------------")
    print("(E) EXIT [format: e]")
    print("============================================================================================")

    userBasket = 0
    movieBasket = 0
    minSupport = 15
    file = ''

    while(True):
        inp = input().split(",")
        if inp[0] == 'c':
            file = inp[2]
            if inp[1] == 'u':
                userBasket = CreateUserBasket(file)
            if inp[1] == 'm':
                movieBasket = CreateMovieBasket(file)

        if inp[0] == 'a':
            if inp[1] == 'u':
                print(myApriori(userBasket, 5, file, 0))
                #print(SON(userBasket, 5, file, 0))
            if inp[1] == 'm':
                print(myApriori(movieBasket, 5, file, 1))
                #print(SON(userBasket, 5, file, 1))

        if inp[0] == 'b':
            if inp[1] == 'u':
                print(myApriori(userBasket, 10, file, 0)[-1])
                #print(SON(userBasket, 5, file, 0)[-1])
            if inp[1] == 'm':
                print(myApriori(movieBasket, 10, file, 1)[-1])
                #print(SON(userBasket, 5, file, 1)[-1])
        if inp[0] == 'e':
            exit()
        if inp[0] == 'm':
            x = inp[1]
            df = ReadMovies(file)
            ids = get_movie_name(x, df)
            print(ids)
        if inp[0] == 'u':
            x = inp[1]
            df = ReadRatings(file)
            ids = get_user_name(x, df)
            print(ids)
        if inp[0] == 'h':
            all_ratings = ReadRatings(file)
            all_ratings["counter"] = 1
            counter = all_ratings[["movieId", "counter"]
                                  ].groupby("movieId").sum()
            counter.sort_values(by=["counter"], ascending=True)
            counter['counter'].plot.hist(bins=100)
            plt.title('Histogram of supports for frequent itemsets')
            plt.xlabel('Support')
            plt.ylabel('Number of Frequent Itemsets')
            plt.tight_layout()
            plt.show()
        if inp[0] == 'o':
            if inp[1] == 'u':
                print(ExactCounting(userBasket, file, 0))
            if inp[1] == 'm':
                print(ExactCounting(movieBasket, file, 1))
        print("----------------------------------------------")


presentResults()
