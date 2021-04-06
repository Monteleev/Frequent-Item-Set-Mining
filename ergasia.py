import pandas as pd
import numpy as np
import sys
from collections import defaultdict
import matplotlib.pyplot as plt

def ReadRatings():
    my_ratings_df = pd.read_csv('ratings_50users.csv')
    return (my_ratings_df)

def ReadMovies():
    my_movies_df = pd.read_csv('movies.csv')
    return (my_movies_df)

def CreateUserBasket():
    my_ratings_df = ReadRatings()
    my_userBaskets = dict((k, frozenset(v.values)) for k, v in my_ratings_df.groupby("userId")["movieId"])
    my_userBaskets_df = pd.DataFrame.from_dict(my_userBaskets,orient='index').transpose()
    my_userBaskets_df.to_csv('my_userBaskets.csv')
    return my_userBaskets

def CreateMovieBasket():
    my_ratings_df = ReadRatings()
    my_movieBaskets = dict((k, frozenset(v.values)) for k, v in my_ratings_df.groupby("movieId")["userId"])
    my_movieBaskets_df = pd.DataFrame.from_dict(my_movieBaskets,orient='index').transpose()
    my_movieBaskets_df.to_csv('my_movieBaskets.csv')
    return my_movieBaskets

def find_frequent_itemsets(itemBasket, k_1_itemsets, minSupport):
    counts = defaultdict(int)
    for user, reviews in itemBasket.items():
        for itemset in k_1_itemsets:
            if itemset.issubset(reviews):
                for other_reviewed_movie in reviews - itemset:
                    current_superset = itemset | frozenset((other_reviewed_movie,))
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

def ExactCounting(itemBasket,minSupport=5,maxLength=15):

    all_ratings = ReadRatings()
    all_ratings["counter"] = 1
    counter = all_ratings[["movieId", "counter"]].groupby("movieId").sum()
    counter.sort_values(by=["counter"], ascending=False)
    frequent_itemsets = {}
    frequent_itemsets[1] = dict((frozenset((movie_id,)), row["counter"])
                                    for movie_id, row in counter.iterrows()
                                    if row["counter"] > minSupport)
    print("There are {} movies with more than {} favorable reviews".format(len(frequent_itemsets[1]), minSupport))
    sys.stdout.flush()
    itemsets=[]
    for k in range(2, maxLength+1):
        cur_frequent_itemsets = find_frequent_itemsets(itemBasket, frequent_itemsets[k-1],minSupport)
        if len(cur_frequent_itemsets) == 0:
            print("Did not find any frequent itemsets of length {}".format(k))
            sys.stdout.flush()
            break
        else:
            print("I found {} frequent itemsets of length {}".format(len(cur_frequent_itemsets), k))
            sys.stdout.flush()
            frequent_itemsets[k] = cur_frequent_itemsets
        
    return(frequent_itemsets)

def myApriori(itemBasket,minSupport,maxLength=5):
    all_ratings = ReadRatings()
    all_ratings["counter"] = 1
    counter = all_ratings[["movieId", "counter"]].groupby("movieId").sum()
    counter.sort_values(by=["counter"], ascending=False)
    frequent_itemsets = {}
    frequent_itemsets[1] = dict((frozenset((movie_id,)), row["counter"])
                                    for movie_id, row in counter.iterrows()
                                    if row["counter"] > minSupport)

    print("There are {} movies with more than {} favorable reviews".format(len(frequent_itemsets[1]), minSupport))
    sys.stdout.flush()
    itemsets=[]
    for k in range(2, maxLength+1):
        cur_frequent_itemsets = find_frequent_itemsets(itemBasket, frequent_itemsets[k-1],minSupport)
        if len(cur_frequent_itemsets) == 0:
            break
        else:
            sets = getList(cur_frequent_itemsets)
            lists = getSet(sets)
            itemsets.append(lists)
            sys.stdout.flush()
            frequent_itemsets[k] = cur_frequent_itemsets
    freq_set = getList(frequent_itemsets[1])
    freq_list = getSet(freq_set)
    freq_sets = [freq_list]+itemsets
    return(freq_sets)

def get_movie_name(movie_id,df):
    title_object = df.loc[df['movieId'] == int(movie_id)]
    return title_object

def get_user_name(user_id,df):
    title_object = df.loc[df['userId'] == int(user_id)]
    return title_object

def SON():
    all_ratings = ReadRatings()
    all_ratings["counter"] = 1
    counter = all_ratings[["movieId", "counter"]].groupby("movieId").sum()
    counter.sort_values(by=["counter"], ascending=False)
    frequent_itemsets = {}
    frequent_itemsets[1] = dict((frozenset((movie_id,)), row["counter"])
                                    for movie_id, row in counter.iterrows()
                                    if row["counter"] > minSupport)

    print("There are {} movies with more than {} favorable reviews".format(len(frequent_itemsets[1]), minSupport))
    sys.stdout.flush()
    itemsets=[]
    for k in range(2, maxLength+1):
        cur_frequent_itemsets = find_frequent_itemsets(itemBasket, frequent_itemsets[k-1],minSupport)
        if len(cur_frequent_itemsets) == 0:
            break
        else:
            sets = getList(cur_frequent_itemsets)
            lists = getSet(sets)
            itemsets.append(lists)
            sys.stdout.flush()
            frequent_itemsets[k] = cur_frequent_itemsets
    freq_set = getList(frequent_itemsets[1])
    freq_list = getSet(freq_set)
    freq_sets = [freq_list]+itemsets
    return(freq_sets)

def presentResults():
    print("============================================================================================")
    print("(C) Create baskets and dataframes from csv file [format: c , <u(sers) , m(ovies)>, <file>]")
    print("(L) Load baskets from csv file [format: l , <u(sers , m(ovies)>, <file>]")
    print("(S) Save baskets to csv file [format: l , <u(sers , m(ovies)>, <file>]")
    print("--------------------------------------------------------------------------------------------")
    print("(A) List ALL frequent itemsets [format: a , < u(sers) , m(ovies) >]")
    print("(B) List BEST (most frequent) itemset(s) [format: b , < u(sers) , m(ovies) >]")
    print("(M) Show details of a particular MOVIE [format: m , < comma-sep. movieIds >]")
    print("(U) Show details of particular USERS [format: u , <comma-sep. userIds>]")
    print("(H) Print the HISTOGRAM frequent itemsets [format: h , < u(sers),m(ovies) >, size]")
    print("(O) ORDER frequent itemsets by increasing support [format: o , < u(sers),m(ovies) >]")
    print("--------------------------------------------------------------------------------------------")
    print("(E) EXIT [format: e]")
    print("============================================================================================")
    
    baskets = CreateUserBasket()
    minSupport = 15

    while(True):
        inp = input().split(",")
        if inp[0] == 'a':
            print(myApriori(baskets,15))
        if inp[0] == 'b':
            print(myApriori(baskets,15)[-1])
        if inp[0] == 'e':
            exit()
        if inp[0] == 'm':
            x=inp[1]
            print(x)
            df = ReadMovies()
            ids = get_movie_name(x,df)
            print(ids)
        if inp[0] == 'u':
            x=inp[1]
            print(x)
            df = ReadRatings()
            ids = get_user_name(x,df)
            print(ids)
        if inp[0] =='h':
            all_ratings = ReadRatings()
            all_ratings["counter"] = 1
            counter = all_ratings[["movieId", "counter"]].groupby("movieId").sum()
            counter.sort_values(by=["counter"], ascending=True)
            counter['counter'].plot.hist(bins=100)
            plt.title('Histogram of supports for frequent itemsets')
            plt.xlabel('Support')
            plt.ylabel('Number of Frequent Itemsets')
            plt.tight_layout()
            plt.show()
        if inp[0] == 'o':
            print(ExactCounting(baskets,10))
        print("----------------------------------------------")
    

xx = CreateUserBasket()
#presentResults()
print(myApriori(xx,4)[0])
