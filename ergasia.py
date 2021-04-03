import pandas as pd
import numpy as np
import sys
from collections import defaultdict

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

def ExactCounting():
    my_ratings_df = ReadRatings()
    reviews_by_users = dict((k, frozenset(v.values)) for k, v in my_ratings_df.groupby("userId")["movieId"])
    x = len(reviews_by_users)
    return reviews_by_users[1]

def find_frequent_itemsets(itemBasket, k_1_itemsets, minSupport):
    counts = defaultdict(int)
    for user, reviews in itemBasket.items():
        for itemset in k_1_itemsets:
            if itemset.issubset(reviews):
                for other_reviewed_movie in reviews - itemset:
                    current_superset = itemset | frozenset((other_reviewed_movie,))
                    counts[current_superset] += 1
    return dict([(itemset, frequency) for itemset, frequency in counts.items() if frequency >= minSupport])

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
    for k in range(2, maxLength+1):
        # Generate candidates of length k, using the frequent itemsets of length k-1
        # Only store the frequent itemsets
        cur_frequent_itemsets = find_frequent_itemsets(itemBasket, frequent_itemsets[k-1],minSupport)
        if len(cur_frequent_itemsets) == 0:
            print("Did not find any frequent itemsets of length {}".format(k))
            sys.stdout.flush()
            break
        else:
            print("I found {} frequent itemsets of length {}".format(len(cur_frequent_itemsets), k))
            #print(cur_frequent_itemsets)
            sys.stdout.flush()
            frequent_itemsets[k] = cur_frequent_itemsets
    # We aren't interested in the itemsets of length 1, so remove those
    del frequent_itemsets[1]
xx = CreateUserBasket()
myApriori(xx,14,15)