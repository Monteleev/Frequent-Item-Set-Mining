import pandas as pd
import numpy as np

def ReadRatings():
    my_ratings_df = pd.read_csv('ratings.csv')
    return (my_ratings_df)

def ReadMovies():
    my_movies_df = pd.read_csv('movies.csv')
    return (my_movies_df)

def CreateUserBasket():
    my_ratings_df = ReadRatings()
    my_userBaskets = my_ratings_df.groupby('userId').apply(lambda x: [dict(zip(x.movieId, x.rating))]).to_dict()
    my_userBaskets_df = pd.DataFrame(my_userBaskets)
    my_userBaskets_df.to_csv('my_userBaskets.csv')







#ReadMovies()
#ReadRatings()
CreateUserBasket()