import pandas as pd
import numpy as np


def get_user_vector(all_genres_ratings, user_genres_ratings):
    user_vector = []
    i = 0
    for e in user_genres_ratings:
        if e == 0:
            user_vector.append(0)
        else:
            user_vector.append(all_genres_ratings[i] - e)
        i += 1
    return np.asarray(user_vector)


def pivot_create(df_genres):
    df_genres["dummy"] = int(1)
    df_genres_piv = df_genres.pivot_table(index="movieID", columns="genre", values="dummy")
    df_genres_names_map = {}
    genres_column_names = []
    for df_genres_piv_col_names in df_genres_piv.columns:
        new_genre = df_genres_piv_col_names
        df_genres_names_map[df_genres_piv_col_names] = new_genre
        genres_column_names.append(new_genre)
    df_genres_piv = df_genres_piv.rename(columns=df_genres_names_map)
    return df_genres_piv, genres_column_names


class ratings_ETL:
    data = pd.DataFrame()
    joined = pd.DataFrame()

    def __init__(self, genres_df_name, ratedmovies_df_name, row_limiter):
        self.df_genres = pd.read_table(genres_df_name, delimiter='\t', skiprows=0)
        self.df_ratedmovies = pd.read_table(ratedmovies_df_name, delimiter='\t', skiprows=0, nrows=row_limiter)
        self.genre_list = []

    def join_genres_ratedmovies(self):
        df_genres, genres_list = pivot_create(self.df_genres)
        df_ratedmovies = self.df_ratedmovies[["movieID", "userID", "rating"]]
        output_df = df_ratedmovies.set_index("movieID").join(df_genres)
        output_df = output_df.fillna(0)
        self.data = output_df.reset_index()
        self.data.rename(columns={'Film-Noir': 'Film_Noir', 'Sci-Fi': 'Sci_Fi'}, inplace=True)
        self.data.columns = map(str.lower, self.data.columns)
        genres_list = map(str.lower, genres_list)
        self.genre_list = sorted(genres_list)
        self.joined = self.data
        return self.data, sorted(genres_list)

    def average_categories_ratings(self):
        average_ratings = []
        for e in self.genre_list:
            try:
                average_ratings.append(self.data.groupby(e)['rating'].mean()[1])
            except:
                average_ratings.append(0)
        return np.asarray(average_ratings)

    def average_user_categories_ratings(self, user):
        df = self.data.loc[self.data['userid'] == user]
        average_ratings = []
        for e in self.genre_list:
            try:
                average_ratings.append(df.groupby(e)['rating'].mean()[1])
            except:
                average_ratings.append(0)
        return np.asarray(average_ratings)

    def usr_vec(self, userID):
        genres_rating = self.average_categories_ratings()
        user_rating = self.average_user_categories_ratings(userID)
        return get_user_vector(genres_rating, user_rating)


def main():
    etl = ratings_ETL('movie_genres.dat', 'user_ratedmovies.dat', 200)
    joined, genres_list = etl.join_genres_ratedmovies()
    print(joined)
    print(genres_list)
    genres_rating = etl.average_categories_ratings()
    print("All genres ratings")
    print(genres_rating)
    user_id = 75
    user_rating = etl.average_user_categories_ratings(user_id)
    print("User", user_id, " genres ratings")
    print(user_rating)
    print("User ", user_id, " vector")
    print(etl.usr_vec(user_id))


if __name__ == "__main__":
    main()
