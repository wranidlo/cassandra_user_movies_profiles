import pandas as pd
import cassandra_client as cass
import ETL as etl
import json


class api_logic:

    def __init__(self):
        self.etl = etl.ratings_ETL('/data/movie_genres.dat',
                                   '/data/user_ratedmovies.dat', 0)
        self.etl.join_genres_ratedmovies()
        self.cass_data = cass.cassandra_use()
        cass.create_table_ratings(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        cass.create_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_profile)
        cass.create_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg)
        cass.create_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_all)
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        self.etl.data = self.etl.joined
        for row in rows:
            print(row)
            self.etl.data = self.etl.data.append(pd.DataFrame(row, index=[id]))

        print("Loaded data")
        print(self.etl.data)

    def add_rating(self, item):
        self.etl.data = self.etl.data.append(pd.json_normalize(item), ignore_index=True)
        cass.push_data_table_ratings(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating,
                                     item)
        print(self.etl.data)

    def get_ratings(self):
        self.etl.data = self.etl.joined
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        for row in rows:
            print(row)
            self.etl.data = self.etl.data.append(pd.DataFrame(row, index=[id]))
        return self.etl.data.to_dict("record")

    def delete_ratings(self):
        self.etl.data = self.etl.data.iloc[0:0]
        cass.clear_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        return self.etl.data.to_dict("record")

    def all_ratings(self):
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        self.etl.data = self.etl.joined
        for row in rows:
            self.etl.data = self.etl.data.append(pd.DataFrame(row, index=[id]))
        genres_rating = self.etl.average_categories_ratings()
        cass.push_data_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg,
                                 0, genres_rating.tolist())
        print("All genres ratings\n", genres_rating.tolist())
        return genres_rating.tolist()

    def get_profile(self, user_id):
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        self.etl.data = self.etl.joined
        for row in rows:
            self.etl.data = self.etl.data.append(pd.DataFrame(row, index=[id]))

        print("User ", user_id, " vector")
        usr_vec = self.etl.usr_vec(user_id)
        print(usr_vec)

        cass.push_data_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg,
                                 user_id, usr_vec)
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg)
        for row in rows:
            print(row)
        return usr_vec.tolist()

    def get_one_user_average(self, user_id):
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_rating)
        self.etl.data = self.etl.joined
        for row in rows:
            self.etl.data = self.etl.data.append(pd.DataFrame(row, index=[id]))

        print("User ", user_id, " average")
        usr_avg = self.etl.average_user_categories_ratings(user_id)
        print(usr_avg)

        cass.push_data_table_avg(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg,
                                 user_id, usr_avg)
        rows = cass.get_data_table(self.cass_data.session, self.cass_data.key_space, self.cass_data.table_avg)
        for row in rows:
            print(row)
        return usr_avg.tolist()
