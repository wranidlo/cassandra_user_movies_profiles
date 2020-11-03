from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def create_keyspace(my_session, keyspace):
    my_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table(my_session, keyspace, table_name):
    my_session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table_name + """ (
    user_id int ,
    avg_movie_rating float,
    PRIMARY KEY(user_id)
    )
    """)


def push_data_table(my_session, keyspace, table_name, userId, avgMovieRating):
    my_session.execute(
        """
    INSERT INTO """ + keyspace + """.""" + table_name + """ (user_id, avg_movie_rating)
    VALUES (%(user_id)s, %(avg_movie_rating)s)
    """,
        {
            'user_id': userId,
            'avg_movie_rating': avgMovieRating
        }
    )


def get_data_table(my_session, keyspace, table):
    rows = my_session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    for row in rows:
        print(row)


def clear_table(my_session, keyspace, table):
    my_session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(my_session, keyspace, table):
    my_session.execute("DROP TABLE " + keyspace + "." + table + ";")


if __name__ == "__main__":
    key_space = "user_ratings"
    table = "user_avg_rating"
    # utworzenia połączenia z klastrem
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    # utworzenie nowego keyspace
    create_keyspace(session, key_space)
    # ustawienie używanego keyspace w sesji
    session.set_keyspace(key_space)
    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych
    session.row_factory = dict_factory
    # tworzenie tabeli
    create_table(session, key_space, table)
    # umieszczanie danych w tabeli
    push_data_table(session, key_space, table, userId=1337, avgMovieRating=4.2)
    # pobieranie zawartości tabeli i wyświetlanie danych
    get_data_table(session, key_space, table)
    # czyszczenie zawartości tabeli
    clear_table(session, key_space, table)
    get_data_table(session, key_space, table)
    # usuwanie tabeli
    delete_table(session, key_space, table)
