from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def create_keyspace(my_session, keyspace):
    my_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table_avg(my_session, keyspace, table):
    my_session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
    userID int ,
    Action float, Adventure float, Animation float, Children float, Comedy float,
    Crime float, Documentary float, Drama float, Fantasy float, Film_Noir float,
    Horror float, IMAX float, Musical float, Mystery float, Romance float, Sci_Fi float,
    Short float, Thriller float, War float, Western float,
    PRIMARY KEY(userID)
    )   
    """)


def create_table_ratings(my_session, keyspace, table):
    my_session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
    userID int, movieID int, rating float,
    Action float, Adventure float, Animation float, Children float, Comedy float,
    Crime float, Documentary float, Drama float, Fantasy float, Film_Noir float,
    Horror float, IMAX float, Musical float, Mystery float, Romance float, Sci_Fi float,
    Short float, Thriller float, War float, Western float,
    PRIMARY KEY(userID, movieID)
    )
    """)


def push_data_table_ratings(my_session, keyspace, table, data):
    my_session.execute(
        """
    INSERT INTO """ + keyspace + """.""" + table + """ (userID, movieID, rating,
    Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy, Film_Noir,
    Horror, IMAX, Musical, Mystery, Romance, Sci_Fi, Short, Thriller, War, Western)
    VALUES (%(userID)s, %(movieID)s, %(rating)s, %(action)s, %(adventure)s,
    %(animation)s, %(children)s, %(comedy)s, %(crime)s, %(documentary)s, 
    %(drama)s, %(fantasy)s, %(film_noir)s, %(horror)s, %(imax)s, 
    %(musical)s, %(mystery)s, %(romance)s, %(sci_fi)s, %(short)s, 
    %(thriller)s, %(war)s, %(western)s)
    """,
        {
            'userID': data['userID'],
            'movieID': data['movieID'],
            'rating': data['rating'],
            'action': data['Action'],
            'adventure': data['Adventure'],
            'animation': data['Animation'],
            'children': data['Children'],
            'comedy': data['Comedy'],
            'crime': data['Crime'],
            'documentary': data['Documentary'],
            'drama': data['Drama'],
            'fantasy': data['Fantasy'],
            'film_noir': data['Film_Noir'],
            'horror': data['Horror'],
            'imax': data['IMAX'],
            'musical': data['Musical'],
            'mystery': data['Mystery'],
            'romance': data['Romance'],
            'sci_fi': data['Sci_Fi'],
            'short': data['Short'],
            'thriller': data['Thriller'],
            'war': data['War'],
            'western': data['Western']
        }
    )


def push_data_table_avg(my_session, keyspace, table, user_id, data):
    my_session.execute(
        """
    INSERT INTO """ + keyspace + """.""" + table + """ (userID,
    Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy, Film_Noir,
    Horror, IMAX, Musical, Mystery, Romance, Sci_Fi, Short, Thriller, War, Western)
    VALUES (%(userID)s, %(action)s, %(adventure)s,
    %(animation)s, %(children)s, %(comedy)s, %(crime)s, %(documentary)s, 
    %(drama)s, %(fantasy)s, %(film_noir)s, %(horror)s, %(imax)s, 
    %(musical)s, %(mystery)s, %(romance)s, %(sci_fi)s, %(short)s, 
    %(thriller)s, %(war)s, %(western)s)
    """,
        {
            'userID': user_id,
            'action': data[0],
            'adventure': data[1],
            'animation': data[2],
            'children': data[3],
            'comedy': data[4],
            'crime': data[5],
            'documentary': data[6],
            'drama': data[7],
            'fantasy': data[8],
            'film_noir': data[9],
            'horror': data[10],
            'imax': data[11],
            'musical': data[12],
            'mystery': data[13],
            'romance': data[14],
            'sci_fi': data[15],
            'short': data[16],
            'thriller': data[17],
            'war': data[18],
            'western': data[19]
        }
    )


def get_data_table(my_session, keyspace, table):
    rows = my_session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    return rows


def get_avg_all_users(my_session, keyspace, table):
    rows = my_session.execute("SELECT AVG(Action), AVG(Adventure), AVG(Animation),"
                              "AVG(Children), AVG(Comedy), AVG(Crime), AVG(Documentary), AVG(Drama), "
                              "AVG(Fantasy), AVG(Film_Noir), AVG(Horror), AVG(IMAX), AVG(Musical), "
                              "AVG(Mystery), AVG(Romance), AVG(Sci_Fi), AVG(Short), AVG(Thriller), "
                              "AVG(War), AVG(Western) FROM " + keyspace + "." + table + ";")
    return rows


def clear_table(my_session, keyspace, table):
    my_session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(my_session, keyspace, table):
    my_session.execute("DROP TABLE " + keyspace + "." + table + ";")


class cassandra_use:

    def __init__(self):
        self.key_space = "user_ratings"
        self.table_profile = "user_profile"
        self.table_avg = "user_avg_rating"
        self.table_all = "all_average"
        self.table_rating = "user_ratings"
        cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = cluster.connect()
        create_keyspace(self.session, self.key_space)
        self.session.set_keyspace(self.key_space)
        self.session.row_factory = dict_factory
