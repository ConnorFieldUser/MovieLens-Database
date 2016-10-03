import psycopg2

import csv

connection = psycopg2.connect("dbname=movielens user=movielens")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS user_table;")

create_table_command = """
CREATE TABLE user_table (
  ID serial PRIMARY KEY,
  TITLE VARCHAR(100),
  RELEASE_DATE VARCHAR(11),
  VID_RELEASE VARCHAR(100),
  IMDB_URL VARCHAR(150),
  UNKNOWN NUMERIC(1),
  ACTION NUMERIC(1),
  ADVENTURE NUMERIC(1),
  ANIMATION NUMERIC(1),
  CHILDRENS NUMERIC(1),
  COMEDY NUMERIC(1),
  CRIME NUMERIC(1),
  DOCUMENTARY NUMERIC(1),
  DRAMA NUMERIC(1),
  FANTASY NUMERIC(1),
  FILM_NOIR NUMERIC(1),
  HORROR NUMERIC(1),
  MUSICAL NUMERIC(1),
  MYSTERY NUMERIC(1),
  ROMANCE NUMERIC(1),
  SCI_FI NUMERIC(1),
  THRILLER NUMERIC(1),
  WAR NUMERIC(1),
  WESTERN NUMERIC(1)
);
"""

cursor.execute(create_table_command)

# delimiter='|'
with open('item.csv', encoding='latin1') as item:
    item = list(csv.DictReader(item, delimiter='|', fieldnames=["ID", "TITLE", "RELEASE_DATE", "VID_RELEASE",
                "IMDB_URL", "UNKNOWN", "ACTION", "ADVENTURE", "ANIMATION", "CHILDRENS", "COMEDY", "CRIME",
                                                                "DOCUMENTARY", "DRAMA", "FANTASY", "FILM_NOIR",
                                                                "HORROR", "MUSICAL", "MYSTERY", "ROMANCE",
                                                                "SCI_FI", "THRILLER", "WAR", "WESTERN"]))
    for row in item:
        cursor.execute("""INSERT INTO user_table VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                          %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (row["ID"], row["TITLE"], row["RELEASE_DATE"], row["VID_RELEASE"], row["IMDB_URL"],
                        row["UNKNOWN"], row["ACTION"], row["ADVENTURE"], row["ANIMATION"], row["CHILDRENS"],
                        row["COMEDY"], row["CRIME"], row["DOCUMENTARY"], row["DRAMA"],  row["FANTASY"],
                        row["FILM_NOIR"], row["HORROR"], row["MUSICAL"], row["MYSTERY"], row["ROMANCE"], row["SCI_FI"],
                        row["THRILLER"], row["WAR"], row["WESTERN"]))
connection.commit()


cursor.close()
connection.close()

# CREATE TABLE public.user_table (
#   id INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('user_table_id_seq'::regclass),
#   age INTEGER,
#   gender CHARACTER VARYING(6),
#   occupation CHARACTER VARYING(40),
#   "zip code" INTEGER
# );
# CREATE UNIQUE INDEX user_table_id_uindex ON user_table USING BTREE (id);
