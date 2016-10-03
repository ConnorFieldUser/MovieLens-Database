import psycopg2

import csv

connection = psycopg2.connect("dbname=movielens user=movielens")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS item_table;")

create_table_command = """
CREATE TABLE item_table (
  MOVIE_ID serial PRIMARY KEY NOT NULL,
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

CREATE UNIQUE INDEX item_table_MOVIE_ID_id_unidex ON public.item_table (MOVIE_ID);
"""

cursor.execute(create_table_command)

# delimiter='|'
with open('item.csv', encoding='latin1') as item:
    item = list(csv.DictReader(item, delimiter='|', fieldnames=["MOVIE_ID", "TITLE", "RELEASE_DATE", "VID_RELEASE",
                "IMDB_URL", "UNKNOWN", "ACTION", "ADVENTURE", "ANIMATION", "CHILDRENS", "COMEDY", "CRIME",
                                                                "DOCUMENTARY", "DRAMA", "FANTASY", "FILM_NOIR",
                                                                "HORROR", "MUSICAL", "MYSTERY", "ROMANCE",
                                                                "SCI_FI", "THRILLER", "WAR", "WESTERN"]))
    for row in item:
        cursor.execute("""INSERT INTO item_table VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                          %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (row["MOVIE_ID"], row["TITLE"], row["RELEASE_DATE"], row["VID_RELEASE"], row["IMDB_URL"],
                        row["UNKNOWN"], row["ACTION"], row["ADVENTURE"], row["ANIMATION"], row["CHILDRENS"],
                        row["COMEDY"], row["CRIME"], row["DOCUMENTARY"], row["DRAMA"],  row["FANTASY"],
                        row["FILM_NOIR"], row["HORROR"], row["MUSICAL"], row["MYSTERY"], row["ROMANCE"], row["SCI_FI"],
                        row["THRILLER"], row["WAR"], row["WESTERN"]))
connection.commit()

# USER_TABLE START HERE

cursor.execute("DROP TABLE IF EXISTS user_table;")

create_table_command = """
CREATE TABLE public.user_table (
  USER_ID serial PRIMARY KEY NOT NULL,
  AGE NUMERIC(3),
  GENDER VARCHAR(1),
  OCCUPATION VARCHAR (20),
  ZIP_CODE VARCHAR(6)
);
CREATE UNIQUE INDEX user_table_USER_ID_id_unidex ON public.user_table (USER_ID);
"""

cursor.execute(create_table_command)

# delimiter='|'
with open('user.csv', encoding='latin1') as user:
    user = list(csv.DictReader(user, delimiter='|', fieldnames=["USER_ID", "AGE", "GENDER", "OCCUPATION", "ZIP_CODE"]))
    for row in user:
        cursor.execute("INSERT INTO user_table VALUES (%s, %s, %s, %s, %s)",
                       (row["USER_ID"], row["AGE"], row["GENDER"], row["OCCUPATION"], row["ZIP_CODE"]))
connection.commit()

# DATA_TABLE START HERE

cursor.execute("DROP TABLE IF EXISTS data_table;")

create_table_command = """
CREATE TABLE public.data_table (
  USER_ID INT,
  ITEM_ID INT,
  RATING VARCHAR(1),
  TIMESTAMP NUMERIC(9)
  CONSTRAINT data_table_item_table_MOVIE_ID_fk FOREIGN KEY (ITEM_ID) REFERENCES item_table (MOVIE ID),
  CONSTRAINT data_table_user_table_USER_ID_fk FOREIGN KEY (USER_ID) REFERENCES item_table (USER_ID)
);
"""

cursor.execute(create_table_command)

with open('data.csv', encoding='latin1') as data:
    data = list(csv.DictReader(data, delimiter='\t', fieldnames=["USER_ID", "ITEM_ID", "RATING", "TIMESTAMP"]))
    for row in data:
        cursor.execute("INSERT INTO data_table VALUES (%s, %s, %s, %s)",
                       (row["USER_ID"], row["ITEM_ID"], row["RATING"], row["TIMESTAMP"]))
connection.commit()

cursor.close()
connection.close()
