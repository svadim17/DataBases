import sqlite3


db = sqlite3.connect('example_db.db')           # connect to database
sql = db.cursor()                               # create cursor

# create table
sql.execute("""CREATE TABLE IF NOT EXISTS users (
    login TEXT,
    password TEXT,
    cash INT
)""")
db.commit()     # confirm database

user_login = input('Login: ')
user_password = input('Password: ')

# add values
sql.execute("INSERT INTO users VALUES (?, ?, ?)", (user_login, user_password, 10))
db.commit()     # confirm


# OUTPUT from database
for value in sql.execute("SELECT * FROM users"):
    print(value)
