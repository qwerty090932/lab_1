import sqlite3
import pandas as pd
con = sqlite3.connect("library.sqlite")
f_damp = open('library.db','r', encoding = 'utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)
con.commit()
cursor = con.cursor()
df1 = pd.read_sql('''
    SELECT 
        title AS Название,
        publisher_name AS Издательство,
        genre_name AS Жанр,
        year_publication AS Год_издания
        FROM book
        JOIN genre USING (genre_id)
        JOIN publisher USING (publisher_id)
        WHERE title NOT LIKE "% %" AND year_publication < :p_year
        ORDER BY year_publication ASC, title ASC
    ''', con, params={"p_year": 2020}
)
# print(df1)
df2 = pd.read_sql('''
    SELECT 
        SUBSTRING(title, 1, 1) AS Буква,
        title AS Книги
        FROM book GROUP BY title
''', con)
# print(df2)
df3 = pd.read_sql('''
    WITH max_count_genres AS (SELECT genre_id, max(count_genres) 
                                FROM (SELECT genre_id, count(*) AS count_genres 
                                        FROM book 
                                        GROUP BY genre_id))
    SELECT genre_name, title, count(*) AS 'Доступное кол-во'
    FROM book, max_count_genres
    JOIN genre USING (genre_id)
    GROUP BY title
    HAVING book.genre_id = max_count_genres.genre_id
''', con)
# print(df3)
df4 = pd.read_sql('''
    WITH scores_table AS (
    SELECT reader_name, iif(JULIANDAY(return_date) - JULIANDAY(borrow_date) <= 14, '5', iif(JULIANDAY(return_date) - JULIANDAY(borrow_date)>14, iif(JULIANDAY(return_date) - JULIANDAY(borrow_date) <= 30, '3', iif(JULIANDAY(return_date) - JULIANDAY(borrow_date)>30, '2', '1')),'1')) 
                            AS scores 
    FROM reader 
    JOIN book_reader USING (reader_id))
    SELECT reader_name, SUM(scores) AS rating FROM scores_table
    GROUP BY reader_name
    
''', con)
# print(df4)
df5 = pd.read_sql('''
    WITH table_max_num AS(
        SELECT max(available_numbers) AS avail_max, publisher_id
        FROM book
        GROUP BY publisher_id
        HAVING available_numbers > 0
    )
    SELECT publisher_name, title, available_numbers
    FROM Table_max_num, book INNER JOIN publisher USING (publisher_id)
    WHERE available_numbers = avail_max and table_max_num.publisher_id = book.publisher_id
    ORDER BY publisher_name, available_numbers desc, title
''', con)
# print(df5)

