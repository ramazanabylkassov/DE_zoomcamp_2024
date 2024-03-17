import duckdb
from practice_1 import question_1_2
from practice_2 import question_3
from practice_3 import question_4

question_1_2()
question_3()
question_4()

try:
    conn = duckdb.connect("dlt_practice.duckdb")
    conn.sql(f"SET search_path = 'dlt_practice.dlt'")

    print(conn.sql("SELECT SUM(age) AS question_3 FROM contact_list"))
    print(conn.sql("SELECT SUM(age) AS question_4 FROM contact_list_merged"))
except duckdb.duckdb.CatalogException:
    print("No duckdb database found, run practice_2 & practice_3 python files first")