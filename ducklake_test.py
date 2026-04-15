import duckdb

con = duckdb.connect()
con.execute("INSTALL ducklake;")
con.execute("LOAD ducklake;")

CATALOG = "catalog.ducklake"

con.execute(
    f"ATTACH 'ducklake:{CATALOG}' AS lake ")

con.execute("USE lake;")

students = con.sql("select * from students").fetchall()

print(students)
