import duckdb

CATALOG = "catalog.ducklake"
DATA_PATH = "lake_data"

def main():
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")

    con.execute(
        f"ATTACH 'ducklake:{CATALOG}' AS lake "
        f"(DATA_PATH '{DATA_PATH}', DATA_INLINING_ROW_LIMIT 0);"
    )
    
    con.execute("USE lake;")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER,
            name VARCHAR,
            cohort VARCHAR,
            score DOUBLE
        );
    """)
    
    con.execute("""
        INSERT INTO students VALUES
            (1, 'Ada Lovelace', 'spring-26', 97.5),
            (2, 'Alan Turing', 'spring-26', 95.0),
            (3, 'Grace Hopper', 'spring-26', 99.1),
            (4, 'Linus Torvalds', 'fall-26', 88.4),
            (5, 'Margaret Hamilton', 'fall-26', 98.7);
    """)
    
    print("\nRows in lake.students:")
    for row in con.execute("SELECT * FROM students ORDER BY id;").fetchall():
        print(" ", row)
    
    print("\nSnapshots:")
    for row in con.execute("""
        SELECT snapshot_id,
               snapshot_time::VARCHAR AS snapshot_time,
               schema_version,
               changes
        FROM ducklake_snapshots('lake');
    """).fetchall():
        print(" ", row)



if __name__ == "__main__":
    main()