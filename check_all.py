import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=5432, database='pckeiba', user='postgres', password='postgres123')
cur = conn.cursor()
for table in ['jvd_se', 'jvd_ra', 'jrd_cyb', 'jrd_joa', 'jrd_bac']:
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    print(f'=== {table} ({len(cols)} cols) ===')
    print(', '.join(cols))
    print()
conn.close()
