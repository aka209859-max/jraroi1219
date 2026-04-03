import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=5432, database='pckeiba', user='postgres', password='postgres123')
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='jrd_kyi' ORDER BY ordinal_position")
for r in cur.fetchall():
    print(r[0])
conn.close()
