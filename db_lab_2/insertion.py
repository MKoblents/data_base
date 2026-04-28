# pip install psycopg2-binary
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": 5432
}

def generate_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    napr_specs = [(i, f"23010{i}", f"Направление {i}") for i in range(1, 51)]
    execute_values(cur, 'INSERT INTO "Н_НАПР_СПЕЦ" ("ИД", "КОД_НАПРСПЕЦ", "НАИМЕНОВАНИЕ") VALUES %s', napr_specs, page_size=500)

    nap_specials = [(i, i, i) for i in range(1, 51)]
    execute_values(cur, 'INSERT INTO "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ" ("ИД", "НС_ИД", "НАПС_ИД") VALUES %s', nap_specials, page_size=500)

    forms = [(1, "очная", "очную"), (2, "заочная", "заочную")]
    execute_values(cur, 'INSERT INTO "Н_ФОРМЫ_ОБУЧЕНИЯ" ("ИД", "ИМЯ_В_ИМИН_ПАДЕЖЕ", "ИМЯ_В_ВИН_ПАДЕЖЕ") VALUES %s', forms)

    print("Генерация планов")
    plans = []
    for i in range(1, 10001):
        is_match = i <= 500
        date = datetime(2012, 9, 1) if is_match else datetime(2015 + (i % 10), 1, 1)
        course = 1 if is_match else (i % 4) + 1
        fo_id = 1 if is_match else 2
        naps_id = 1 if is_match else ((i - 1) % 49) + 2
        plans.append((i, 1, naps_id, course, fo_id, date))
    execute_values(cur, 'INSERT INTO "Н_ПЛАНЫ" ("ИД", "ТПЛ_ИД", "НАПС_ИД", "КУРС", "ФО_ИД", "ДАТА_УТВЕРЖДЕНИЯ") VALUES %s', plans, page_size=10000)

    print("Генерация групп")
    groups_plans = []
    for i in range(1, 10000):
        group_name = f"{i:04d}"
        if i <= 1000:
            plan_id = ((i - 1) % 500) + 1
        else:
            plan_id = 501 + ((i - 1001) % 9499)
        groups_plans.append((group_name, plan_id))
    execute_values(cur, 'INSERT INTO "Н_ГРУППЫ_ПЛАНОВ" ("ГРУППА", "ПЛАН_ИД") VALUES %s', groups_plans, page_size=10000)

    print("Генерация людей")
    people = [(i, f"Фамилия_{i}", f"Имя_{i}", f"Отчество_{i}") for i in range(1, 500001)]
    execute_values(cur, 'INSERT INTO "Н_ЛЮДИ" ("ИД", "ФАМИЛИЯ", "ИМЯ", "ОТЧЕСТВО") VALUES %s', people, page_size=50000)

    print("Генерация учеников")
    students = []
    for i in range(1, 500001):
        if i <= 25000:
            group_idx = ((i - 1) % 1000) + 1 
        else:
            group_idx = 1001 + ((i - 25001) % 8999)
        group_name = f"{group_idx:04d}"
        students.append((i, i, "обучен", "утвержден", group_name, i))
    execute_values(cur, 'INSERT INTO "Н_УЧЕНИКИ" ("ИД", "ЧЛВК_ИД", "ПРИЗНАК", "СОСТОЯНИЕ", "ГРУППА", "П_ПРКОК_ИД") VALUES %s', students, page_size=50000)

    conn.commit()
    
    
    print("Обновление статистики (VACUUM ANALYZE)...")
    conn.autocommit = True
    cur.execute("VACUUM ANALYZE")
    conn.autocommit = False

    with conn.cursor() as vcur:
        vcur.execute('SELECT COUNT(*) FROM "Н_УЧЕНИКИ"')
        total = vcur.fetchone()[0]
        vcur.execute("""
            SELECT COUNT(*) FROM "Н_УЧЕНИКИ" у
            JOIN "Н_ГРУППЫ_ПЛАНОВ" гп ON у."ГРУППА" = гп."ГРУППА"
            JOIN "Н_ПЛАНЫ" п ON гп."ПЛАН_ИД" = п."ИД"
            JOIN "Н_ФОРМЫ_ОБУЧЕНИЯ" фо ON п."ФО_ИД" = фо."ИД"
            JOIN "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ" ннс ON п."НАПС_ИД" = ннс."НАПС_ИД"
            JOIN "Н_НАПР_СПЕЦ" нсп ON ннс."НС_ИД" = нсп."ИД"
            WHERE п."ДАТА_УТВЕРЖДЕНИЯ" = DATE '2012-09-01'
              AND п."КУРС" = 1
              AND LOWER(фо."ИМЯ_В_ВИН_ПАДЕЖЕ") LIKE 'очн%'
              AND нсп."КОД_НАПРСПЕЦ" = '230101'
        """)
        matched = vcur.fetchone()[0]
    cur.close()
    conn.close()

if __name__ == "__main__":
    generate_data()