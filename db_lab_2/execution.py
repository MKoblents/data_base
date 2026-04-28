import psycopg2
import statistics

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": 5432
}

QUERY_EXISTS = """
SELECT у."ГРУППА" AS "Номер группы", у."ИД" AS "Номер студента", л."ФАМИЛИЯ" AS "Фамилия", л."ИМЯ" AS "Имя", л."ОТЧЕСТВО" AS "Отчество", у."П_ПРКОК_ИД" AS "Номер пункта приказа"
FROM "Н_ЛЮДИ" л JOIN "Н_УЧЕНИКИ" у ON л."ИД" = у."ЧЛВК_ИД"
WHERE EXISTS (
    SELECT 1 FROM "Н_ГРУППЫ_ПЛАНОВ" гп JOIN "Н_ПЛАНЫ" п ON гп."ПЛАН_ИД" = п."ИД"
    JOIN "Н_ФОРМЫ_ОБУЧЕНИЯ" фо ON п."ФО_ИД" = фо."ИД"
    join "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ" ON п."НАПС_ИД" = "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ"."НАПС_ИД"
    JOIN "Н_НАПР_СПЕЦ" ON "Н_НАПР_СПЕЦ"."ИД" = "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ"."НС_ИД"
    WHERE гп."ГРУППА" = у."ГРУППА" AND п."ДАТА_УТВЕРЖДЕНИЯ" = DATE '2012-09-01'
    AND п."КУРС" = 1 AND LOWER(фо."ИМЯ_В_ВИН_ПАДЕЖЕ") LIKE 'очн%' AND "КОД_НАПРСПЕЦ" = '230101');
"""

QUERY_JOIN = """
SELECT у."ГРУППА" AS "Номер группы", у."ИД" AS "Номер студента", л."ФАМИЛИЯ" AS "Фамилия", л."ИМЯ" AS "Имя", л."ОТЧЕСТВО" AS "Отчество", у."П_ПРКОК_ИД" AS "Номер пункта приказа"
FROM "Н_ЛЮДИ" л JOIN "Н_УЧЕНИКИ" у ON л."ИД" = у."ЧЛВК_ИД"
join "Н_ГРУППЫ_ПЛАНОВ" гп on у."ГРУППА" = гп."ГРУППА"
JOIN "Н_ПЛАНЫ" п ON гп."ПЛАН_ИД" = п."ИД"
JOIN "Н_ФОРМЫ_ОБУЧЕНИЯ" фо ON п."ФО_ИД" = фо."ИД"
join "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ" ON п."НАПС_ИД" = "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ"."НАПС_ИД"
JOIN "Н_НАПР_СПЕЦ" ON "Н_НАПР_СПЕЦ"."ИД" = "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ"."НС_ИД"
WHERE п."ДАТА_УТВЕРЖДЕНИЯ" = DATE '2012-09-01' AND п."КУРС" = 1
AND LOWER(фо."ИМЯ_В_ВИН_ПАДЕЖЕ") LIKE 'очн%' AND "КОД_НАПРСПЕЦ" = '230101'
"""

def prepare_session(conn):
    with conn.cursor() as cur:
        cur.execute("RESET ALL")
        cur.execute("SET work_mem = '64MB'")
        cur.execute("SET random_page_cost = 1.1")
        cur.execute("SET effective_cache_size = '2GB'")
        cur.execute("""
            ANALYZE "Н_ЛЮДИ", "Н_УЧЕНИКИ", "Н_ГРУППЫ_ПЛАНОВ", 
                    "Н_ПЛАНЫ", "Н_ФОРМЫ_ОБУЧЕНИЯ", 
                    "Н_НАПРАВЛЕНИЯ_СПЕЦИАЛ", "Н_НАПР_СПЕЦ"
        """)
        conn.commit()

def run_benchmark(query, iterations=20, warmup=3):
    conn = psycopg2.connect(**DB_CONFIG)
    prepare_session(conn)
    collected = []

    print(f"  Running: {warmup} warmup + {iterations} measurement iterations")
    for i in range(warmup + iterations):
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON) {query}")
            plan = cur.fetchone()[0]

        root = plan[0]
        node = root.get("Plan", {})

        metrics = {
            'execution_time_ms': float(root.get('Execution Time', 0)),
            'planning_time_ms': float(root.get('Planning Time', 0)),
            'actual_rows': int(node.get('Actual Rows', 0)),
            'loops': int(node.get('Actual Loops', 1)),
            'buf_hit': int(node.get('Shared Hit Blocks', 0)),
            'buf_read': int(node.get('Shared Read Blocks', 0)),
            'buf_dirtied': int(node.get('Shared Dirtied Blocks', 0)),
            'buf_written': int(node.get('Shared Written Blocks', 0)),
            'buf_temp_read': int(node.get('Temp Read Blocks', 0)),
            'buf_temp_written': int(node.get('Temp Written Blocks', 0))
        }

        if i >= warmup:
            collected.append(metrics)
            print(f"  Iteration {i - warmup + 1}/{iterations}: Exec={metrics['execution_time_ms']:.2f}ms")

    conn.close()
    return collected

def compute_statistics(collected):
    if not collected: return {}
    stats = {}
    for key in collected[0]:
        values = [item[key] for item in collected]
        stats[key] = {
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else 0.0,
            'min': min(values),
            'max': max(values)
        }
    return stats

def main():
    print("="*130)
    print("BENCHMARK: EXISTS vs JOIN (PostgreSQL) — FULL METRICS")
    print("="*130)
    
    print("\n[1] Executing EXISTS query...")
    exists_data = run_benchmark(QUERY_EXISTS)
    stats_exists = compute_statistics(exists_data)

    print("\n[2] Executing JOIN query...")
    join_data = run_benchmark(QUERY_JOIN)
    stats_join = compute_statistics(join_data)

    print("\n" + "="*130)
    print("AGGREGATED RESULTS (Mean & Median in separate columns)")
    print("="*130)
    
    metrics_order = [
        'execution_time_ms', 'planning_time_ms', 'actual_rows', 'loops',
        'buf_hit', 'buf_read', 'buf_dirtied', 'buf_written',
        'buf_temp_read', 'buf_temp_written'
    ]
    
    header = f"{'Metric':<25} | {'EXISTS (Mean)':>14} | {'EXISTS (Median)':>14} | {'JOIN (Mean)':>14} | {'JOIN (Median)':>14} | {'Δ (%)':>8}"
    print(header)
    print("-" * 130)
    
    for key in metrics_order:
        e = stats_exists.get(key, {})
        j = stats_join.get(key, {})
        
        e_mean = e.get('mean', 0)
        e_med = e.get('median', 0)
        j_mean = j.get('mean', 0)
        j_med = j.get('median', 0)
        
        if key in ('execution_time_ms', 'planning_time_ms') and j_mean != 0:
            delta = (e_mean - j_mean) / j_mean * 100
            delta_str = f"{delta:+.1f}%"
        else:
            delta_str = "—"
        
        def fmt(v):
            return f"{v:,.0f}" if isinstance(v, (int, float)) and abs(v) >= 1000 else f"{v:.2f}"
            
        print(f"{key:<25} | {fmt(e_mean):>14} | {fmt(e_med):>14} | {fmt(j_mean):>14} | {fmt(j_med):>14} | {delta_str:>8}")
    
    print("="*130)

if __name__ == "__main__":
    main()