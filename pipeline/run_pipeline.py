import subprocess
import sys
import time
from datetime import datetime

def ejecutar(script, nombre):
    print(f"\n{'='*50}")
    print(f"▶ {nombre}")
    print('='*50)
    inicio = time.time()
    resultado = subprocess.run(
        [sys.executable, script],
        capture_output=False
    )
    fin = time.time()
    if resultado.returncode == 0:
        print(f"✅ {nombre} completado en {fin-inicio:.2f}s")
    else:
        print(f"❌ Error en {nombre}")
        sys.exit(1)
    return fin - inicio

if __name__ == "__main__":
    print("\n" + "="*50)
    print("PIPELINE MEDALLION — INDUMAQ S.R.L.")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    inicio_total = time.time()

    t1 = ejecutar(
        r"E:\OneDrive\Escritorio\IDL3_BIG DATA\medallion_indumaq\bronze\scripts\01_bronze_ingest.py",
        "BRONZE — Ingesta (simula Apache NiFi → HDFS)"
    )
    t2 = ejecutar(
        r"E:\OneDrive\Escritorio\IDL3_BIG DATA\medallion_indumaq\silver\scripts\02_silver_etl.py",
        "SILVER — ETL (simula Apache Spark)"
    )
    t3 = ejecutar(
        r"E:\OneDrive\Escritorio\IDL3_BIG DATA\medallion_indumaq\gold\scripts\03_gold_aggregations.py",
        "GOLD — Agregaciones (simula Apache Hive)"
    )

    total = time.time() - inicio_total

    print("\n" + "="*50)
    print("✅ PIPELINE MEDALLION COMPLETADO")
    print(f"   Bronze : {t1:.2f}s")
    print(f"   Silver : {t2:.2f}s")
    print(f"   Gold   : {t3:.2f}s")
    print(f"   Total  : {total:.2f}s")
    print("="*50)