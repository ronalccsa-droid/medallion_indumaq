import pandas as pd
import numpy as np
import os
from datetime import datetime

# Ruta donde se guardan los datos crudos
RAW_PATH = os.path.join(os.path.dirname(__file__), "../raw")
os.makedirs(RAW_PATH, exist_ok=True)

np.random.seed(0)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

def ingestar_proyectos(n=520):
    regiones = ["Arequipa","Puno","Cusco","Moquegua","Tacna",
                None,"AREQUIPA","arequipa"]           # ← errores reales
    tipos    = ["Carretera Nacional","Regional","Trocha","Puente","Bypass","??"]
    estados  = ["En ejecución","Planificado","Paralizado","Concluido",
                "en ejecucion","NULL"]                # ← variantes sucias

    df = pd.DataFrame({
        "id_proyecto"   : [f"PRY-{i:04d}" for i in range(1, n+1)],
        "region"        : np.random.choice(regiones, n),
        "tipo_via"      : np.random.choice(tipos, n),
        "estado"        : np.random.choice(estados, n),
        "contrato_soles": np.where(
            np.random.rand(n) < 0.05, -999,          # ← valores inválidos 5%
            np.random.uniform(500_000, 15_000_000, n).round(2)
        ),
        "avance_pct"    : np.where(
            np.random.rand(n) < 0.04, 999,           # ← 4% con error
            np.random.uniform(0, 100, n).round(1)
        ),
        "cpi"           : np.random.normal(0.95, 0.15, n).round(3),
        "spi"           : np.random.normal(0.93, 0.12, n).round(3),
        "_ingested_at"  : TIMESTAMP,
        "_source"       : "sqlserver_indumaq",
    })

    # Duplicar 20 filas (simula fallo de conexión que re-envía datos)
    df = pd.concat([df, df.sample(20)], ignore_index=True)

    path = f"{RAW_PATH}/proyectos_raw_{TIMESTAMP}.csv"
    df.to_csv(path, index=False)
    print(f"[BRONZE] proyectos → {len(df)} filas guardadas en {path}")
    return path

def ingestar_avance(n=50100):
    partidas = ["Corte de terreno","Relleno compactado","Base granular",
                "Imprimación","Carpeta asfáltica","Obras de arte","Señalización"]

    df = pd.DataFrame({
        "fecha"           : np.random.choice(
            pd.date_range("2024-01-01", "2024-12-31"), n),
        "id_proyecto"     : [f"PRY-{np.random.randint(1,501):04d}" for _ in range(n)],
        "partida"         : np.random.choice(partidas, n),
        "metrado_prog"    : np.random.uniform(10, 500, n).round(2),
        "metrado_ejec"    : np.where(
            np.random.rand(n) < 0.03, np.nan,        # ← 3% sin dato
            np.random.uniform(8, 510, n).round(2)
        ),
        "costo_prog_soles": np.random.uniform(1000, 80000, n).round(2),
        "costo_ejec_soles": np.random.uniform(900, 85000, n).round(2),
        "_ingested_at"    : TIMESTAMP,
        "_source"         : "tablet_field_app",
    })

    path = f"{RAW_PATH}/avance_raw_{TIMESTAMP}.csv"
    df.to_csv(path, index=False)
    print(f"[BRONZE] avance    → {len(df)} filas")
    return path

def ingestar_equipos(n=20050):
    tipos   = ["Excavadora","Compactadora","Motoniveladora","Volquete",
               "Pavimentadora","Retroexcavadora","Rodillo","Grúa"]
    estados = ["Operativo","Mantenimiento","Falla crítica","En espera",
               "OPERATIVO","null"]                    # ← texto inconsistente

    df = pd.DataFrame({
        "equipo_id"       : [f"EQ-{i:04d}" for i in np.random.randint(1,2001,n)],
        "tipo_equipo"     : np.random.choice(tipos, n),
        "horas_trabajadas": np.where(
            np.random.rand(n) < 0.02, -1,            # ← horas negativas (error)
            np.random.uniform(0, 2200, n).round(1)
        ),
        "combustible_L"   : np.random.uniform(50, 5500, n).round(1),
        "estado"          : np.random.choice(estados, n),
        "eficiencia_pct"  : np.random.uniform(40, 105, n).round(1),
        "fecha_registro"  : np.random.choice(
            pd.date_range("2024-01-01","2024-12-31"), n),
        "_ingested_at"    : TIMESTAMP,
        "_source"         : "iot_telemetry",
    })
    df = pd.concat([df, df.sample(50)], ignore_index=True)  # duplicados IoT

    path = f"{RAW_PATH}/equipos_raw_{TIMESTAMP}.csv"
    df.to_csv(path, index=False)
    print(f"[BRONZE] equipos   → {len(df)} filas")
    return path

def ingestar_materiales(n=30020):
    materiales  = ["Asfalto RC-250","Piedra 3/4","Arena Gruesa","Cemento",
                   "Emulsión","Geomalla","Tubería HDPE","Acero Corrugado"]
    proveedores = ["CONCREMAX","YURA","EXSA","UNACEM","CEMENTO SUR","FIRTH"]

    df = pd.DataFrame({
        "material"       : np.random.choice(materiales, n),
        "cantidad"       : np.where(
            np.random.rand(n) < 0.03, np.nan,
            np.random.uniform(10, 5000, n).round(2)
        ),
        "precio_unitario": np.random.uniform(5, 800, n).round(2),
        "unidad"         : np.random.choice(["m3","ton","m2","ml","und",None], n),
        "proveedor"      : np.random.choice(proveedores, n),
        "fecha_pedido"   : np.random.choice(
            pd.date_range("2024-01-01","2024-12-31"), n),
        "_ingested_at"   : TIMESTAMP,
        "_source"        : "erp_odoo",
    })

    path = f"{RAW_PATH}/materiales_raw_{TIMESTAMP}.csv"
    df.to_csv(path, index=False)
    print(f"[BRONZE] materiales→ {len(df)} filas")
    return path

def ingestar_incidencias(n=5020):
    tipos = ["Falla mecánica","Falla eléctrica","Accidente","Rotura tubería",
             "Deslizamiento","Falla software","Lluvia"]
    sevs  = ["Baja","Media","Alta","Crítica","BAJA","critica"]

    df = pd.DataFrame({
        "tipo_falla"       : np.random.choice(tipos, n),
        "severidad"        : np.random.choice(sevs, n,
                              p=[0.25,0.25,0.18,0.07,0.17,0.08]),
        "tiempo_parada_hrs": np.where(
            np.random.rand(n) < 0.04, -5,
            np.random.exponential(8, n).round(1)
        ),
        "costo_reparacion" : np.random.uniform(300, 55000, n).round(2),
        "equipo_id"        : [f"EQ-{np.random.randint(1,2001):04d}" for _ in range(n)],
        "id_proyecto"      : [f"PRY-{np.random.randint(1,501):04d}" for _ in range(n)],
        "fecha"            : np.random.choice(
            pd.date_range("2024-01-01","2024-12-31"), n),
        "_ingested_at"     : TIMESTAMP,
        "_source"          : "sistema_hsec",
    })

    path = f"{RAW_PATH}/incidencias_raw_{TIMESTAMP}.csv"
    df.to_csv(path, index=False)
    print(f"[BRONZE] incidencias→ {len(df)} filas")
    return path

# ── EJECUTAR ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("CAPA BRONZE — INDUMAQ S.R.L.")
    print("Simula: Apache NiFi → HDFS /data/bronze/")
    print("=" * 50)
    ingestar_proyectos()
    ingestar_avance()
    ingestar_equipos()
    ingestar_materiales()
    ingestar_incidencias()
    print("\n✅ Bronze completado. Revisa bronze/raw/")
