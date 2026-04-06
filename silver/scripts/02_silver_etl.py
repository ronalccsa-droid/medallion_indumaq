import pandas as pd
import glob
import os
from supabase import create_client

SUPABASE_URL = "https://axsulnbhpqeoxspvxdfs.supabase.co"
SUPABASE_KEY = "sb_secret_Hpr_3N-3rDH0WyQRyv7Phg_VQyBelJx"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BRONZE_RAW = os.path.join(os.path.dirname(__file__), "../../bronze/raw")

print("="*50)
print("CAPA SILVER — INDUMAQ S.R.L.")
print("="*50)

def latest(pattern):
    files = glob.glob(os.path.join(BRONZE_RAW, pattern))
    return max(files, key=os.path.getmtime) if files else None

def upload(df, table):
    # Elimina columnas extra que Supabase no reconoce
    cols_extra = ["_ingested_at", "_source", "_processed_at"]
    df = df.drop(columns=[c for c in cols_extra if c in df.columns])
    
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    for i in range(0, len(records), 500):
        supabase.table(table).upsert(records[i:i+500]).execute()
    print(f"  ✅ {table}: {len(records)} registros")

# PROYECTOS
f = latest("proyectos_raw_*.csv")
if f:
    df = pd.read_csv(f)
    df.columns = df.columns.str.lower().str.strip()
    print("Columnas proyectos:", df.columns.tolist())
    if "id_proyecto" in df.columns:
        df = df.rename(columns={"id_proyecto": "proyecto_id"})
    if "proyecto_id" not in df.columns:
        df["proyecto_id"] = ["PRY-" + str(i).zfill(4) for i in range(len(df))]
    df = df.drop_duplicates(subset=["proyecto_id"])
    
    # Manejamos contrato_soles como presupuesto real
    if "contrato_soles" in df.columns:
        df["contrato_soles"] = pd.to_numeric(df["contrato_soles"], errors="coerce").fillna(0)
    if "presupuesto" in df.columns:
        df = df.drop(columns=["presupuesto"])
        
    upload(df, "silver_proyectos")

# AVANCES
f = latest("avance_raw_*.csv")
if f:
    df = pd.read_csv(f)
    df.columns = df.columns.str.lower().str.strip()
    if "id_proyecto" in df.columns:
        df = df.rename(columns={"id_proyecto": "proyecto_id"})
    df["avance_id"] = ["AV-" + str(i).zfill(6) for i in range(len(df))]
    df["costo_prog_soles"] = pd.to_numeric(df.get("costo_prog_soles", 0), errors="coerce").fillna(0)
    df["costo_ejec_soles"] = pd.to_numeric(df.get("costo_ejec_soles", 0), errors="coerce").fillna(0)
    df["porcentaje_avance"] = (df["costo_ejec_soles"] / df["costo_prog_soles"].replace(0,1) * 100).clip(0,100).round(2)
    cols = ["avance_id","proyecto_id","fecha","partida","costo_prog_soles","costo_ejec_soles","porcentaje_avance"]
    df = df[[c for c in cols if c in df.columns]]
    upload(df, "silver_avances")

# EQUIPOS
f = latest("equipos_raw_*.csv")
if f:
    df = pd.read_csv(f)
    df.columns = df.columns.str.lower().str.strip()
    df = df.drop_duplicates(subset=["equipo_id"])
    df["horas_trabajadas"] = pd.to_numeric(df.get("horas_trabajadas",0), errors="coerce").fillna(0)
    df["costo_hora"] = 85.0
    df["costo_total"] = df["horas_trabajadas"] * df["costo_hora"]
    if "id_proyecto" in df.columns:
        df = df.rename(columns={"id_proyecto": "proyecto_id"})
    if "proyecto_id" not in df.columns:
        df["proyecto_id"] = None
    upload(df, "silver_equipos")

# MATERIALES
f = latest("materiales_raw_*.csv")
if f:
    df = pd.read_csv(f)
    df.columns = df.columns.str.lower().str.strip()
    print("Columnas materiales:", df.columns.tolist())
    if "id_proyecto" in df.columns:
        df = df.rename(columns={"id_proyecto": "proyecto_id"})
    if "material_id" not in df.columns:
        df["material_id"] = ["MAT-" + str(i).zfill(6) for i in range(len(df))]
    df = df.drop_duplicates(subset=["material_id"])
    for col in ["precio_unitario","cantidad","costo_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "costo_total" not in df.columns:
        df["costo_total"] = df.get("precio_unitario",0) * df.get("cantidad",1)
    upload(df, "silver_materiales")

print("\n✅ Silver completado.")