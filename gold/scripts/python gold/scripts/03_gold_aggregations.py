import pandas as pd
import os
from supabase import create_client

SUPABASE_URL = "https://axsulnbhpqeoxspvxdfs.supabase.co"
SUPABASE_KEY = "sb_secret_Hpr_3N-3rDH0WyQRyv7Phg_VQyBelJx"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("=" * 50)
print("CAPA GOLD — INDUMAQ S.R.L.")
print("Agregaciones y KPIs para el dashboard")
print("=" * 50)

def load(table):
    data = supabase.table(table).select("*").execute().data
    return pd.DataFrame(data) if data else pd.DataFrame()

print("🔄 Cargando tablas Silver...")
proyectos  = load("silver_proyectos")
materiales = load("silver_materiales")
equipos    = load("silver_equipos")
avances    = load("silver_avances")

print(f"  Proyectos:  {len(proyectos)}")
print(f"  Materiales: {len(materiales)}")
print(f"  Equipos:    {len(equipos)}")
print(f"  Avances:    {len(avances)}")

kpis = []
for _, p in proyectos.iterrows():
    pid = p.get("proyecto_id")

    gasto_mat = materiales[materiales["proyecto_id"] == pid]["costo_total"].sum() if len(materiales) > 0 else 0
    gasto_eq  = equipos[equipos["proyecto_id"] == pid]["costo_total"].sum() if len(equipos) > 0 else 0
    gasto_total = gasto_mat + gasto_eq

    # Leemos contrato_soles porque ese es el presupuesto real en Supabase
    presupuesto = float(p.get("contrato_soles") or p.get("presupuesto") or 1)
    cpi = round(presupuesto / gasto_total, 4) if gasto_total > 0 else None

    av_rows = avances[avances["proyecto_id"] == pid] if len(avances) > 0 else pd.DataFrame()
    avance_prom = round(av_rows["porcentaje_avance"].mean(), 2) if not av_rows.empty else 0

    kpis.append({
        "proyecto_id":      pid,
        "nombre_proyecto":  p.get("nombre_proyecto") or p.get("region", "Proyecto Genérico"),
        "presupuesto":      presupuesto,
        "gasto_materiales": float(gasto_mat),
        "gasto_equipos":    float(gasto_eq),
        "gasto_mano_obra":  0.0,
        "gasto_total":      float(gasto_total),
        "cpi":              cpi,
        "avance_promedio":  float(avance_prom),
        "estado":           str(p.get("estado", "activo")),
    })

df_kpis = pd.DataFrame(kpis)

if not df_kpis.empty:
    records = df_kpis.where(pd.notnull(df_kpis), None).to_dict(orient="records")
    supabase.table("gold_kpis_proyectos").upsert(records).execute()
    print(f"\n✅ Gold KPIs: {len(records)} proyectos guardados en Supabase")
    print(df_kpis[["nombre_proyecto", "presupuesto", "gasto_total", "cpi", "avance_promedio"]].head().to_string())
else:
    print("\n⚠️ No se encontraron proyectos en la capa Silver para procesar.")

print("\n🏆 Gold completado.")