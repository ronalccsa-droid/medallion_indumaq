import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import random
from supabase import create_client

# ── Configuración de Página ───────────────────────────────
st.set_page_config(
    page_title="INDUMAQ S.R.L. — Sistema de Control Big Data",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo CSS personalizado para métricas y layouts
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: bold; }
    [data-testid="stMetricDelta"] { font-size: 0.9rem; }
    div[data-testid="stExpander"] div[role="button"] p { font-weight: bold; font-size: 1.1rem; color: #2196F3; }
    .status-container { padding: 10px; border-radius: 5px; margin-bottom: 10px; }
    .perf-ok { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
    .perf-bad { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    </style>
    """, unsafe_allow_html=True)

# ── Conexión a Supabase ───────────────────────────────────
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error("⚠️ Error de credenciales. Verifica .streamlit/secrets.toml")
    st.stop()

# ── Carga de Datos Gold (Mejorada con Simulación de Datos) ───────
@st.cache_data(ttl=60)
def cargar_datos():
    try:
        # Intentamos cargar desde Supabase
        res = supabase.table("gold_kpis_proyectos").select("*").execute()
        df = pd.DataFrame(res.data)
        
        # Validación de datos vacíos (tu problema actual)
        if df.empty or df['gasto_total'].sum() == 0:
            st.warning("⚠️ Los datos reales en Supabase están incompletos. Generando datos simulados para visualización...")
            df = generar_datos_simulados()
            es_real = False
        else:
            es_real = True

        # Limpieza y normalización
        col_proyecto = "nombre_proyecto" if "nombre_proyecto" in df.columns else "proyecto_id"
        if col_proyecto not in df.columns: col_proyecto = df.columns[0]
        
        # Convertimos columnas a numérico asegurando que no haya errores
        cols_num = ["presupuesto", "gasto_materiales", "gasto_equipos", "gasto_total", "cpi", "avance_promedio"]
        for col in cols_num:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Limpieza de nombres de proyecto y region (Capitalize)
        df[col_proyecto] = df[col_proyecto].astype(str).str.strip().str.capitalize()
        if 'region' in df.columns:
            df['region'] = df['region'].astype(str).str.strip().str.capitalize()
        else:
            df['region'] = np.random.choice(["Arequipa", "Cusco", "Puno", "Tacna"], len(df)) # Fallback
            
        return df, col_proyecto, es_real
    except Exception as e:
        st.error(f"Error cargando datos: {e}. Generando datos simulados...")
        return generar_datos_simulados(), "nombre_proyecto", False

# Función auxiliar para simular datos realistas cuando la base de datos está vacía
def generar_datos_simulados():
    proyectos_nombres = ["Mejoramiento carretera Tacna-Tarata", "Pavimentación urbana Cayma", "Construcción Puente Uchumayo", 
                         "Camino rural Cabanaconde", "Ampliación vía Yanahuara", "Rehabilitación Puno-Juliaca", "Vía costanera Mollendo"]
    regiones = ["Arequipa", "Arequipa", "Arequipa", "Arequipa", "Arequipa", "Puno", "Tacna"]
    estados = ["En ejecución", "En ejecución", "Concluido", "Suspendido", "En revisión", "Concluido", "En ejecución"]
    
    data = []
    for i, (nombre, region) in enumerate(zip(proyectos_nombres, regiones)):
        presupuesto = random.uniform(2_000_000, 15_000_000)
        gasto_total = presupuesto * random.uniform(0.3, 1.1) # Simula sobrecosto o ahorro
        avance = random.uniform(0.1, 100)
        cpi = presupuesto / gasto_total if gasto_total > 0 else 1.0
        
        data.append({
            "proyecto_id": f"PRY-{str(i+1).zfill(4)}",
            "nombre_proyecto": nombre,
            "region": region,
            "estado": random.choice(estados),
            "presupuesto": presupuesto,
            "gasto_total": gasto_total,
            "gasto_materiales": gasto_total * random.uniform(0.4, 0.6),
            "gasto_equipos": gasto_total * random.uniform(0.2, 0.4),
            "gasto_mano_obra": gasto_total * 0.1,
            "cpi": round(cpi, 3),
            "avance_promedio": round(avance, 1)
        })
    return pd.DataFrame(data)

df_proyectos_all, col_proyecto, datos_son_reales = cargar_datos()

# ── Header Profesional ────────────────────────────────────
header_col1, header_col2 = st.columns([1, 4])
with header_col1:
    st.image("https://images.vexels.com/content/138407/preview/crane-construction-flat-icon-5d34fb.png", width=120) # Icono genérico de grúa
with header_col2:
    st.title("🏗️ INDUMAQ S.R.L.")
    st.header("Dashboard Gerencial de Control de Costos — Big Data")
    # Indicador de estado de la data
    tipo_data_msg = "Datos Reales desde Supabase" if datos_son_reales else "Demostración con Datos Simulados"
    st.caption(f"{tipo_data_msg} | Arquitectura Medallion IDL3 — Arequipa, Perú")

st.divider()

# ── Barra Lateral (Filtros UX Mejorada) ───────────────────
st.sidebar.header("🔍 Filtros de Análisis")
st.sidebar.divider()

# Filtro Región
regiones = df_proyectos_all["region"].dropna().unique().tolist()
regiones.sort()
region_sel = st.sidebar.multiselect("📍 Región", regiones, default=regiones)

# Filtro Estado
estados = df_proyectos_all["estado"].dropna().unique().tolist()
estados.sort()
estado_sel = st.sidebar.multiselect("🚦 Estado Proyecto", estados, default=estados)

st.sidebar.divider()
st.sidebar.info("Este dashboard analiza la eficiencia económica de los proyectos viales en el sur del Perú.")

# Aplicar filtros
df_filtrado = df_proyectos_all[
    (df_proyectos_all["region"].isin(region_sel)) &
    (df_proyectos_all["estado"].isin(estado_sel))
]

if df_filtrado.empty:
    st.error("❌ No hay proyectos que coincidan con los filtros seleccionados.")
    st.stop()

# ── Definición de Pestañas (Tabs) ───────────────────────
tab1, tab2 = st.tabs(["📊 Resumen Gerencial Portafolio", "📋 Detalle por Proyecto"])

with tab1:
    # ── KPIs Principales del Portafolio ───────────────────
    total_presupuesto = df_filtrado["presupuesto"].sum()
    total_gasto = df_filtrado["gasto_total"].sum()
    proyectos_count = len(df_filtrado)
    
    # Calcular CPI promedio del portafolio (Evitamos N/A)
    # CPI portafolio = Suma(Presupuestos) / Suma(Gastos Totales)
    cpi_global = total_presupuesto / total_gasto if total_gasto > 0 else 1.0
    avance_global_avg = df_filtrado["avance_promedio"].mean()

    # Contenedor para KPIs Portafolio
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        st.metric("Total Proyectos", f"{proyectos_count}")

    with kpi_col2:
        st.metric("Presupuesto Controlado", f"S/. {total_presupuesto:,.0f}")
    
    with kpi_col3:
        # KPI Gasto con indicador de sobrecosto
        desviacion = total_gasto - total_presupuesto
        delta_msg = f"S/. {abs(desviacion):,.0f} " + ("✅ Ahorro" if desviacion <= 0 else "⚠️ Sobrecosto")
        st.metric("Gasto Total Ejecutado", f"S/. {total_gasto:,.0f}", 
                  delta=delta_msg, delta_color="inverse" if desviacion > 0 else "normal")

    with kpi_col4:
        # CPI Portafolio Profesional
        perf_class = "perf-ok" if cpi_global >= 1 else "perf-bad"
        # Usamos markdown para aplicar estilo de fondo
        st.markdown(f'<div class="status-container {perf_class}">', unsafe_allow_html=True)
        st.metric("Índice de Costo Global (CPI)", f"{cpi_global:.2f}", 
                  help="Suma Presupuesto / Suma Gasto Actual. < 1.0 = Sobrecosto.")
        st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    # ── Gráficos Gerenciales (UX Mejorada) ─────────────────
    c_graf1, c_graf2 = st.columns([2, 1])

    with c_graf1:
        st.subheader("📈 Presupuesto vs Gasto Ejecutado por Proyecto (S/.)")
        df_melted = df_filtrado.melt(id_vars=[col_proyecto], value_vars=["presupuesto", "gasto_total"],
                                     var_name="Métrica", value_name="Monto")
        # Gráfico Plotly moderno con colores de la marca
        fig1 = px.bar(
            df_melted,
            x=col_proyecto,
            y="Monto",
            color="Métrica",
            barmode="group",
            color_discrete_map={"presupuesto": "#2196F3", "gasto_total": "#FF5722"}, # Azúl vs Naranja vibrante
            labels={"Monto": "Moneda Soles (S/.)", col_proyecto: "Proyecto"},
            title="Presupuesto vs Gasto Actual (S/.)"
        )
        fig1.update_layout(xaxis_tickangle=-40, height=500)
        fig1.update_yaxes(tickformat="$,.0f") # Formato de miles y S/. en eje Y
        st.plotly_chart(fig1, use_container_width=True)

    with c_graf2:
        st.subheader("Desglose Total de Gastos del Portafolio")
        desglose = df_filtrado[["gasto_materiales", "gasto_equipos", "gasto_mano_obra"]].sum()
        gastos_pie_df = pd.DataFrame({
            "Categoría": ["Materiales", "Equipos", "Mano de Obra"],
            "Monto": desglose.values
        })
        fig4 = px.pie(
            gastos_pie_df,
            names="Categoría",
            values="Monto",
            hole=.4, # Gráfico de dona, más moderno
            color_discrete_sequence=["#1976D2", "#FFA000", "#43A047"], # Variaciones de azul, naranja, verde
            labels={"Monto": "Total Gastado"},
            title="Distribución de Gasto Ejecutado"
        )
        fig4.update_traces(textinfo='percent+label', textposition='inside')
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()
    
    # ── Portafolio Bubble Chart (Avance vs CPI) ─────────────
    st.subheader("🚧 Mapa de Riesgo: Avance Físico (%) vs Eficiencia de Costo (CPI)")
    fig_risk = px.scatter(
        df_filtrado,
        x="avance_promedio",
        y="cpi",
        size="presupuesto",
        color="cpi",
        color_continuous_scale=["red", "yellow", "green"], # Semáforo de riesgo
        text=col_proyecto, # Etiquetas de texto en los puntos
        range_x=[0, 105],
        range_y=[0, 2],
        labels={"avance_promedio": "Avance Promedio (%)", "cpi": "Índice de Costo (CPI)"},
        title="Riesgo del Portafolio: Tamaño es Presupuesto"
    )
    fig_risk.update_traces(textposition='top center')
    st.plotly_chart(fig_risk, use_container_width=True)

with tab2:
    # ── Análisis Individual por Proyecto ────────────────────
    st.subheader("🔍 Seleccione un proyecto para análisis detallado")
    
    # Selector Dinámico basado en filtros anteriores
    proyectos_filtrados = df_filtrado[col_proyecto].tolist()
    proyectos_filtrados.sort()
    proyecto_sel = st.selectbox("📁 Proyecto a Analizar", proyectos_filtrados)
    
    # Filtrado final del proyecto único
    p_data = df_filtrado[df_filtrado[col_proyecto] == proyecto_sel].iloc[0]
    
    c_det1, c_det2 = st.columns(2)
    
    with c_det1:
        # Ficha técnica del proyecto
        st.write(f"### Ficha Técnica: {proyecto_sel}")
        st.write(f"**📍 Región:** {p_data['region']}")
        st.write(f"**🚦 Estado Actual:** {p_data.get('estado', 'N/A')}")
        
        pres = float(p_data["presupuesto"])
        ejec = float(p_data["gasto_total"])
        desv = ejec - pres
        
        st.markdown(f'<p style="font-size:1.5rem; font-weight:bold; color: #1976D2;">Presupuesto: S/. {pres:,.0f}</p>', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:1.5rem; font-weight:bold; color: #D32F2F;">Gasto Actual: S/. {ejec:,.0f}</p>', unsafe_allow_html=True)
        
        # Estado de desempeño individual
        cpi_p = float(p_data['cpi'])
        desemp_msg = f"CPI {cpi_p:.2f} — ✅ Ejecutando con ahorro" if cpi_p >= 1 else f"CPI {cpi_p:.2f} — ⚠️ Sobrecosto detectado"
        desemp_class = "perf-ok" if cpi_p >= 1 else "perf-bad"
        st.markdown(f'<div class="status-container {desemp_class}">{desemp_msg}</div>', unsafe_allow_html=True)
    
    with c_det2:
        # Desglose de gastos del proyecto
        gastos_p = pd.DataFrame({
            "Categoría": ["Materiales", "Equipos", "Mano de Obra"],
            "Monto": [
                float(p_data.get("gasto_materiales", 0) or 0),
                float(p_data.get("gasto_equipos", 0) or 0),
                float(p_data.get("gasto_mano_obra", 0) or 0)
            ]
        })
        gastos_p = gastos_p[gastos_p["Monto"] > 0] # Filtramos nulos
        
        if not gastos_p.empty:
            st.write("### Desglose de Gastos Actuales")
            fig_p_pie = px.bar(
                gastos_p,
                x="Categoría",
                y="Monto",
                color="Categoría",
                color_discrete_map={"Materiales": "#1976D2", "Equipos": "#FFA000", "Mano de Obra": "#43A047"},
                labels={"Monto": "Gasto (S/.)"}
            )
            fig_p_pie.update_layout(showlegend=False, height=350)
            fig_p_pie.update_yaxes(tickformat="$,.0f")
            st.plotly_chart(fig_p_pie, use_container_width=True)
        else:
            st.info("No hay desglose de gastos disponible para este proyecto.")

    st.divider()
    # Tabla de Datos Gold Pura (UX Mejorada, más compacta)
    st.subheader("📋 Tabla Gold — KPIs Completos del Portafolio Filtrado")
    # Limpiamos nombres de columnas de Supabase para que sean profesionales
    df_presentacion = df_filtrado.drop(columns=['_ingested_at', '_source', 'created_at'], errors='ignore')
    # Formateamos numéricos para que se lean bien en la tabla
    st.dataframe(df_presentacion.style.format({
        'presupuesto': 'S/. {:,.0f}',
        'gasto_total': 'S/. {:,.0f}',
        'gasto_materiales': 'S/. {:,.0f}',
        'gasto_equipos': 'S/. {:,.0f}',
        'cpi': '{:.2f}',
        'avance_promedio': '{:.1f}%'
    }), use_container_width=True)

# ── Footer Profesional ────────────────────────────────────
st.divider()
footer_col1, footer_col2 = st.columns([4, 1])
with footer_col1:
    st.caption("INDUMAQ S.R.L. | Arquitectura Medallion Big Data IDL3 — Panel de Control Ejecutivo")
with footer_col2:
    st.caption("Arequipa, Perú — Abril 2026")