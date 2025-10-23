import pandas as pd
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Ignorar advertencias de matplotlib/seaborn que a veces aparecen
warnings.filterwarnings("ignore")

# =========================================================================
# 1. CONFIGURACIÓN INICIAL
# =========================================================================

# Nombre del archivo de entrada
INPUT_FILE = "base_de_datos_tiktok.csv"
TODAY = datetime.now()

# ⚠️ La carpeta de salida incluye la fecha de ejecución ⚠️
FOLDER_NAME = f"analisis_{TODAY.strftime('%Y%m%d')}"

# Determinar el mes y año actual para el filtrado
CURRENT_MONTH_YEAR = TODAY.strftime('%Y-%m')

# Columnas de interacciones (se usarán en varios pasos)
ENGAGEMENT_METRICS = [
    'digg_count', 'comment_count', 'share_count', 'collect_count',
    'download_count', 'repost_count', 'whatsapp_share_count'
]
ERV_NAMES = [
    'ERV_Likes', 'ERV_Comments', 'ERV_Shares', 'ERV_Collects',
    'ERV_Downloads', 'ERV_Reposts', 'ERV_WhatsApp'
]

def setup_environment():
    """Crea la carpeta de salida y carga el DataFrame."""
    
    # Crear la carpeta de salida
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)
        print(f"Carpeta de salida creada: '{FOLDER_NAME}'")
    else:
        print(f"Usando carpeta de salida existente: '{FOLDER_NAME}'")

    # Cargar los datos
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Archivo '{INPUT_FILE}' cargado. Filas totales: {len(df)}")
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{INPUT_FILE}' no se encontró.")
        return None

    # Limpieza: Asegurar que las columnas de fecha y conteo sean correctas
    df['readable_date'] = pd.to_datetime(df['readable_date'], errors='coerce')
    
    # Convertir columnas de conteo a tipo numérico (integer)
    count_columns = ['play_count'] + ENGAGEMENT_METRICS
    for col in count_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    return df

# =========================================================================
# 2. PASO 1: PREPARACIÓN Y FILTRADO
# =========================================================================

def step_1_data_preparation(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra el DataFrame por el mes en curso."""
    
    print(f"\n--- PASO 1: Filtrando datos para el mes/año: {CURRENT_MONTH_YEAR} ---")
    
    # Filtrar los datos por el mes y año actual
    df['analysis_month'] = df['readable_date'].dt.strftime('%Y-%m')
    df_filtered = df[df['analysis_month'] == CURRENT_MONTH_YEAR].copy()
    
    # Guardar el DataFrame filtrado
    output_path = os.path.join(FOLDER_NAME, "01_data_filtrada_mensual.csv")
    df_filtered.to_csv(output_path, index=False)
    
    print(f"Filas después del filtrado: {len(df_filtered)}")
    print(f"Datos filtrados guardados en: {output_path}")
    
    # Crear columna datetime para el análisis de oportunidad posterior
    df_filtered['create_time_dt'] = pd.to_datetime(df_filtered['create_time'], unit='s')
    
    return df_filtered

# =========================================================================
# 3. PASO 2: MÉTRICAS BÁSICAS MENSUALES POR PERFIL
# =========================================================================

def step_2_monthly_summary(df: pd.DataFrame):
    """Genera la tabla de resumen de actividad mensual por perfil."""
    
    print("\n--- PASO 2: Métricas Básicas Mensuales por Perfil (Resumen) ---")

    # 1. Definir agregaciones: contar videos y sumar métricas
    aggregation_functions = {
        'video_id': 'count' # Videos publicados
    }
    for col in ['play_count'] + ENGAGEMENT_METRICS:
        aggregation_functions[col] = 'sum' # Suma de interacciones

    # 2. Aplicar la agregación
    df_summary = df.groupby('profile_handle').agg(aggregation_functions).reset_index()
    df_summary = df_summary.rename(columns={'video_id': 'videos_publicados_mes'})
    
    # 3. Ordenar y guardar
    df_summary = df_summary.sort_values(by='videos_publicados_mes', ascending=False)
    output_path = os.path.join(FOLDER_NAME, "02_monthly_summary.csv")
    df_summary.to_csv(output_path, index=False)
    
    print(f"Tabla de resumen mensual guardada en: {output_path}")

# =========================================================================
# 4. PASO 3: ANÁLISIS DE ENGAGEMENT (RATIOS) Y VISUALIZACIÓN
# =========================================================================

def step_3_engagement_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la Tasa de Engagement por Vista (ERV) por métrica para cada video,
    calcula el promedio por perfil y genera gráficos.
    """
    
    print("\n--- PASO 3: Análisis de Engagement (Ratios) y Visualización ---")
    
    # Remplazar play_count 0 con 1 para evitar ZeroDivisionError en el denominador,
    # aunque la fórmula maneja esto para establecer el ERV a 0 en ese caso.
    # Usaremos np.where para establecer el ratio a 0 si play_count es 0.
    
    df_erv_summary = pd.DataFrame({'profile_handle': df['profile_handle'].unique()}).set_index('profile_handle')

    # 1. Cálculo de Tasa de Engagement por Métrica (ERV) por video
    # FÓRMULA: ERV = (Métrica / play_count) * 100
    for metric, name in zip(ENGAGEMENT_METRICS, ERV_NAMES):
        # np.where(condición, valor_si_verdadero, valor_si_falso)
        df[name] = np.where(
            df['play_count'] > 0,
            (df[metric] / df['play_count']) * 100,
            0
        )
        # Agregar el promedio al DataFrame resumen por perfil
        df_erv_summary[name] = df.groupby('profile_handle')[name].mean()

    # 2. Calcular la Tasa de Engagement Total por video (para el Paso 4)
    df['total_interactions'] = df[ENGAGEMENT_METRICS].sum(axis=1)
    # FÓRMULA: Tasa de Engagement Total = (Suma de Interacciones / play_count) * 100
    df['total_engagement_rate'] = np.where(
        df['play_count'] > 0,
        (df['total_interactions'] / df['play_count']) * 100,
        0
    )
    
    # 3. Guardar el resumen de promedios de ERV por perfil
    df_erv_summary = df_erv_summary.reset_index()
    output_path = os.path.join(FOLDER_NAME, "03_profile_engagement_ratios.csv")
    df_erv_summary.to_csv(output_path, index=False)
    print(f"Tabla de promedios de ERV guardada en: {output_path}")
    
    # 4. Preparación de datos para Visualización (Long format)
    df_plot_data = df_erv_summary.melt(
        id_vars='profile_handle',
        var_name='Metric',
        value_name='Avg_ERV'
    )
    output_plot_data = os.path.join(FOLDER_NAME, "03_plot_engagement_ratios_data.csv")
    df_plot_data.to_csv(output_plot_data, index=False)
    print(f"Datos para la visualización guardados en: {output_plot_data}")
    
    # 5. Generar Gráfico de Barras Agrupadas
    plt.figure(figsize=(14, 8))
    sns.barplot(
        data=df_plot_data,
        x='profile_handle',
        y='Avg_ERV',
        hue='Metric',
        palette='viridis'
    )
    plt.title('Tasa de Engagement Promedio por Vista (ERV) por Perfil y Métrica', fontsize=16)
    plt.xlabel('Perfil', fontsize=14)
    plt.ylabel('Tasa de Engagement Promedio (%)', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Métrica de Interacción', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    output_png = os.path.join(FOLDER_NAME, "03_ERV_bar_chart.png")
    plt.savefig(output_png)
    plt.close()
    print(f"Gráfico de Barras guardado en: {output_png}")
    
    # 6. Generar Heatmap (Mapa de Calor)
    df_heatmap = df_erv_summary.set_index('profile_handle')
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        df_heatmap,
        annot=True,
        fmt=".2f",
        cmap="YlGnBu",
        linewidths=.5,
        cbar_kws={'label': 'Tasa de Engagement Promedio (%)'}
    )
    plt.title('Heatmap de Tasa de Engagement Promedio por Perfil y Métrica', fontsize=16)
    plt.ylabel('Perfil', fontsize=14)
    plt.xlabel('Métrica de Engagement (ERV)', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    output_png = os.path.join(FOLDER_NAME, "03_ERV_heatmap.png")
    plt.savefig(output_png)
    plt.close()
    print(f"Heatmap guardado en: {output_png}")
    
    return df

# =========================================================================
# 5. PASO 4: TOP 3 PUBLICACIONES CON MAYOR ENGAGEMENT
# =========================================================================

def step_4_top_3_posts(df: pd.DataFrame):
    """Identifica el Top 3 global de publicaciones con mayor Tasa de Engagement Total."""
    
    print("\n--- PASO 4: Top 3 de Publicaciones con Mayor Engagement ---")
    
    # 1. El cálculo de 'total_engagement_rate' se realizó en el Paso 3
    # Ordenar por Tasa de Engagement Total y seleccionar el Top 3
    df_top_3 = df.sort_values(by='total_engagement_rate', ascending=False).head(3)

    # 2. Seleccionar y formatear columnas
    df_top_3 = df_top_3[[
        'profile_handle', 'description', 'url', 'video_id', 'total_engagement_rate'
    ]].copy()
    df_top_3['total_engagement_rate'] = df_top_3['total_engagement_rate'].round(2)

    # 3. Guardar el resultado en CSV
    output_path = os.path.join(FOLDER_NAME, "04_top_3_posts.csv")
    df_top_3.to_csv(output_path, index=False)
    
    print(f"Tabla de Top 3 posts guardada en: {output_path}")

# =========================================================================
# 6. PASOS 5 Y 6: ANÁLISIS DE TENDENCIA Y OPORTUNIDAD
# =========================================================================

def step_5_6_advanced_analysis(df: pd.DataFrame):
    """
    Realiza análisis de frecuencia diaria, longitud de contenido y tiempo óptimo.
    """
    
    print("\n--- PASO 5/6: Análisis de Tendencia y Oportunidad ---")
    
    # -----------------------------------------------------------
    # 6.A. Análisis de Frecuencia de Publicación Diaria (Paso 5)
    # -----------------------------------------------------------
    
    # 1. Extraer solo la fecha
    df['date_only'] = df['readable_date'].dt.date
    
    # 2. Contar la cantidad de posts diarios por perfil
    df_daily_posts = df.groupby(['date_only', 'profile_handle']).agg(
        posts_count=('video_id', 'count')
    ).reset_index()

    # 3. Guardar la tabla de datos diarios (CSV)
    df_daily_posts['date_only'] = df_daily_posts['date_only'].astype(str)
    output_path_csv = os.path.join(FOLDER_NAME, "05_daily_post_count.csv")
    df_daily_posts.to_csv(output_path_csv, index=False)
    print(f"6.A. Tabla de posts diarios guardada en: {output_path_csv}")

    # 4. Generar Gráfico de Líneas
    df_plot = df_daily_posts.copy()
    df_plot['date_only'] = pd.to_datetime(df_plot['date_only'])
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(
        data=df_plot, x='date_only', y='posts_count', hue='profile_handle',
        marker='o', dashes=False, palette='Spectral'
    )
    plt.title('Cantidad de Posts Diarios por Perfil (Tendencia Mensual)', fontsize=16)
    plt.xlabel('Fecha', fontsize=14)
    plt.ylabel('Cantidad de Posts', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title='Perfil', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    output_path_png = os.path.join(FOLDER_NAME, "05_daily_post_line_chart.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"6.A. Gráfico de Líneas guardado en: {output_path_png}")

    # -----------------------------------------------------------
    # 6.B. Análisis de Longitud de Contenido
    # -----------------------------------------------------------
    
    # 1. Calcular la longitud del contenido
    df['description_length'] = df['description'].fillna('').astype(str).apply(len)
    df['transcript_length'] = df['transcript'].fillna('').astype(str).apply(len)

    # 2. Agrupar y calcular el promedio de longitud
    df_content_length = df.groupby('profile_handle')[['description_length', 'transcript_length']].mean().reset_index()
    df_content_length.columns = ['profile_handle', 'avg_description_length', 'avg_transcript_length']

    # 3. Guardar la tabla (CSV)
    output_path = os.path.join(FOLDER_NAME, "06b_content_length_summary.csv")
    df_content_length.to_csv(output_path, index=False)
    print(f"\n6.B. Tabla de longitud de contenido guardada en: {output_path}")

    # -----------------------------------------------------------
    # 6.C. Análisis de Oportunidad por Hora y Día (Optimal Time)
    # -----------------------------------------------------------
    
    # 1. Extraer hora y día de la semana
    df['hour'] = df['create_time_dt'].dt.hour
    df['day_of_week'] = df['create_time_dt'].dt.dayofweek # 0=Lunes, 6=Domingo
    day_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}

    # 2. Hora Óptima: Agrupar por hora y perfil, y calcular el promedio de play_count
    df_optimal_hour = df.groupby(['hour', 'profile_handle']).agg(
        avg_play_count=('play_count', 'mean')
    ).reset_index()

    # 3. Guardar la tabla de hora óptima (CSV)
    output_path_hour = os.path.join(FOLDER_NAME, "06c_optimal_hour_analysis.csv")
    df_optimal_hour.to_csv(output_path_hour, index=False)
    print(f"6.C. Tabla de hora óptima guardada en: {output_path_hour}")

    # 4. Generar Gráfico de Líneas para Hora Óptima
    plt.figure(figsize=(14, 6))
    sns.lineplot(
        data=df_optimal_hour, x='hour', y='avg_play_count', hue='profile_handle',
        marker='o', dashes=False, palette='Spectral'
    )
    plt.title('Vistas Promedio por Hora de Publicación (Optimal Hour)', fontsize=16)
    plt.xlabel('Hora del Día (0-23)', fontsize=14)
    plt.ylabel('Vistas Promedio (Play Count)', fontsize=14)
    plt.xticks(range(0, 24))
    plt.grid(axis='both', linestyle='--', alpha=0.7)
    plt.legend(title='Perfil', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    output_path_png = os.path.join(FOLDER_NAME, "06c_optimal_hour_line_chart.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"6.C. Gráfico de Líneas de hora óptima guardado en: {output_path_png}")
    
    # 5. Día Óptimo: Agrupar por día de la semana y perfil, y calcular el promedio de play_count
    df_optimal_day = df.groupby(['day_of_week', 'profile_handle']).agg(
        avg_play_count=('play_count', 'mean')
    ).reset_index()
    
    # 6. Guardar la tabla de día óptimo (CSV)
    df_optimal_day['day_name'] = df_optimal_day['day_of_week'].map(day_map)
    output_path_day = os.path.join(FOLDER_NAME, "06c_optimal_day_analysis.csv")
    df_optimal_day.to_csv(output_path_day, index=False)
    print(f"6.C. Tabla de día óptimo guardada en: {output_path_day}")


# =========================================================================
# FUNCIÓN PRINCIPAL DE EJECUCIÓN
# =========================================================================
def main():
    df = setup_environment()
    
    if df is not None:
        # Ejecutar los pasos en orden
        df_filtered = step_1_data_preparation(df)
        
        if len(df_filtered) > 0:
            step_2_monthly_summary(df_filtered)
            df_with_erv = step_3_engagement_analysis(df_filtered)
            step_4_top_3_posts(df_with_erv)
            step_5_6_advanced_analysis(df_with_erv)
            
            print("\n✅ Proceso de análisis de métricas finalizado. Los resultados están en la carpeta:", FOLDER_NAME)
        else:
            print("\n⚠️ No se encontraron datos para el mes en curso. Finalizando el script.")

if __name__ == "__main__":
    main()
