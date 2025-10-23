import requests
import csv
import os
import time
from datetime import datetime, date
import pandas as pd
import json

# --- CONFIGURACIÓN E INICIALIZACIÓN ---
try:
    # Asegúrate de que tu archivo 'config.py' existe y contiene la variable 'SCRAPE_API_KEY'.
    from config import SCRAPE_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable 'SCRAPE_API_KEY'.")
    exit()

# --- CONSTANTES GLOBALES ---
PROFILES_FILE = "perfiles_tiktok.txt"
OUTPUT_CSV_FILE_TIKTOK = "base_de_datos_tiktok.csv"
BATCH_SIZE = 5             # Guardar el progreso cada 5 archivos nuevos
DUPLICATE_THRESHOLD = 4    # Detenerse si 4 o más posts son duplicados en la página

# Endpoint de la API
BASE_URL_POSTS = "https://api.scrapecreators.com/v3/tiktok/profile/videos"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY
}

# Definición del encabezado final
FIELDNAMES_TIKTOK = [
    'profile_handle', 'video_id', 'description', 
    'create_time', 'readable_date', 'url', # 'create_time' es el TIMESTAMP UNIX (ID ÚNICO)
    'play_count', 'digg_count', 'comment_count', 'share_count', 
    'collect_count', 'download_count', 'repost_count', 
    'whatsapp_share_count', 'transcript'
]

# --- FUNCIONES AUXILIARES ---

def get_start_of_current_month_timestamp() -> int:
    """ 
    Calcula el timestamp UNIX del primer segundo del mes actual.
    """
    # 1. Obtiene la fecha actual
    today = date.today()
    # 2. Crea un objeto datetime para el primer día del mes actual, a las 00:00:00
    start_of_month = datetime(today.year, today.month, 1)
    # 3. Convierte ese datetime a timestamp UNIX (entero)
    return int(start_of_month.timestamp())


def load_existing_timestamps(filename: str) -> set:
    """ 
    Carga los timestamps UNIX (identificadores únicos) existentes para deduplicación. 
    """
    existing_timestamps = set()
    if os.path.exists(filename):
        try:
            # Lectura del CSV forzando 'create_time' a string
            df_existing = pd.read_csv(filename, dtype={'create_time': str})
            
            existing_timestamps = set(df_existing['create_time'].dropna())
            
        except Exception as e:
            print(f"⚠️ Advertencia: No se pudo cargar el historial de timestamps. Error: {e}. Se continuará con set vacío.")
    return existing_timestamps


def get_tiktok_videos_page(handle: str, cursor: str or None) -> dict or None:
    """ 
    Realiza una llamada a la API para obtener videos.
    """
    url = BASE_URL_POSTS
    headers = {"x-api-key": SCRAPE_API_KEY}
    params = {"handle": handle}
    
    if cursor:
        params["max_cursor"] = cursor
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        
        data = response.json()
        
        if not isinstance(data, dict) or data.get('status_code') != 0:
             print(f"  ⚠️ Error de API: {data.get('status_msg', 'Mensaje de error no disponible')}")
             return None
        
        if not data.get('aweme_list') and data.get('has_more') == 0:
             return data
            
        return data
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión o HTTP para el perfil {handle}: {e}")
        return None
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al obtener videos de {handle}: {e}")
        return None

def save_batch_to_csv_tiktok(data_batch: list, file_name: str, total_new_posts_added: int):
    """ Guarda el lote de datos y actualiza la consola. """
    try:
        # Determinar si se debe escribir el encabezado
        write_header = not os.path.exists(file_name) or os.path.getsize(file_name) == 0
        
        with open(file_name, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES_TIKTOK)

            if write_header:
                writer.writeheader()

            writer.writerows(data_batch)
        print(f"  💾 Guardado incremental exitoso. Total de posts únicos añadidos hasta ahora: {total_new_posts_added}.")
        
    except Exception as e:
        print(f"❌ Error al escribir en el archivo CSV: {e}")

# --- FUNCIÓN PRINCIPAL ---
def main():
    print("🚀 Iniciando el colector de videos de TikTok con lógica de parada por densidad y filtrado por mes.")

    # 1. Calcular el límite de tiempo: el primer segundo del mes actual
    MONTH_LIMIT_TS = get_start_of_current_month_timestamp()
    print(f"🗓️ Se registrarán solo posts creados después de: {datetime.fromtimestamp(MONTH_LIMIT_TS).strftime('%Y-%m-%d %H:%M:%S')}")

    perfiles = []
    try:
        with open("perfiles_tiktok.txt", "r") as f:
            perfiles = [line.strip() for line in f if line.strip()]
        if not perfiles:
            print("El archivo 'perfiles_tiktok.txt' está vacío.")
            return
    except FileNotFoundError:
        print("El archivo 'perfiles_tiktok.txt' no se encontró. Asegúrate de crearlo.")
        return

    existing_timestamps = load_existing_timestamps(OUTPUT_CSV_FILE_TIKTOK)
    print(f"✅ Se cargaron {len(existing_timestamps)} posts existentes para deduplicación.")

    total_new_posts_added = 0
    new_data_batch = []
    
    # -------------------------------------------------------------
    # BUCLE PRINCIPAL DE PERFILES
    # -------------------------------------------------------------
    for i, perfil in enumerate(perfiles):
        print(f"\n--- Procesando perfil {i+1}/{len(perfiles)}: {perfil} ---")
        
        max_cursor = None
        posts_added_in_current_profile = 0
        
        # BUCLE DE PAGINACIÓN
        while True:
            videos_data_json = get_tiktok_videos_page(perfil, max_cursor)
            
            if not videos_data_json: 
                break
                
            posts = videos_data_json.get('aweme_list')
            max_cursor = videos_data_json.get('max_cursor')
            
            if not posts:
                print(f"  > API no devolvió posts en esta página. Finalizando para {perfil}.")
                break

            print(f"  > Procesando lote de {len(posts)} publicaciones de la API. Total añadidos: {total_new_posts_added} (hasta ahora).")
            
            duplicate_count_in_page = 0
            new_timestamps_to_add = set()
            
            # ITERACIÓN DE POSTS
            for video in posts:
                # create_time es el timestamp UNIX (el ID ÚNICO)
                create_time_str = str(video.get('create_time', 'N/A'))
                
                try:
                    create_time_int = int(create_time_str)
                except ValueError:
                    print(f"  ⚠️ Advertencia: Timestamp inválido '{create_time_str}'. Saltando post.")
                    continue
                
                # 🛑 LÓGICA DE FILTRADO TEMPORAL 🛑
                # Si el post es anterior al mes actual, NO lo guardamos ni lo contamos, pero CONTINUAMOS
                if create_time_int < MONTH_LIMIT_TS:
                    # En lugar de detener la paginación, solo descartamos el post y continuamos
                    # Esto permite que los posts anclados no frenen el proceso.
                    continue
                
                # 1. Lógica de Deduplicación
                if create_time_str in existing_timestamps:
                    duplicate_count_in_page += 1
                    continue # Saltar post duplicado
                
                # --- Lógica de procesamiento de Post NUEVO Y RECIENTE ---
                stats = video.get('statistics', {})
                
                # Formatear el timestamp a fecha legible 
                readable_date_str = datetime.fromtimestamp(create_time_int).strftime('%Y-%m-%d %H:%M:%S')

                post_data = {
                    'profile_handle': perfil,
                    'video_id': video.get('aweme_id', 'N/A'),
                    'description': video.get('desc', 'N/A').replace('\n', ' ').replace('\r', ''),
                    'create_time': create_time_str, 
                    'readable_date': readable_date_str, 
                    'url': video.get('share_info', {}).get('share_url', video.get('url', 'N/A')), 
                    
                    'play_count': stats.get('play_count', 0),
                    'digg_count': stats.get('digg_count', 0),
                    'comment_count': stats.get('comment_count', 0),
                    'share_count': stats.get('share_count', 0),
                    'collect_count': stats.get('collect_count', 0),
                    'download_count': stats.get('download_count', 0),
                    'repost_count': stats.get('repost_count', 0),
                    'whatsapp_share_count': stats.get('whatsapp_share_count', 0),
                    'transcript': 'N/A' 
                }
                
                # 2. Almacenamiento y Contabilidad
                new_data_batch.append(post_data)
                total_new_posts_added += 1
                posts_added_in_current_profile += 1
                new_timestamps_to_add.add(create_time_str)

                # 3. Guardado Incremental (BATCH_SIZE)
                if len(new_data_batch) >= BATCH_SIZE:
                    save_batch_to_csv_tiktok(new_data_batch, OUTPUT_CSV_FILE_TIKTOK, total_new_posts_added)
                    existing_timestamps.update(new_timestamps_to_add) 
                    new_data_batch = [] 
                    new_timestamps_to_add = set()
            
            # --- MANEJO DE PARADA POR DENSIDAD (DUPLICATE_THRESHOLD) ---
            should_break = False

            if duplicate_count_in_page >= DUPLICATE_THRESHOLD:
                print(f"  🛑 ALERTA DE PARADA: Se encontraron {duplicate_count_in_page} posts duplicados en esta página de {len(posts)} posts. Deteniendo búsqueda para {perfil}.")
                should_break = True
            
            # 4. Consolidar y salir si es necesario
            if should_break or not max_cursor or videos_data_json.get('has_more') == 0:
                # Guardar el lote restante (si lo hay)
                if new_data_batch:
                   save_batch_to_csv_tiktok(new_data_batch, OUTPUT_CSV_FILE_TIKTOK, total_new_posts_added)
                   # Marcar estos últimos posts nuevos como vistos
                   existing_timestamps.update(new_timestamps_to_add)
                   new_data_batch = []
                
                if should_break:
                    break # Sale del bucle WHILE de paginación
            
            # Si no hubo parada, consolidamos los nuevos posts en el set principal
            existing_timestamps.update(new_timestamps_to_add) 

            # Pausa entre paginaciones
            time.sleep(2)
            
        print(f"  ✅ Perfil {perfil} procesado. Posts nuevos añadidos en este perfil: {posts_added_in_current_profile}.")
        time.sleep(5) # Pausa más larga entre perfiles
        
    print(f"\n🎉 PROCESO COMPLETADO. Total de posts únicos añadidos: {total_new_posts_added}.")

if __name__ == "__main__":
    main()
