# main.py
import requests
import csv
from datetime import datetime
import time
import os

# Importar la clave de la API desde config.py
from config import SCRAPE_API_KEY

def get_tiktok_profile_videos(handle: str, api_key: str, amount: int = 20) -> list or None:
    """
    Realiza una llamada a la API de ScrapeCreators para obtener los videos de un perfil de TikTok.
    """
    url = "https://api.scrapecreators.com/v3/tiktok/profile-videos"
    headers = {
        "x-api-key": api_key,
    }
    params = {
        "handle": handle,
        "amount": amount
    }

    try:
        print(f"Buscando {amount} publicaciones para el perfil: {handle}...")
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()  # Lanza una excepción para errores HTTP (4xx o 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al conectar con la API para el perfil {handle}: {e}")
        return None
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al obtener los videos de {handle}: {e}")
        return None

def save_to_csv_incremental(data: list, file_name: str, mode='a', write_header=False):
    """
    Guarda los datos en un archivo CSV de forma incremental.
    """
    try:
        with open(file_name, mode, newline='', encoding='utf-8') as f:
            fieldnames = [
                'profile_handle',
                'video_id',
                'description',
                'create_time',
                'url',
                'play_count',
                'digg_count',
                'comment_count',
                'share_count',
                'collect_count',
                'download_count',
                'repost_count',
                'whatsapp_share_count'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if write_header:
                writer.writeheader()

            for row in data:
                writer.writerow(row)
        print(f"✅ Se guardaron {len(data)} publicaciones en '{file_name}'.")
    except Exception as e:
        print(f"❌ Error al escribir en el archivo CSV: {e}")

def main():
    """
    Función principal para iterar por perfiles y extraer datos de publicaciones.
    """
    
    # ------------------
    # VARIABLES DE CONFIGURACIÓN
    # ------------------
    # Puedes modificar esta variable para cambiar la cantidad de posts a recuperar
    POSTS_TO_FETCH_PER_PROFILE = 20 
    # Cantidad de publicaciones a guardar antes de escribir en el archivo
    SAVE_BATCH_SIZE = 10 
    # Nombre del archivo de salida
    OUTPUT_FILENAME = "base_de_datos_tiktok.csv" 
    # ------------------

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

    # Si el archivo de salida ya existe, lo eliminamos para empezar de cero
    if os.path.exists(OUTPUT_FILENAME):
        os.remove(OUTPUT_FILENAME)

    all_posts = []
    
    for i, perfil in enumerate(perfiles):
        print(f"\nProcesando perfil {i+1}/{len(perfiles)}: {perfil}")
        videos_data = get_tiktok_profile_videos(perfil, SCRAPE_API_KEY, amount=POSTS_TO_FETCH_PER_PROFILE)
        
        if videos_data:
            for video in videos_data:
                try:
                    stats = video.get('statistics', {})
                    post_data = {
                        'profile_handle': perfil,
                        'video_id': video.get('aweme_id', 'N/A'),
                        'description': video.get('desc', 'N/A').replace('\n', ' ').replace('\r', ''),
                        'create_time': datetime.fromtimestamp(video.get('create_time', 0)).strftime('%Y-%m-%d %H:%M:%S') if video.get('create_time') else 'N/A',
                        'url': video.get('share_url', 'N/A'),
                        'play_count': stats.get('play_count', 0),
                        'digg_count': stats.get('digg_count', 0),
                        'comment_count': stats.get('comment_count', 0),
                        'share_count': stats.get('share_count', 0),
                        'collect_count': stats.get('collect_count', 0),
                        'download_count': stats.get('download_count', 0),
                        'repost_count': stats.get('repost_count', 0),
                        'whatsapp_share_count': stats.get('whatsapp_share_count', 0)
                    }
                    all_posts.append(post_data)

                    # Escribir en el CSV cada 10 publicaciones
                    if len(all_posts) >= SAVE_BATCH_SIZE:
                        write_header = not os.path.exists(OUTPUT_FILENAME) or os.path.getsize(OUTPUT_FILENAME) == 0
                        save_to_csv_incremental(all_posts, OUTPUT_FILENAME, write_header=write_header)
                        all_posts = [] # Limpiamos la lista para la siguiente tanda
                
                except Exception as e:
                    print(f"❌ Error procesando un video del perfil {perfil}: {e}")
                    continue

        # Pequeña pausa para evitar sobrecargar la API entre perfiles
        time.sleep(2)
    
    # Escribir los datos restantes al final del proceso
    if all_posts:
        write_header = not os.path.exists(OUTPUT_FILENAME) or os.path.getsize(OUTPUT_FILENAME) == 0
        save_to_csv_incremental(all_posts, OUTPUT_FILENAME, write_header=write_header)

    print("\n🎉 Proceso de extracción de publicaciones completado.")

if __name__ == "__main__":
    main()
