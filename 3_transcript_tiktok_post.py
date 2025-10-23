import requests
import csv
import pandas as pd
import time
import os
import re

# Importar la clave de la API desde config.py
# Asegúrate de que esta línea esté correcta en tu entorno:
from config import SCRAPE_API_KEY

def get_tiktok_transcript(video_url: str, api_key: str, lang: str = 'es') -> str or None:
    """
    Realiza una llamada a la API de ScrapeCreators para obtener la transcripción de un video.
    """
    url = "https://api.scrapecreators.com/v1/tiktok/video/transcript"
    headers = {
        "x-api-key": api_key,
    }
    params = {
        "url": video_url,
        "language": lang
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # Extraer el texto legible de la transcripción
        transcript_text = data.get('transcript', '')
        
        # Si la transcripción es una cadena vacía o no válida
        if not transcript_text or transcript_text.lower() in ('n/a', 'none', 'error'):
            return "TRANSCRIPCION_NO_DISPONIBLE" # Usar una marca de error específica

        # Limpiar el texto: remover timestamps y otros metadatos
        clean_text = re.sub(r'(\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}\n)', '', transcript_text)
        clean_text = re.sub(r'WEBVTT\n\n', '', clean_text)
        clean_text = clean_text.replace('\n', ' ').strip()
        
        return clean_text
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al conectar con la API para la URL {video_url}: {e}")
        return None
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al procesar la transcripción: {e}")
        return None

def main():
    """
    Función principal para leer el CSV, obtener transcripciones y actualizar el archivo.
    Busca específicamente 'N/A' en la columna 'transcript' para reanudar el trabajo.
    """
    input_filename = "base_de_datos_tiktok.csv"
    output_filename = "base_de_datos_tiktok.csv"
    
    # Tamaño del lote para el guardado incremental
    BATCH_SIZE = 5 
    
    if not os.path.exists(input_filename):
        print(f"El archivo '{input_filename}' no se encontró. Asegúrate de haber ejecutado el script anterior.")
        return

    # Leer el archivo CSV
    try:
        # Forzar la lectura de 'transcript' como string para comparar con "N/A"
        df = pd.read_csv(input_filename, dtype={'transcript': str}) 
    except Exception as e:
        print(f"❌ Error al leer el archivo CSV: {e}")
        return
    
    # 📝 IMPORTANTE: Asegurarse de que la columna exista y que los N/A sean 'N/A' si Pandas los convierte a NaN.
    if 'transcript' not in df.columns:
        df['transcript'] = 'N/A'
    # Rellenar cualquier valor NaN (valor nulo de Pandas) con "N/A" para el filtro
    df['transcript'] = df['transcript'].fillna('N/A') 

    print(f"✅ Archivo cargado. Total de publicaciones: {len(df)}.")

    # Crear la máscara de filtro: buscar filas donde 'transcript' sea 'N/A'
    posts_to_process = df[df['transcript'] == 'N/A']
    
    if posts_to_process.empty:
        print("🎉 Todas las publicaciones ya tienen transcripción (o no tienen 'N/A'). Proceso finalizado.")
        return
    
    print(f"⏳ Se encontraron {len(posts_to_process)} publicaciones pendientes de transcripción.")

    # Recorrer solo las filas filtradas
    posts_processed = 0
    total_posts_processed_session = 0
    
    # Usamos .index para iterar sobre los índices originales del DataFrame
    for index in posts_to_process.index:
        row = df.loc[index]
        video_url = row.get('url')
        
        # Doble verificación: si la URL falta, marcamos como error y continuamos
        if pd.isna(video_url) or video_url.strip() == '':
             df.loc[index, 'transcript'] = "URL_NO_VALIDA"
             continue

        print(f"Procesando fila (original): {index + 1}/{len(df)} | URL: {video_url[:50]}...")
        
        # Obtener la transcripción
        transcript = get_tiktok_transcript(video_url, SCRAPE_API_KEY, lang='es')
        
        if transcript:
            # Si hubo éxito, guardamos la transcripción
            df.loc[index, 'transcript'] = transcript
            print(f"✔️ Transcripción obtenida (Tamaño: {len(transcript)} caracteres).")
        else:
            # Si falló (Error de API, conexión, etc.), guardamos la marca de error para no reintentar de inmediato
            df.loc[index, 'transcript'] = "FALLO_API_REINTENTAR"
            print(f"❌ Fallo al obtener transcripción para la URL: {video_url}.")


        posts_processed += 1
        total_posts_processed_session += 1
        
        # 💾 Guardado Incremental 💾
        if total_posts_processed_session % BATCH_SIZE == 0:
            df.to_csv(output_filename, index=False, quoting=csv.QUOTE_ALL)
            print(f"\n💾 Avance guardado. Se actualizaron {BATCH_SIZE} registros. Total en sesión: {total_posts_processed_session}.")
            
        
        # Pequeña pausa para no sobrecargar la API
        time.sleep(1)

    # Guardar los datos restantes al final del proceso
    if total_posts_processed_session % BATCH_SIZE != 0:
        df.to_csv(output_filename, index=False, quoting=csv.QUOTE_ALL)
        print(f"\n💾 Proceso finalizado. Se actualizaron los últimos {total_posts_processed_session % BATCH_SIZE} registros. Total en sesión: {total_posts_processed_session}.")
    else:
        # Caso donde el último guardado coincidió con el BATCH_SIZE
        print(f"\n🎉 Proceso de extracción de transcripciones completado. Total de transcripciones intentadas: {total_posts_processed_session}.")

if __name__ == "__main__":
    main()
