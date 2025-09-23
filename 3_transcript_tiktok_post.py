# main.py
import requests
import csv
import pandas as pd
import time
import os
import re

# Importar la clave de la API desde config.py
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
    """
    input_filename = "base_de_datos_tiktok.csv"
    output_filename = "base_de_datos_tiktok.csv"
    
    if not os.path.exists(input_filename):
        print(f"El archivo '{input_filename}' no se encontró. Asegúrate de haber ejecutado el script anterior.")
        return

    # Leer el archivo CSV en un DataFrame de Pandas para un manejo más sencillo
    try:
        df = pd.read_csv(input_filename)
    except Exception as e:
        print(f"❌ Error al leer el archivo CSV: {e}")
        return
    
    # Agregar la columna 'transcript' si no existe
    if 'transcript' not in df.columns:
        df['transcript'] = ''

    # Recorrer cada fila del DataFrame y obtener la transcripción
    posts_processed = 0
    for index, row in df.iterrows():
        video_url = row.get('url')
        
        # Saltar si la URL no es válida o si ya tiene una transcripción (para reanudar)
        if pd.isna(video_url) or pd.notna(row['transcript']) and row['transcript'] != '':
            continue
        
        print(f"Procesando publicación {index + 1}/{len(df)}: {video_url}")
        
        # Obtener la transcripción y limpiar el texto
        transcript = get_tiktok_transcript(video_url, SCRAPE_API_KEY, lang='es')
        
        if transcript:
            df.at[index, 'transcript'] = transcript
            posts_processed += 1
            print(f"✔️ Transcripción obtenida para la URL de perfil: {row.get('profile_handle')}.")
        else:
            print(f"❌ No se pudo obtener la transcripción para la URL: {video_url}.")

        # Guardar en el CSV cada 10 publicaciones procesadas
        if posts_processed >= 10:
            df.to_csv(output_filename, index=False, quoting=csv.QUOTE_ALL)
            print(f"\n💾 Avance guardado. Se actualizaron {posts_processed} registros.")
            posts_processed = 0 # Reiniciar el contador

        # Pequeña pausa para no sobrecargar la API
        time.sleep(1)

    # Guardar los datos restantes al final del proceso
    if posts_processed > 0:
        df.to_csv(output_filename, index=False, quoting=csv.QUOTE_ALL)
        print(f"\n💾 Avance guardado. Se actualizaron los últimos {posts_processed} registros.")

    print("\n🎉 Proceso de extracción de transcripciones completado. El archivo CSV ha sido actualizado.")

if __name__ == "__main__":
    main()
