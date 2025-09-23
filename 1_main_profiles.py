# main.py
import requests
import csv
from datetime import datetime
import time
import os

# Importar las claves de la API desde config.py
from config import SCRAPE_API_KEY

def get_tiktok_profile_data(handle: str, api_key: str) -> dict or None:
    """
    Realiza una llamada a la API de ScrapeCreators para obtener los datos de un perfil de TikTok.
    """
    url = "https://api.scrapecreators.com/v1/tiktok/profile"
    headers = {
        "x-api-key": api_key,
    }
    params = {
        "handle": handle
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Lanza una excepción para errores HTTP (4xx o 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la API para el perfil {handle}: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP para el perfil {handle}: {e}")
        if response.status_code == 404:
            print("Perfil no encontrado o URL incorrecta.")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado para el perfil {handle}: {e}")
        return None

def main():
    """
    Función principal para leer perfiles, obtener datos y exportar a CSV.
    """
    output_filename = f"consolidado_perfiles_tiktok_{datetime.now().strftime('%Y%m%d')}.csv"
    perfiles = []
    perfiles_encontrados = 0

    # 1. Leer los perfiles desde el archivo
    try:
        with open("perfiles_tiktok.txt", "r") as f:
            perfiles = [line.strip() for line in f if line.strip()]
        if not perfiles:
            print("El archivo 'perfiles_tiktok.txt' está vacío. Por favor, agrega perfiles.")
            return
    except FileNotFoundError:
        print("El archivo 'perfiles_tiktok.txt' no se encontró.")
        return

    # 2. Recolectar la información de cada perfil
    datos_consolidados = []
    for i, perfil in enumerate(perfiles):
        print(f"Recolectando datos para el perfil: {perfil}...")
        data = get_tiktok_profile_data(perfil, SCRAPE_API_KEY)
        
        if data and data.get('user') and data.get('stats'):
            user_data = data['user']
            stats_data = data['stats']
            
            # Datos principales requeridos
            perfil_info = {
                'nickname': user_data.get('nickname', 'N/A'),
                'uniqueId': user_data.get('uniqueId', 'N/A'),
                'followerCount': stats_data.get('followerCount', 0),
                'heartCount': stats_data.get('heartCount', 0),
                'videoCount': stats_data.get('videoCount', 0),
                'followingCount': stats_data.get('followingCount', 0),
                'friendCount': stats_data.get('friendCount', 0)
            }
            
            # Datos adicionales para seguimiento propuestos
            perfil_info['bioLink'] = user_data.get('bioLink', {}).get('link', 'N/A')
            perfil_info['signature'] = user_data.get('signature', 'N/A')
            perfil_info['isVerified'] = user_data.get('verified', 'N/A')
            perfil_info['privateAccount'] = user_data.get('privateAccount', 'N/A')
            perfil_info['diggCount'] = stats_data.get('diggCount', 0)
            perfil_info['lastUpdated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            datos_consolidados.append(perfil_info)
            perfiles_encontrados += 1
            print(f"✔️ Datos de {perfil} recolectados exitosamente.")
        else:
            print(f"❌ No se pudieron obtener datos completos para el perfil {perfil}.")
            
        # Espera para evitar sobrecargar la API y manejar límites de velocidad
        if i < len(perfiles) - 1:
            time.sleep(2)  # Pausa de 2 segundos entre cada llamada

    if not datos_consolidados:
        print("No se encontraron datos de perfiles válidos para exportar.")
        return

    # 3. Exportar la información a un archivo CSV
    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = datos_consolidados[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(datos_consolidados)
        
    print(f"\n✅ Proceso completado. Se exportaron los datos de {perfiles_encontrados} perfiles al archivo '{output_filename}'.")
    print("Columnas adicionales incluidas para un mejor seguimiento: bioLink, signature, isVerified, privateAccount, diggCount, y lastUpdated.")

if __name__ == "__main__":
    main()
