# main.py
import pandas as pd
import os
import re
from collections import defaultdict

def get_spanish_stopwords() -> set:
    """
    Retorna un conjunto con stopwords comunes del idioma español.
    """
    return {
        'de', 'la', 'que', 'el', 'en', 'e', 'y', 'a', 'los', 'del', 'se', 'las', 'por', 'un',
        'para', 'con', 'no', 'una', 'su', 'al', 'lo', 'como', 'más', 'pero', 'sus',
        'le', 'ya', 'o', 'este', 'ha', 'sí', 'porque', 'esta', 'son', 'entre', 'está',
        'cuando', 'muy', 'sin', 'sobre', 'mi', 'también', 'me', 'donde', 'quien', 'ir',
        'él', 'si', 'dentro', 'desde', 'tanto', 'hacia', 'hasta', 'mientras', 'haciendo',
        'antes', 'ahora', 'aunque', 'casi', 'cuando', 'después', 'durante', 'entonces',
        'hacer', 'ni', 'solo', 'además', 'así', 'vez', 'todo', 'siempre', 'ser', 'estos',
        'estar', 'uno', 'todos', 'cual', 'les', 'nos', 'nuestra', 'nuestros', 'vuestra',
        'vuestros', 'vuestro', 'vuestra', 'vos', 'esos', 'esas', 'aquel', 'aquella',
        'aquello', 'aquellos', 'ellas', 'ellos', 'mis', 'tu', 'tus', 'yo', 'este',
        'eso', 'está', 'fui', 'fue', 'fuimos', 'fueron'
    }

def clean_text(text: str) -> str:
    """
    Limpia el texto, eliminando URLs, menciones, emojis y caracteres especiales.
    """
    if not isinstance(text, str):
        return ""
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'@\w+|#\w+', '', text)
    text = re.sub(r'[^a-zA-ZáéíóúüñÁÉÍÓÚÜÑ\s]', '', text, re.I|re.A)
    text = text.lower()
    return text

def remove_stopwords(text: str, stop_words: set) -> str:
    """
    Elimina las stopwords del texto.
    """
    words = text.split()
    filtered_words = [word for word in words if word not in stop_words]
    return " ".join(filtered_words)

def main():
    """
    Función principal para procesar los datos de engagement y generar los corpus.
    """
    input_filename = "base_de_datos_tiktok.csv"
    output_folder = "discurso_mayor_engagement"

    if not os.path.exists(input_filename):
        print(f"El archivo '{input_filename}' no se encontró. Asegúrate de haberlo creado.")
        return

    # 1. Crear la carpeta de salida si no existe
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Carpeta '{output_folder}' creada exitosamente.")

    # 2. Cargar los datos
    try:
        df = pd.read_csv(input_filename, encoding='utf-8')
    except Exception as e:
        print(f"❌ Error al leer el archivo CSV: {e}")
        return

    # 3. Calcular el engagement para cada publicación
    # Manejar el caso de play_count igual a 0 para evitar ZeroDivisionError
    df['play_count'] = df['play_count'].replace(0, 1)
    df['engagement'] = (df['digg_count'] + df['comment_count']) / df['play_count']

    # 4. Obtener los 5 posts con mayor engagement por perfil
    top_5_posts = df.sort_values('engagement', ascending=False).groupby('profile_handle').head(5)

    # 5. Consolidar el texto y guardar los archivos
    spanish_stopwords = get_spanish_stopwords()
    consolidated_texts = defaultdict(str)

    for index, row in top_5_posts.iterrows():
        perfil = str(row.get('profile_handle', 'desconocido'))
        descripcion = str(row.get('description', ''))
        transcripcion = str(row.get('transcript', ''))
        
        # Concatenar el texto de descripción y transcripción
        texto_completo = descripcion + " " + transcripcion
        
        # Preprocesamiento del texto
        texto_limpio = clean_text(texto_completo)
        texto_sin_stopwords = remove_stopwords(texto_limpio, spanish_stopwords)
        
        consolidated_texts[perfil] += texto_sin_stopwords + " "

    # 6. Guardar el corpus consolidado en archivos individuales
    for perfil, corpus in consolidated_texts.items():
        output_txt_path = os.path.join(output_folder, f"{perfil}_corpus_engagement.txt")
        try:
            with open(output_txt_path, 'w', encoding='utf-8') as f:
                f.write(corpus.strip())
            print(f"  ✔️ Corpus de mayor engagement para '{perfil}' guardado en: {output_txt_path}")
        except Exception as e:
            print(f"  ❌ Error al guardar el archivo de texto para {perfil}: {e}")
            
    print("\n🎉 Proceso de consolidación de corpus de mayor engagement completado.")

if __name__ == "__main__":
    main()
