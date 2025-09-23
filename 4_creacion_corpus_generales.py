# main.py
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os
import re
from collections import defaultdict

def get_spanish_stopwords() -> set:
    """
    Retorna un conjunto con stopwords comunes del idioma español.
    """
    return {
        'de', 'la', 'que', 'el', 'e', 'en', 'y', 'a', 'los', 'del', 'se', 'las', 'por', 'un',
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
    # Eliminar URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    # Eliminar menciones (@usuario) y hashtags
    text = re.sub(r'@\w+|#\w+', '', text)
    # Eliminar caracteres especiales y números
    text = re.sub(r'[^a-zA-ZáéíóúüñÁÉÍÓÚÜÑ\s]', '', text, re.I|re.A)
    # Convertir a minúsculas
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
    Función principal para procesar el texto y generar la nube de palabras.
    """
    input_filename = "base_de_datos_tiktok.csv"
    output_folder = "discurso_perfiles"

    if not os.path.exists(input_filename):
        print(f"El archivo '{input_filename}' no se encontró. Asegúrate de haberlo creado.")
        return

    # 1. Crear la carpeta de salida
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Carpeta '{output_folder}' creada exitosamente.")

    # 2. Cargar los datos
    try:
        df = pd.read_csv(input_filename, encoding='utf-8')
    except Exception as e:
        print(f"❌ Error al leer el archivo CSV: {e}")
        return

    # Obtener la lista de stopwords
    spanish_stopwords = get_spanish_stopwords()
    
    # 3. Consolidar texto por perfil
    consolidated_texts = defaultdict(str)
    for index, row in df.iterrows():
        perfil = str(row.get('profile_handle', 'desconocido'))
        descripcion = str(row.get('description', ''))
        transcripcion = str(row.get('transcript', ''))
        
        # Concatenar el texto de descripción y transcripción
        texto_completo = descripcion + " " + transcripcion
        consolidated_texts[perfil] += texto_completo + " "

    # 4. Procesar y guardar el texto consolidado y la nube de palabras para cada perfil
    for perfil, texto_crudo in consolidated_texts.items():
        print(f"Procesando datos para el perfil: {perfil}")
        
        # Preprocesamiento del texto
        texto_limpio = clean_text(texto_crudo)
        texto_sin_stopwords = remove_stopwords(texto_limpio, spanish_stopwords)
        
        # a. Guardar el texto en un archivo .txt
        output_txt_path = os.path.join(output_folder, f"{perfil}_corpus.txt")
        try:
            with open(output_txt_path, 'w', encoding='utf-8') as f:
                f.write(texto_sin_stopwords)
            print(f"  ✔️ Archivo de texto guardado en: {output_txt_path}")
        except Exception as e:
            print(f"  ❌ Error al guardar el archivo de texto para {perfil}: {e}")
            continue

        # b. Generar la nube de palabras
        if texto_sin_stopwords:
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(texto_sin_stopwords)
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
            output_png_path = os.path.join(output_folder, f"{perfil}_world_cloud.png")
            try:
                plt.savefig(output_png_path)
                print(f"  ✔️ Nube de palabras guardada en: {output_png_path}")
                plt.close() # Cierra la figura para evitar que se muestren en pantalla
            except Exception as e:
                print(f"  ❌ Error al guardar la nube de palabras para {perfil}: {e}")
        else:
            print(f"  ⚠️ No hay texto suficiente para generar la nube de palabras de {perfil}.")

    print("\n🎉 Proceso de consolidación de texto y generación de nubes de palabras completado.")

if __name__ == "__main__":
    main()
