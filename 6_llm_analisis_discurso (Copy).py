import google.generativeai as genai
import os
import re
from config import GEMINI_API_KEY # Asegúrate de que tu clave API de Gemini esté en el archivo config.py

# ------------------
# VARIABLES DE CONFIGURACIÓN
# ------------------
# Carpeta con los corpus de texto a analizar
INPUT_FOLDER = "discurso_perfiles"
# Carpeta donde se guardarán los resultados del análisis
OUTPUT_FOLDER = "analisis_discurso_llm"
# Modelo a usar
MODEL_NAME = 'gemini-2.5-flash'
# ------------------

def configure_api():
    """Configura la API de Gemini con la clave proporcionada."""
    genai.configure(api_key=GEMINI_API_KEY)

def generate_llm_response(prompt: str) -> str or None:
    """
    Envía el prompt al modelo de Gemini y retorna la respuesta.
    """
    try:
        model = genai.GenerativeModel(MODEL_NAME)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"❌ Error al llamar a la API de Gemini: {e}")
        return None

def main():
    """
    Función principal para iterar sobre los corpus, generar análisis y guardar resultados.
    """
    # 1. Configurar la API de Gemini
    configure_api()

    # 2. Crear la carpeta de salida
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Carpeta '{OUTPUT_FOLDER}' creada exitosamente.")

    # 3. Leer los archivos de corpus de texto
    corpus_files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('_corpus.txt')]

    if not corpus_files:
        print(f"No se encontraron archivos de corpus en la carpeta '{INPUT_FOLDER}'.")
        return

    # 4. Iterar sobre cada archivo y generar el análisis
    for filename in corpus_files:
        profile_name = filename.replace('_corpus.txt', '')
        file_path = os.path.join(INPUT_FOLDER, filename)
        
        print(f"\nAnalizando el discurso del perfil: {profile_name}...")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                corpus_text = f.read()
            
            # 5. Rellenar el prompt con el texto del corpus, manteniendo la versión original
            MASTER_PROMPT = """
            Actúa como un analista político y de comunicación experto. A continuación, te proporcionaré el corpus de texto completo de todas las publicaciones de un candidato presidencial de Colombia en Instagram.

            Tu tarea es leer y analizar profundamente este texto y generar un reporte conciso, de máximo una página en Español, que contenga las siguientes secciones claramente definidas:

            **1. Perfil de Comunicación:**
            Describe en uno o dos párrafos el estilo general de comunicación del candidato. ¿Es formal o informal? ¿Cercano o distante? ¿Usa un lenguaje técnico o popular?

            **2. Temas Principales:**
            Identifica y enumera los 3 a 5 temas más recurrentes en su discurso (ej. Seguridad, Economía, Educación, Corrupción, Medio Ambiente). Proporciona un breve ejemplo de cómo aborda cada tema.

            **3. Tono y Sentimiento Dominante:**
            ¿Cuál es el tono general del discurso? ¿Es optimista, confrontacional, esperanzador, crítico, propositivo?

            **4. Palabras Clave de Poder:**
            Lista las palabras o frases cortas que el candidato repite estratégicamente para enmarcar su mensaje (ej. "cambio real", "mano dura", "justicia social", "futuro", "potencia de la vida").

            **5. Conclusión Estratégica:**
            En un párrafo final, resume la estrategia de comunicación general del candidato. ¿A qué audiencia parece estar hablándole y qué busca evocar con su discurso?


            Aquí está el texto del candidato:

            {corpus_text}
            """
            
            filled_prompt = MASTER_PROMPT.format(corpus_text=corpus_text)
            
            # 6. Obtener la respuesta del LLM
            llm_analysis = generate_llm_response(filled_prompt)

            # 7. Guardar el análisis en un archivo
            if llm_analysis:
                output_path = os.path.join(OUTPUT_FOLDER, f"{profile_name}_analisis_discurso.txt")
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(llm_analysis)
                print(f"  ✔️ Análisis de discurso para '{profile_name}' guardado en: {output_path}")
            else:
                print(f"  ❌ No se pudo generar el análisis para '{profile_name}'.")

        except FileNotFoundError:
            print(f"  ❌ Archivo no encontrado: {file_path}. Saltando al siguiente.")
            continue
        except Exception as e:
            print(f"  ❌ Ocurrió un error inesperado al procesar '{profile_name}': {e}")
            continue

    print("\n🎉 Proceso de análisis de discurso con el LLM completado.")

if __name__ == "__main__":
    main()
