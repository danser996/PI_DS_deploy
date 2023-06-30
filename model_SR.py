import pandas as pd
import string
import warnings
warnings.filterwarnings('ignore')
# importamos el modulo de sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

def recomendacion(df, indices, title, cosine_sim):
    """Esta funcion recibe el titulo de una pelicula y retorna una recomendacion 
    de 5 peliculas similares de acuerdo a la similitud que tengan en actores,
    generos, nombre de pelicula y reseña(overview)"""
    idx = indices[title]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key= lambda x:x[1], reverse=True)
    sim_scores = sim_scores[1:6]
    movies_idx = [i[0] for i in sim_scores]
    return df['title'].iloc[movies_idx]

def rec_ppal(movie:str = 'toy story'):
    """Esta funcion contiene todo el cuerpo del procedimiento que se realiza para hacer el sistema 
    de recomendacion, dentro de este se realiza la vectorizacion de las palabras, y se hace un tratamiento
    de esta, invoca la funcion de recomendacion y finalmente retorna una lista con las 5 peliculas
    recomendadas"""
    # importamos el dataset que contiene el titilo y el tag que utilizaremos para el calculo de la matrix
    # y usar la similitud coseno
    # data = pd.read_csv('data_SR.zip', index_col=0)
    data = pd.read_parquet('data_SR.parquet')
    df = data[['title', 'tags']]
    # df = df.sample(frac=0.05)
    df = df.iloc[0:5000]
    df.reset_index(drop=True, inplace=True)
    df['tags'] = df['tags'].str.replace('[{}]'.format(string.punctuation), ' ')
    # Usamos el vectorizer de sklearn para calcular la frecuencia de las palabras que 
    # aparecen en nuestro tag y el parametro stop words para descartar todas las 
    # pablabras comunes del idioma ingles que no aportar valor a mi modelo
    tfidf = TfidfVectorizer(stop_words='english')
    df['tags'].fillna('', inplace=True) # reemplazamos los valores nulos del dataframe por un vacio
    tfidf_matrix = tfidf.fit_transform(df['tags']) # creamos la matriz donde estaran las palabras de cada tag y su frecuencia
    tfidf.vocabulary_ # imprimimos el vocabulario encontrado y su frecuencia a lo largo del dataframe
    """calcula el nucleo lineal entre los parametros recibidos y 
    me sirve para comparar al llamar una pelicula se compara con 
    todas las otras peliculas y selecciona la que tiene mas relacion"""
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix) 
    # Creamos una serie con el title de las peliculas
    indices = pd.Series(df.index, index=df['title']).drop_duplicates()
    rec = recomendacion(df=df, indices=indices, title=movie, cosine_sim=cosine_sim)
    rec = list(rec.to_dict().values())
    return rec