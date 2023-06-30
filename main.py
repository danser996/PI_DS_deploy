# importacion de modelos requeridos
from fastapi import FastAPI
import pandas as pd
from model_SR import rec_ppal

# # cargamos base de datos como dataframe
global data, data_sr
# csv = "data_API.zip"
# data = pd.read_csv(csv, index_col=0)
data = pd.read_parquet('data_API.parquet')
data_sr = pd.read_parquet('data_SR.parquet')
# # creamos un objeto del tipo fastAPI
app = FastAPI()

# # ## funciones API proyecto individual
@app.get('/cantidad_filmaciones_mes/{mes}')
def cantidad_filmaciones_mes(mes:str = 'enero'):
    '''Se ingresa el mes y la funcion retorna la cantidad de peliculas 
    que se estrenaron ese mes historicamente'''
    df = data[['id', 'release_month']]
    df = df[df['release_month'] == mes]
    df.drop_duplicates(inplace=True)
    cantidad_movies_mes = df.id.unique().size
    return {'mes':mes, 
            'cantidad': cantidad_movies_mes}

@app.get('/cantidad_filmaciones_dia/{dia}')
def cantidad_filmaciones_dia(dia:str = 'lunes'):
    '''Se ingresa el dia y la funcion retorna la cantidad de 
    peliculas que se estrenaron ese dia historicamente'''
    df = data[['id', 'release_day']]
    df.replace({'miércoles':'miercoles', 'sábado':'sabado'}, inplace=True)
    df = df[df.release_day == dia]
    df.drop_duplicates(inplace=True)
    cantidad_peliculas_dia = df.id.unique().size
    return {'dia':dia, 
            'cantidad':cantidad_peliculas_dia}

@app.get('/score_titulo/{titulo}')
def score_titulo(titulo:str = 'Toy Story'):
    '''Se ingresa el título de una filmación esperando como respuesta 
    el título, el año de estreno y el score'''
    df = data[['title', 'popularity', 'release_year']]
    df = df[df.title == titulo]
    df.drop_duplicates(inplace=True)
    df.reset_index(inplace=True)
    anio = int(df.release_year[0])
    score = float(df.popularity[0])
    return {'titulo':titulo, 
            'anio': anio, 
            'popularidad': score}

@app.get('/votos_titulo/{titulo}')
def votos_titulo(titulo:str = 'Toy Story'):
    '''Se ingresa el título de una filmación esperando como respuesta el título, 
    la cantidad de votos y el valor promedio de las votaciones. 
    La misma variable deberá de contar con al menos 2000 valoraciones, 
    caso contrario, debemos contar con un mensaje avisando que no cumple 
    esta condición y que por ende, no se devuelve ningun valor.'''
    df = data[['title', 'release_year', 'vote_count', 'vote_average']]
    df = df[df.title == titulo]
    df.drop_duplicates(inplace=True)
    df.reset_index(inplace=True)
    cant_voto = int(df.vote_count[0])
    avg_voto = float(df.vote_average[0])
    anio = int(df.release_year[0])
    if cant_voto >= 2000:
        return {'titulo': titulo, 
                'anio': anio, 
                'voto_total': cant_voto, 
                'voto_promedio': avg_voto}
    else:
        mensaje = f'la pelicula {titulo}, no cumple con la condicion de mas de 2000 valoraciones, no se puede retornar ningun valor'
        return {'mensaje': mensaje}

@app.get('/get_actor/{nombre_actor}')
def get_actor(nombre_actor:str):
    '''Se ingresa el nombre de un actor que se encuentre dentro de un dataset debiendo 
    devolver el éxito del mismo medido a través del retorno. 
    Además, la cantidad de películas que en las que ha participado y el promedio de retorno'''
    df = data[['id', 'name_actor',  'return']]
    df = df[df.name_actor == nombre_actor]
    df.drop_duplicates(inplace=True)
    retorno_total = df['return'].sum()
    cantidad_peliculas = df.id.unique().size
    promedio_retorno = retorno_total/cantidad_peliculas
    return {'actor':nombre_actor, 
            'cantidad_filmaciones': cantidad_peliculas, 
            'retorno_total': retorno_total, 
            'retorno_promedio': promedio_retorno}

@app.get('/get_director/{nombre_director}')
def get_director(nombre_director:str = 'John Lasseter'):
    ''' Se ingresa el nombre de un director que se encuentre dentro de un dataset 
    debiendo devolver el éxito del mismo medido a través del retorno. 
    Además, deberá devolver el nombre de cada película con la fecha de lanzamiento, 
    retorno individual, costo y ganancia de la misma.'''
    df = data[['title', 'release_date', 'name_director', 'return', 'budget', 'revenue']]
    df = df[df.name_director == nombre_director]
    df.drop_duplicates(inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['index', 'name_director'], inplace=True)
    retorno_total = df['return'].sum()
    lista = [df.iloc[i].to_dict() for i in range(df.shape[0])]
    return {'director': nombre_director, 
            'retorno_total_director': retorno_total, 
            'peliculas': lista}

# ML
@app.get('/recomendacion/{titulo}')
def recomendacion(titulo:str = 'Boomerang'):
    '''Ingresas un nombre de pelicula y te recomienda las similares en una lista'''
    titulo = titulo.lower()
    rec = rec_ppal(data=data_sr,movie=titulo)
    rec
    return {'lista recomendada': rec}