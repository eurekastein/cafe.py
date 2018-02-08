# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 15:22:22 2018

@author: kry
"""
import argparse
from subprocess import call
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import create_engine

coords = [(497450,1818108), (641043,1825712)]
con = psycopg2.connect(database='cafe', user='postgres',
                           password='postgres', host='192.168.18.22')

#Falta asignar los nodos a la red y cfrom psycopg2 import sqlrear la topología desde aquí =( 

def sql_costo(tabla_origen, tabla_destino, columna_costo):
  params = {'origen': tabla_origen, 'destino': tabla_destino, 
            'costo': columna_costo}
  sql = """SELECT DISTINCT ON (start_vid) 
         start_vid, end_vid, agg_cost
  FROM   (SELECT * FROM pgr_dijkstraCost(
      'select id, source, target, %(costo)s as cost from red',
      array(select distinct(nodos_%(origen)s) from %(origen)s),
      array(select distinct(nodos_%(destino)s) from %(destino)s),
  	 directed:=false)
  ) as sub
  ORDER  BY start_vid, agg_cost asc""" % params
  return sql


def agrega_nodos(coords, tipo, engine, con):
    """Toma un punto (de algún tipo) from psycopg2 import sqly lo enchufa en la tabla que
        le toque, asignando el nodo de la red mas cercano.
    """
    tipos = ('cultivo', 'acopios', 'beneficio', 'tostadora', 'exportadora')
    if tipo not in tipos: 
        return 'Error tu tipo no existe'
    
    for i, coordenada in enumerate(coords):
        # cambiar las coordenaDAS de acuerdo al elemento de la lista
        print("El punto %s es %s" % (i, coordenada))
        params = {'tipo': tipo, 'coordx': coordenada[0], 'coordy':coordenada[1]} 
        sql = """SELECT  id as nodos_%(tipo)s
           FROM red_vertices_pgrfrom psycopg2 import sql
           ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(%(coordx)d, %(coordy)d),
           32615)
           LIMIT 1
              """ % params
        wkt_str = 'MULTIPOINT('+ str(coordenada[0]) + ' ' + str(coordenada[1]) + ')'
        wkt_element = WKTElement(wkt_str, srid=32615)
        resultado = pd.read_sql_query(sql, con)
        resultado['geom'] = wkt_element
        resultado = gpd.GeoDataFrame(resultado)
        resultado.to_sql(tipo, engine, if_exists='append', 
                         dtype={'geom': Geometry('MULTIPOINT', srid= 32615)},
                         index=False)

def calcula( con, engine, dbase='cafe', host='192.168.18.22', usr='postgres', 
            pwd='postgres', cultivo='cultivo', acopios='acopios', 
            beneficio='beneficio', tostadora='tostadora', 
            exportadora='exportadora', costo='costo'): 
    
    print("empezando.......")
    #Conexion a postgres
    dist_cultivo_almacen = pd.read_sql_query(sql_costo(cultivo, 
                                                           acopios, 
                                                           costo), con)
    
    dist_acopios_beneficio = pd.read_sql_query(sql_costo(acopios, 
                                                         beneficio, 
                                                         costo), con)
    dist_beneficio_tostadora = pd.read_sql_query(sql_costo(beneficio, 
                                                               tostadora, 
                                                               costo), con)
    dist_tostadora_exportadora = pd.read_sql_query(sql_costo(
                                                                tostadora,
                                                                exportadora,
                                                                costo), con)
    
    
    p1 = dist_cultivo_almacen.merge(dist_acopios_beneficio, left_on='end_vid', 
                                    right_on='start_vid')
    p1.columns = ['id_cultivo', 'id_acopios', 'costo_c_a', 'id_acopios_y', 
                  'id_beneficio', 'costo_a_b']
    p2 = p1.merge(dist_beneficio_tostadora, left_on='id_beneficio', 
                  right_on='start_vid')
    #cambia los nombres a las columnafrom psycopg2 import sqls 
    l = list(p2.columns)
    l[-3:] = ['id_beneficio','id_tostadora','costo_b_t']
    p2.columns = l
    
    resultado = p2.merge(dist_tostadora_exportadora, left_on='id_tostadora', 
                         right_on='start_vid')
    rel_cultivo_acopio = resultado[['id_cultivo', 'id_acopios']]
        
    l = list(resultado.columns)
    l[-3:] = ['id_tostadora','id_exportadora','costo_t_e']
    resultado.columns = l
    
    resultado = resultado.set_index('id_cultivo')
    
    
    #asi selecciono los nulos:
    #nulos = p1[p1['start_vid_y'].isnull()]
    
    #costos = []
    #for c in list(resultado.columns):
    #  if c.startswith('agg_cost'):
    #    costos.append(c)
    
    # Calcular la suma de los costos
    columnas_costo = [c for c in list(resultado.columns) 
                      if c.startswith('costo')]
    costos = resultado[columnas_costo].sum(axis=1)  
    
    # Traer de la BD la tabla de cultivos
    query_cultivos = "select * from cultivo"
    cultivo = gpd.GeoDataFrame.from_postgis(query_cultivos, con, 
                                            crs={'init':'epsg:32615'})
    # Unir los nodos cultivos a los ids de cultivo originales:
    
    merge_cultivo = rel_cultivo_acopio.merge(cultivo, left_on='id_cultivo', 
                  right_on='nodos_cultivo')
    
    merge_cultivo = merge_cultivo[['id_cultivo', 'id_acopios', 'id','geom']]
    
    merge_cultivo.columns = ['nodo_cultivo','nodo_acopio','id_cultivo','geom']
    
    
    # Unir los costos a la tabla de cultivos
    costos = pd.DataFrame(costos)
    costos.columns = ['costo_total']
    cultivo_costo = cultivo.merge(costos, right_index=True, left_on='nodos_cultivo')
    
    # Guardar como shape para gdal
    cultivo_costo.to_file('costo.shp')
    
    # Guardar el resultado en la base datos
    engine = create_engine('postgresql://postgres:postgres@192.168.18.22:5432/cafe')
    cultivo_costo['geom'] = cultivo_costo.geometry.apply(
            lambda x: WKTElement(x.wkt, srid=32615))
    #cultivo.drop('geometry', 1, inplace=True)
    cultivo_costo.to_sql('resultado', engine, if_exists='replace', index=False,
                    dtype={'geom': Geometry('MULTIPOINT', srid= 32615)})
    #guardar merge_cultivo como tabla 
    merge_cultivo = gpd.GeoDataFrame(merge_cultivo).set_geometry('geom')
    merge_cultivo['geom'] = merge_cultivo.geometry.apply(
            lambda x: WKTElement(x.wkt, srid=32615))   
    merge_cultivo.to_sql('cultivo_acopios', engine, if_exists='replace', index=False,
                    dtype={'geom': Geometry('MULTIPOINT', srid= 32615)})
    #Hacer poligono para cada acopio
   
    for i, acopio in enumerate(merge_cultivo.nodo_acopio.unique()):
        s = """insert into poligono_cultivos 
        select %s as id_acopio , sub.geom
        from
        (select * from st_setsrid(pgr_pointsAsPolygon('select nodo_acopio::int4 as id, st_x 
        (st_geometryn(geom,1)) as x,  st_y  (st_geometryn(geom,1)) as y 
         from cultivo_acopios where nodo_acopio= %s'),32615)as geom)as sub""" % (acopio, acopio)
        print(i)
        cur = con.cursor()
        try:
            cur.execute(s)
            con.commit()
            print('pude')
        except Exception as e:
            con.commit()
            print(e)
            pass
        
    # Agarrar el shape y convertirlo a raster con gdal, la instrucción call se 
    #utiliza para  traer un comando y ejecutarlo. 
    
    comando = ['gdal_grid', '-zfield', 'costo_tota', '-l', 'costo', '-a', 
    'invdist:power=2.0:smothing=0.0:radius1=0.0:radius2=0.0:angle=0.0:max_points=0:min_points=0:nodata=0.0',
     '-of', 'GTiff', 'costo.shp', 
     'costo.tif']
    try:
      call(comando)
    except:
        print('valió verga')
    print("Yastuvo carnalitooooo, tienes tus lindas redes")
    
    return resultado


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Está bien vergas')
    parser.add_argument('--database', help="la base de datos",
                        default="cafe")
    parser.add_argument('--host', help="el servidor",
                        default="192.168.18.22")
    parser.add_argument('--user', help="usuario base de datos",
                        default="postgres")   
    parser.add_argument('--pwd', help="contraseña base de datos",
                        default="postgres")
    parser.add_argument('--cultivo', help="tabla de puntos de cultivo",
                        default="cultivo")
    parser.add_argument('--acopios', help="tabla de puntos de acopios",
                        default="acopios")   
    parser.add_argument('--beneficio', help="tabla de puntos de beneficio",
                        default="beneficio")
    parser.add_argument('--tostadora', help="tabla de puntos de tostadora",
                        default="tostadora")   
    parser.add_argument('--exportadora', help="tabla de puntos de exportadora",
                        default="exportadora")
    parser.add_argument('--costo', help="columna de costo",
                        default="costo")    
    args = parser.parse_args()

    dbase = args.database
    host = args.host
    usr = args.user
    pwd = args.pwd
    cultivo = args.cultivo 
    acopios = args.acopios
    beneficio = args.beneficio
    tostadora = args.tostadora
    exportadora = args.exportadora
    costo = args.costo
    con = psycopg2.connect(database=dbase, user=usr, password=pwd, host=host)
    engine_str = 'postgresql://%s:%s@%s:5432/%s' % (usr, pwd, host, dbase)
    engine = create_engine(engine_str)       

    calcula(con, engine, dbase, host, usr, pwd, cultivo, acopios, beneficio, tostadora, 
            exportadora, costo)   
 
    
    