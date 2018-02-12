# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 15:22:22 2018

@author: kry
"""
import argparse
import psycopg2
from sqlalchemy import create_engine
from utils.functions import calcula

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

    calcula(con, engine, dbase, host, usr, pwd, cultivo, acopios, beneficio,
            tostadora, exportadora, costo)
