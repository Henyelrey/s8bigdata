"""Genera las tres fuentes Bronze de S04 con el mismo simulador del productor de S10.

100 000 lecturas, una por segundo (INTERVALO_S), divididas como en S04 en:
- campo_electrico.csv        DateTime, CE           (tabla principal)
- campo_magnetico.csv        DateTime, CM           (180 lecturas con codigo de error 99999)
- variables_ambientales.csv  6 variables, DateTime  (cabecera "SolarRad." y DateTime duplicada con nulos)
"""
import csv
import datetime as dt
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "uso-campo-electrico", "app"))
sys.path.insert(0, "/opt/uso-campo-electrico-app")
from simulador_campo_electrico import INTERVALO_S, estado_inicial, siguiente_lectura

N = 100_000
INICIO = dt.datetime(2026, 10, 19, 0, 0, 0)
SALIDA = os.path.dirname(os.path.abspath(__file__))
rng = random.Random(42)

lectura = estado_inicial(rng)
ce_rows, cm_rows, va_rows = [], [], []
indices_error_cm = set(rng.sample(range(N), 180))
indices_duplicados = set(rng.sample(range(N), 1200))
for i in range(N):
    lectura = siguiente_lectura(lectura, rng)
    ts = (INICIO + dt.timedelta(seconds=i * INTERVALO_S)).strftime("%Y-%m-%d %H:%M:%S")
    ce_rows.append((ts, lectura["CE"]))
    cm_rows.append((ts, 99999.0 if i in indices_error_cm else lectura["CM"]))
    va = [lectura["TempOut"], lectura["OutHum"], lectura["WindSpeed"], lectura["Bar"], lectura["SolarRad"], lectura["UVIndex"], ts]
    if i in indices_duplicados:  # el mismo segundo exportado dos veces: una copia incompleta
        incompleta = list(va)
        for j in rng.sample(range(6), 2):
            incompleta[j] = ""
        va_rows.append(incompleta)
    va_rows.append(va)

def escribir(nombre, cabecera, filas):
    with open(os.path.join(SALIDA, nombre), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cabecera)
        w.writerows(filas)
    print(f"{nombre}: {len(filas):,} filas")

escribir("campo_electrico.csv", ["DateTime", "CE"], ce_rows)
escribir("campo_magnetico.csv", ["DateTime", "CM"], cm_rows)
escribir("variables_ambientales.csv", ["TempOut", "OutHum", "WindSpeed", "Bar", "SolarRad.", "UVIndex", "DateTime"], va_rows)
