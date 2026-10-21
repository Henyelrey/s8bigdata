"""Simulador de la estacion de campo electrico (Proyecto Sello, S04/S10).

Una sola fuente de verdad para la dinamica de las 8 variables fisicas:
- generar_datos_sinteticos.py la usa para crear el historico de S04 (1 lectura/s).
- producer_campo_electrico.py la usa para publicar en Kafka en tiempo real (1 lectura/s).
Asi el modelo entrenado sobre el historico ve en streaming exactamente el mismo proceso.
"""
import random

INTERVALO_S = 1  # una lectura por segundo, igual en el historico y en el streaming

# Rangos de la guia de S10 (min/max reales del Gold original de S04).
RANGOS = {
    "CE": (-6.74, 2.85),
    "CM": (24020.0, 24498.4),
    "TempOut": (11.8, 27.2),
    "OutHum": (61.0, 92.0),
    "WindSpeed": (0.0, 17.7),
    "Bar": (945.3, 953.7),
    "SolarRad": (0.0, 770.0),
    "UVIndex": (0.0, 6.3),
}
PASO_MAXIMO = {
    "CE": 0.08, "CM": 1.5, "TempOut": 0.1, "OutHum": 0.4,
    "WindSpeed": 0.3, "Bar": 0.1, "SolarRad": 8.0, "UVIndex": 0.15,
}
# CE no es una caminata libre: revierte hacia un nivel que depende del clima actual
# (mas temperatura sube CE, mas viento y humedad lo bajan). Eso le da a S04 una relacion
# real entre CE y las otras 7 variables, y a S10 algo que aprender mas alla de la persistencia.
REVERSION_CE = 0.05


def _acotar(valor, variable):
    minimo, maximo = RANGOS[variable]
    return min(max(valor, minimo), maximo)


def nivel_objetivo_ce(lectura):
    return _acotar(
        -1.95
        + 0.18 * (lectura["TempOut"] - 19.5)
        - 0.12 * (lectura["WindSpeed"] - 8.85)
        - 0.03 * (lectura["OutHum"] - 76.5)
        + 0.002 * (lectura["SolarRad"] - 385.0),
        "CE",
    )


def estado_inicial(rng=random):
    lectura = {v: round(rng.uniform(*r), 2) for v, r in RANGOS.items()}
    lectura["CE"] = round(nivel_objetivo_ce(lectura), 2)
    return lectura


def siguiente_lectura(lectura, rng=random):
    nueva = {}
    for variable in RANGOS:
        if variable == "CE":
            continue
        paso = PASO_MAXIMO[variable]
        nueva[variable] = round(_acotar(lectura[variable] + rng.uniform(-paso, paso), variable), 2)
    objetivo = nivel_objetivo_ce(lectura)
    ce = lectura["CE"] + REVERSION_CE * (objetivo - lectura["CE"]) + rng.uniform(-PASO_MAXIMO["CE"], PASO_MAXIMO["CE"])
    nueva["CE"] = round(_acotar(ce, "CE"), 2)
    return {v: nueva[v] for v in RANGOS}
