# Análisis de Vuelos con PySpark

## Descripción
Proyecto de análisis de datos de vuelos utilizando PySpark para procesamiento distribuido.

## Estructura del Proyecto

proyecto/
├── data/
│   ├── fecha.dat
│   ├── paises.ada
│   ├── retrasos.dat
│   └── vuelos.dat
├── src/
│   ├── __init__.py
│   ├── analysis.py
│   └── data_loader.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_main.py
├── output/
└── requirements.txt

## Requisitos
- Python 3.8+
- PySpark
- pytest

## Instalación

1. Clonar el repositorio:
```bash
git clone <repository-url>
cd <project-directory>

## Instalación
python -m venv venv
source venv/bin/activate  

# Instalacion de dependencias
pip install -r requirements.txt

# Ejecucion del programa
python src/main.py

# Ejecucion de pruebas unitarias
python -v

# Estructura de Datos
- fecha.dat: Registro de fechas de vuelos
- paises.ada: Catálogo de países
- retrasos.dat: Registro de retrasos
- vuelos.dat: Bitácora de vuelos

#Análisis Disponibles
- País con mayor número de despegues
- País con mayor cantidad de aterrizajes
- Días con mayor/menor actividad
- Análisis de retrasos
- Identificación de países VIP
- Filtrado específico para Perú y México
