from data_loader import load_fecha, load_paises, load_retrasos, load_vuelos
from analysis import run_analysis

def main():
    # Cargar los datos de los archivos
    fecha_df = load_fecha("data/fecha.dat")
    paises_df = load_paises("data/paises.ada")
    retrasos_df = load_retrasos("data/retrasos.dat")
    vuelos_df = load_vuelos("data/vuelos.dat")
    
    # Mostrar las primeras filas de cada DataFrame (solo para verificar)
    print("Fecha DataFrame:")
    fecha_df.show()
    
    print("Países DataFrame:")
    paises_df.show()
    
    print("Retrasos DataFrame:")
    retrasos_df.show()
    
    print("Vuelos DataFrame:")
    vuelos_df.show()

    run_analysis()

if __name__ == "__main__":
    main()
