import ray
import time

# Inicializar Ray en modo de cluster
ray.init(address='auto')  # Asumiendo que Ray está en modo de cluster
# ray.init(local_mode=True)  # Inicializar Ray en modo local sin utilizar la distribución

# Definir una función simple para procesar la lista de manera paralela
@ray.remote
def procesar_sublista(sublista):
    # Simular un procesamiento que toma tiempo
    time.sleep(1)
    # Calcular la suma de la sublista
    suma = sum(sublista)
    return suma

def computacion_distribuida():
    # Crear una lista grande para procesar
    lista_grande = [i for i in range(1, 10001)]

    # Dividir la lista en sub-listas más pequeñas
    sub_listas = [lista_grande[i:i+100] for i in range(0, len(lista_grande), 100)]

    # Medir el tiempo de inicio
    start_time = time.time()

    # Enviar las sub-listas a los nodos del cluster para procesamiento paralelo
    resultados_paralelos = ray.get([procesar_sublista.remote(sublista) for sublista in sub_listas])

    # Calcular la suma total de los resultados paralelos
    suma_total = sum(resultados_paralelos)

    # Medir el tiempo de finalización
    end_time = time.time()

    # Imprimir el resultado y el tiempo transcurrido
    print("La suma total es:", suma_total)
    print(f"Tiempo de ejecución: {end_time - start_time:.4f} segundos")

# Ejecutar la computación distribuida
computacion_distribuida()

# Cerrar Ray al finalizar
ray.shutdown()
