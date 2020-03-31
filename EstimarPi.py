import random

from pyspark.sql import SparkSession


def get_coordinate():
    # Obtiene una coordenada
    return random.uniform(0, 1), random.uniform(0, 1)


def coordinate_in_circle(coordinate):
    # Calcula si una coordenada está dentro del círculo
    return (coordinate[0]**2 + coordinate[1]**2) <= 1


def estimate_pi (iterations):
    darts_in = 0

    for i in range(0, iterations):
        if coordinate_in_circle(get_coordinate()):
            darts_in += 1

    return darts_in/iterations*4


def estimate_pi_blocks(total_calcs, blocks):
    # Estima el valor de pi dividido en bloques
    blocks_number = [int(total_calcs/blocks) for i in range(0, blocks)]
    return sum(list(map(estimate_pi, blocks_number))) / blocks


def estimate_pi_spark(total_calcs, blocks):
    # Estima el valor de pi dividido en bloques con spark
    # ---
    # Creamos una sesión con Spark
    MASTER = 'local[3]'
    session = SparkSession.builder.master(MASTER).appName('My PI estimator').getOrCreate()

    blocks_calculation = [int(total_calcs/blocks) for i in range(0, blocks)]

    # Creamos un RDD y aplicamos los cálculos
    rdd = session.sparkContext.parallelize(blocks_calculation)
    total_sum = rdd.map(estimate_pi).reduce(lambda a, b: a + b)

    return total_sum / blocks


if __name__ == '__main__':
    print(
        estimate_pi_spark(100*1000*1000, 3)
    )