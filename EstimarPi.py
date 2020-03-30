import random


def get_coordinate():
    # Obtiene una coordenada
    return random.uniform(0, 1), random.uniform(0, 1)


def coordinate_in_circle(coordinate):
    # Calcula si una coordenada está dentro del círculo
    return (coordinate[0]**2 + coordinate[1]**2) <= 1


def estimate_pi(iterations):
    darts_in = 0

    for i in range(0, iterations):
        if coordinate_in_circle(get_coordinate()):
            darts_in += 1

    return darts_in/iterations*4

