import math
from unittest import TestCase

from EstimarPi import estimate_pi


class Test(TestCase):
    def test_coordinate_in_circle(self):
        estimacion_pi = estimate_pi(100000000)
        print(estimacion_pi)
        self.assertAlmostEqual(estimacion_pi, math.pi, 1)
