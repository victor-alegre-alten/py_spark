import math
from unittest import TestCase

from EstimarPi import estimate_pi, estimate_pi_blocks


class Test(TestCase):
    def test_coordinate_in_circle(self):
        estimacion_pi = estimate_pi(1000000)
        self.assertAlmostEqual(estimacion_pi, math.pi, 1)

    def test_estimate_pi_blocks(self):
        estimacion_pi = estimate_pi_blocks(100000, 5)
        self.assertAlmostEqual(estimacion_pi, math.pi, 1)