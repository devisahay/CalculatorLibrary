from unittest import TestCase
# from circle import circle_radius
from math import pi
from . import circle as c

class TestCircleArea(TestCase):
    def test_area(self):
        self.assertAlmostEqual(c.circle_radius(1), pi)
        self.assertAlmostEqual(c.circle_radius(0),0)
        self.assertAlmostEqual(c.circle_radius(2.1), pi*(2.1 **2))

    
    def test_negative(self):
        self.assertRaises(ValueError, c.circle_radius, -2)

    