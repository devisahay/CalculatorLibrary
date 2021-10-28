import unittest
# from main import addition, substraction
from . import main as m

class TestCalc(unittest.TestCase):
    def test_addition(self):
        self.assertEqual(m.addition(2,4), 6)
        self.assertEqual(m.addition(-1,4), 3)
        self.assertEqual(m.addition(-4,4), 0)
        self.assertEqual(m.addition(-7,-2),-9)

    def test_substraction(self):
        self.assertEqual(m.substraction(5,2), 3)
        self.assertRaises(ValueError, m.substraction, 1, 4)
        self.assertRaises(TypeError, m.substraction, 'a', 3)
