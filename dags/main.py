def addition(a,b):
    return a+b

def substraction(a,b):
    if not isinstance(a, int) or not isinstance(b, int):
        raise TypeError("Not supported operands.")
    if a < b:
        raise ValueError("Result will be in negative.")
    return a - b

