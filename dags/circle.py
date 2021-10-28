from math import pi

def circle_radius(r):
    if r < 0:
        raise ValueError("Radius of circle should be the positive number.")
    return pi*(r**2)

if __name__ == "__main__":
    test_r = [1,4, None, -4, True, "hello"]
    message = "Area of circle {radius} is {val}"

    for radius in test_r:
        val = circle_radius(radius)
        print(message.format(radius=radius, val=val))