def question_1_2():
    def square_root_generator(limit):
        n = 1
        while n <= limit:
            yield n ** 0.5
            n += 1

    # Example usage:
    limit = 5
    generator = square_root_generator(limit)

    output = 0

    for sqrt_value in generator:
        output += sqrt_value

    print("Question 1:", round(output, 3))

    n = 1
    for sqrt_value in square_root_generator(13):
        if n == 13:
            print("Question 2:", round(sqrt_value, 3))
        n += 1