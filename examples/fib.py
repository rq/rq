def slow_fib(n):
    if n <= 1:
        return 1
    else:
        return slow_fib(n - 1) + slow_fib(n - 2)
