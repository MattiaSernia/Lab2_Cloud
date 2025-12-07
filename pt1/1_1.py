from math import sin
def integration(a, b, N):
    int_len= (b - a) / N
    total = 0.0
    sup=a+int_len
    for i in range(N):
       h= abs(sin(sup))
       total+= h*int_len
       sup+=int_len
    return total
for i in [10,100,100,1000,10000,100000,1000000]:
    print(integration(0, 3.14159, i))


