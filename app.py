from math import sin
from flask import Flask, jsonify

app = Flask(__name__)

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
##for i in [10,100,100,1000,10000,100000,1000000]:
##    print(integration(0, 3.14159, i))

N_VALUES = [10, 100, 100, 1000, 10000, 100000, 1000000]

@app.route('/numericalintegralservice/<lower>/<upper>')
def numerical_integration_service(lower, upper):
    results = {}
    lower= float(lower)
    upper= float(upper)
    for N in N_VALUES:
        result = integration(lower, upper, N)
        results[str(N)] = result
        
    return jsonify({
        'lower': lower,
        'upper': upper,
        'results': results,
        'message': f"Numerical integration of abs(sin(x)) from {lower} to {upper} calculated for N values."
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)