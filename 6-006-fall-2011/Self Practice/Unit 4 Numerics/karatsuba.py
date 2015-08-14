import sys

def add(x, y): 
    """
    add x and y in high precision and return z as the result. 
    x and y should be non-negative

    RUNTIME: O(max(len(x), len(y))   
    """
    if (len(x) < len(y)):
        (x, y) = (y, x)
    z = [0 for i in range(len(x) + 1)] 
    for i in range(len(x)):
        z[i] = z[i] + x[i]
        if (i < len(y)):
            z[i] += y[i]
        if (z[i] >= 10):
            z[i + 1] += 1
            z[i] -= 10
    if (z[len(z) - 1] == 0):
        z.pop()
    return z

def sub(x, y):
    """
    subtract x and y in high precision and return z as the result.
    x and y should be non-negative and x shoulbe be bigeger than or equal to y

    RUNTIME: O(max(len(x), len(y)))
    """

    z = [0 for i in range(len(x))]
    for i in range(len(x)):
        z[i] = z[i] + x[i]
        if (i < len(y)):
            z[i] -= y[i]
        if (z[i] < 0):
            z[i] += 10
            z[i + 1] -= 1
    while (len(z) > 1 and z[len(z) - 1] == 0):
        z.pop()
    return z

def shift_left(x, n):
    """
    fill x with n zeros in its back.
   
    RUNTIME: O(max(len(x), n))
    """ 

    if (len(x) == 1 and x[0] == 0):
        return [0]
    else:
        return [0 for i in range(n)] + x

def mul(x, y):
    """
    using karatsuba algorithm to implements the high precision multiplication.
    x and y should 

    RUNTIME: O(max(len(x), len(y)) ^ (log base 2 of 3))
    """
    
    if (len(x) < len(y)):
        (x, y) = (y, x)
    if (len(x) == 1):
        tmp = x[0] * y[0]
        z = [tmp % 10]
        if (tmp >= 10):
            z.append(tmp / 10)
        return z
    mid = len(x) / 2
    x0 = x[:mid]
    x1 = x[mid:]
    y0 = y[:mid]
    y1 = y[mid:]
    z0 = mul(x0, y0)
    if (len(y1) > 0):
        z2 = mul(x1, y1) 
        addY0Y1 = add(y0, y1)
    else:
        z2 = [0]
        addY0Y1 = y0
    z1 = sub(mul(add(x0, x1), addY0Y1), add(z0, z2))
    z = add(z0, add(shift_left(z1, mid), shift_left(z2, mid * 2))) 
    return z
        
def karatsuba(strX, strY):
   """
   input two integers in string form and output the mulitiplication of these two integers.  

   RUNTIME: O(max(len(strX), len(strY)) ^ (log base 2 of 3))
   """
   x = [ord(c) - ord('0') for c in strX]
   y = [ord(c) - ord('0') for c in strY] 
   x.reverse()
   y.reverse()
   z = mul(x, y)
   strZ = ''
   for n in z:
       strZ = chr(n + ord('0')) + strZ 
   return strZ

if __name__ == "__main__":
    line = raw_input("Please enter two numbers:") 
    (strX, strY) = line.split(" ")
    strZ = karatsuba(strX, strY)
    print strZ
