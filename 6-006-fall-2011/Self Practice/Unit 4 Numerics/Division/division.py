def rshift(num, n):    
    str_num = str(num)
    if len(str_num) <= n:
        return 0
    return int(str_num[:len(str_num) - n])

def lshift(num, n):
    return int(str(num) + "".join(["0" for i in range(n)]))

def slow_div(num, den):
    return num / den

def fast_div(num, den):
    if num < den:
        return 0
    assert(den > 0)
    len_num = len(str(num))
    len_den = len(str(den))
    result = lshift(1, len_den) / den
    len_prec = len_den
    while len_prec != len_num:
        len_impr = min(len_num, 2 * len_prec) - len_prec
        len_prec += len_impr
        result = lshift(result, len_impr)
        while True:
            temp = result * 2 - rshift(result * result * den, len_prec)
            if temp <= result:
                break
            result = temp
        if not lshift(1, len_prec) == result * den:
            result -= 1
    result = rshift(result * num, len_num)
    if (result + 1) * den <= num:
        result += 1
    return result

def main():
    cases = int(raw_input())
    for tc in range(cases):
        num = int(raw_input())
        den = int(raw_input())
        pre = int(raw_input())
        res = slow_div(lshift(num, pre), den)
        int_part = rshift(res, pre)
        float_part = res - lshift(rshift(res, pre), pre)
        print "Case #" + str(tc + 1) + ": " + str(int_part) + "." + ("%0" + str(pre) +"d") % (float_part)

if __name__ == "__main__":
    main()
