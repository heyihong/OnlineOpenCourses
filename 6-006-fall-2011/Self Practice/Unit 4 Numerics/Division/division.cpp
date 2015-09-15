#include <iostream>
#include <cstdio>
#include <algorithm>
#include <vector>
#include <string>

#include <assert.h>

#include "fft.cpp"

using namespace std;

class BigNum {
public:
    BigNum(string digits) {
        d.resize(digits.size());
        for (int i = 0; i != digits.size(); ++i)
           d[i] = digits[digits.size() - 1 - i] - '0'; 
        normalize();
    }
    
    BigNum(int number) {
        for (; number; number /= 10)
            d.push_back(number % 10);
        if (d.empty()) {
            d.push_back(0);
        }
    }

    bool operator==(const BigNum & other) const {
        if (d.size() != other.d.size()) {
            return false;
        }    
        for (int i = 0; i != d.size(); ++i)
            if (d[i] != other.d[i]) {
                return false;
            }
        return true;
    }

    bool operator<(const BigNum & other) const {
        if (d.size() != other.d.size()) {
            return d.size() < other.d.size();
        }
        for (int i = d.size() - 1; i != -1; --i)
            if (d[i] != other.d[i]) {
                return d[i] < other.d[i];
            }
        return false;
    }

    bool operator<=(const BigNum & other) const {
        return *this < other || *this == other;
    }

    bool is_pow_10() const {
        for (int i = 0; i != d.size() - 1; ++i)
            if (d[i] != 0) {
                return false;
            }
        return d.back() == 1;
    }
    
    BigNum& add(const BigNum & other) {
        d.resize(max(d.size(), other.d.size()));
        int carry = 0;
        for (int i = 0; i != d.size(); ++i) {
            d[i] += carry + (i < other.d.size() ? other.d[i] : 0);
            if (d[i] >= 10) {
                d[i] -= 10;
                carry = 1;
            }
            else {
                carry = 0;
            }
        }
        if (carry) {
            d.push_back(carry);
        }
        return *this;
    } 

    BigNum& sub(const BigNum & other) { 
        assert(other <= *this);
        int carry = 0;
        for (int i = 0; i != d.size(); ++i) {
            d[i] -= carry + (i < other.d.size() ? other.d[i] : 0);
            if (d[i] < 0) {
                d[i] += 10;
                carry = 1;
            }
            else {
                carry = 0;
            }
        } 
        normalize();
        return *this;
    }

    BigNum& mul(const BigNum & other) {
        if (d.size() < 64 || other.d.size() < 64) {
            slow_mul(other);
        }
        else {
            fast_mul(other);
        }
        return *this; 
    }

    BigNum& div(const BigNum & other) {
        fast_div(other);
        return *this;
    }

    BigNum& rshift(int n) {
        if (d.size() <= n) {
            d.resize(0); 
            d.push_back(0);
        } 
        else {
            copy(d.begin() + n, d.end(), d.begin());
            d.resize(d.size() - n);
        }
        return *this;
    }

    BigNum& lshift(int n) {
        if (d.size() > 1 || d[0] != 0) {
            int old_size = d.size();
            d.resize(d.size() + n);
            copy_backward(d.begin(), d.begin() + old_size, d.end());
            fill(d.begin(), d.begin() + n, 0);
        }
        return *this;
    }

    const string to_string() const {
        string result = "";
        for (int i = d.size() - 1; i != -1; --i) {
            result += (char)(d[i] + '0');
        }
        return result;
    }

private:
    BigNum(const vector<int> & _d) : d(_d) {
    }

    void slow_mul(const BigNum & other) {
        vector<int> result(d.size() + other.d.size(), 0); 
        for (int i = 0; i != d.size(); ++i) {
            int carry = 0;
            for (int j = 0; j != other.d.size(); ++j) {
                int sum = result[i + j] + d[i] * other.d[j] + carry;
                result[i + j] = sum % 10; 
                carry = sum / 10;
            }
            result[i + other.d.size()] = carry;
        }
        d = result;
        normalize();
    }

    void fast_mul(const BigNum & other) {
        fft::N = d.size();
        fft::M = other.d.size();
        copy(d.begin(), d.end(), fft::A);
        copy(other.d.begin(), other.d.end(), fft::B);
        fft::FFT_Multiply();
        int size = fft::S;
        d.resize(size);
        copy(fft::Ans, fft::Ans + size, d.begin());
    }

    void fast_div(const BigNum & other) {
        if (*this < other) {
            d.resize(0);
            d.push_back(0);
            return;
        }
        int len_den = 1;
        BigNum result = BigNum(100 / (other.d.back() + (other.d.size() > 1))); 
        string str_den = other.to_string(); 
        while (len_den < other.d.size()) {
            int len_impr  = min(len_den * 2, (int)other.d.size()) - len_den;
            len_den += len_impr;
            BigNum den = BigNum(str_den.substr(0, len_den));
            if (len_den != other.d.size()) {
                den.add(BigNum("1"));
            }
            newton_method(result, den, len_impr, len_den * 2);  
            fix_precision(result, den, 2 * len_den);
        } 
        int len_prec = 2 * len_den;
        while (len_prec < d.size()) {
            int len_impr = min(len_prec * 2 - len_den, (int)d.size()) - len_prec;
            len_prec += len_impr;
            newton_method(result, other, len_impr, len_prec);
            fix_precision(result, other, len_prec); 
        }
        if (len_prec > d.size()) {
            result.rshift(len_prec - d.size());
            len_prec = d.size();
        }
        // 计算出结果
        result.mul(*this).rshift(len_prec);
        // 最后的调整
        BigNum product = result;
        product.mul(other).add(other);
        if (product <= *this) {
            result.add(BigNum("1"));
        }
        d = result.d;
    }

    void newton_method(BigNum & x, const BigNum & b, int ls, int rs) {
        // x' = (2 * x << ls) + (x * x * b >> rs - 2 * ls) 
        assert(rs >= 2 * ls); 
        BigNum temp = x;
        temp.mul(temp).mul(b).rshift(rs - 2 * ls);
        x.mul(BigNum("2")).lshift(ls).sub(temp);
    }

    void fix_precision(BigNum & x, const BigNum & b, int len_prec) {
        BigNum temp = x;
        temp.mul(b);    
        BigNum remain = BigNum("1");
        remain.lshift(len_prec);
        if (! (temp <= remain)) {
            x.sub(BigNum("1"));
            return;
        }
        remain.sub(temp); 
        vector<BigNum> p;
        temp = b;
        while (temp <= remain) {
            p.push_back(temp);
            temp.add(p.back());
        }
        assert(p.size() <= 30);
        int add_num = 0;
        for (int i = p.size() - 1; i != -1; --i) {
            if (p[i] <= remain) {
                remain.sub(p[i]);
                add_num += 1 << i;
            }
        } 
        x.add(BigNum(add_num));
    }

    void normalize() {
        for (;d.size() > 1 && d.back() == 0; d.pop_back());
    }

    vector<int> d;
};

int main() {
    int test_case;
    cin >> test_case;
    for (int tc = 1; tc <= test_case; ++tc) {
        string str_num, str_den;
        int prec;
        cin >> str_num >> str_den >> prec;
        BigNum num(str_num);
        BigNum den(str_den);
        num.lshift(prec).div(den);
        BigNum int_part = num;
        int_part.rshift(prec);
        BigNum float_part = num;
        num.rshift(prec).lshift(prec);
        float_part.sub(num);

        cout << "Case #" << tc << ": " << int_part.to_string() << ".";
        string fp = float_part.to_string();
        for (int i = prec - fp.size(); i; --i)
            cout << "0";
        cout << fp << endl;
    }
    return 0;
}
