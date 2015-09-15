#include <cstring>
#include <complex>

using namespace std;

namespace fft {

    const int MaxN = 1 << 20; // 2^k greater than real MaxN
    const double Pi = 3.14159265358979323846264338327950288;
    int Size;
    int Rev(int x) { // 计算反转数
        int T = Size, Ans = 0;
        for (; T > 1; T >>= 1, x >>= 1) Ans = (Ans << 1) | (x & 1);
        return Ans;
    }
    complex <double> Ret[MaxN*2];
    void FFT(complex <double> *A, int T) {
        memset(Ret, 0, sizeof(Ret));
        for (int i = 0; i < Size; i ++) Ret[Rev(i)] = A[i];
        for (int i = 2; i <= Size; i <<= 1) {
            double Arc = 2 * Pi * T / (double) i;
            complex <double> wm(cos(Arc), sin(Arc));
            for (int k = 0; k < Size; k += i) {
                complex <double> w(1,0);
                for (int j = 0; j*2 < i; j ++) {
                    complex <double> t = w * Ret[k + j + i / 2];
                    complex <double> u = Ret[k + j];
                    Ret[k + j] = u + t;
                    Ret[k + j + i / 2] = u - t;
                    w *= wm;
                }
            }
        }
    }

    complex <double> ArrA[MaxN*2], ArrB[MaxN*2];

    int N, M, S;

    int A[MaxN*2], B[MaxN*2], Ans[MaxN*2];

    void FFT_Multiply() {
        for (int i = 0; 2 * i < N; ++i)
            A[i] = (i * 2 + 1 < N ? A[i * 2 + 1] : 0) * 10 + A[i * 2];
        for (int i = 0; 2 * i < M; ++i)
            B[i] = (i * 2 + 1 < M ? B[i * 2 + 1] : 0) * 10 + B[i * 2];
        N = (N + 1) / 2;
        M = (M + 1) / 2;
        Size = max(N, M); // 2^k greater than real MaxN
        while ((Size & -Size) != Size)
        Size ++; Size <<= 1; // Double it for multiplication
        memset(ArrA, 0, sizeof(ArrA));
        memset(ArrB, 0, sizeof(ArrB));
        for (int i = 0; i < N; i ++) ArrA[i] = complex <double> (A[i], 0);
        for (int i = 0; i < M; i ++) ArrB[i] = complex <double> (B[i], 0);
        FFT(ArrA, 1);
        memcpy(ArrA, Ret, sizeof(Ret));
        FFT(ArrB, 1);
        for (int i = 0; i < Size; i ++)
        ArrA[i] *= Ret[i];
        FFT(ArrA, -1);
        for (int i = 0; i < Size; i ++) Ret[i] /= (double) Size;
        for (int i = 0; i < Size; i ++) {
            Ans[i] += (int) (Ret[i].real() + 0.5);
            if (i != Size - 1) {
                Ans[i + 1] = Ans[i] / 100;
                Ans[i] %= 100;
            }
        }
        for (S = Size; S > 1 && Ans[S - 1] == 0; --S); 
        S *= 2;
        for (int i = S - 1; i != -1; --i)
            if (i & 1) {
                Ans[i] = Ans[i / 2] / 10;
            }
            else {
                Ans[i] = Ans[i / 2] % 10;
            }
        if (Ans[S - 1] == 0) {
            --S;
        }
    }
}
