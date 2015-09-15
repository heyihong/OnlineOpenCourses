
import random

maxN = 500000
maxM = 500000
numRDatas = 30
numBDatas = 3 

def genRandomDatas():
	rDatas = []
	for i in range(numRDatas):
		num = random.randint(0, 10 ** i)
		den = random.randint(1, 10 ** i)
		prec = i + 1
		rDatas.append((num, den, prec))
	return rDatas

def genSpecialDatas():
	sDatas = []
	sDatas.append((1, 2, 1))
	sDatas.append((1, 9, 1))
	sDatas.append((29999, 2000000, 4))
	sDatas.append((30000, 2000000, 4))
	sDatas.append((30001, 2000000, 4))
	sDatas.append((1, 10 ** maxM - 1, maxN))
	return sDatas

def genBigDatas():
	bDatas = []
	for i in range(numBDatas):
		num = random.randint(0, 10 ** maxN - 1)
		den = random.randint(1, 10 ** maxM - 1)
		prec = maxN 
		bDatas.append((num, den, prec))
	bDatas.append((random.randint(0, 10 ** maxN - 1), random.randint(1, 10 ** (maxM / 2)), maxN))
	bDatas.append((random.randint(0, 10 ** (maxN / 2)), random.randint(1, 10 ** maxM), maxN))
	return bDatas

def main():
	genFuncs = [genRandomDatas, genSpecialDatas, genBigDatas]
	datas = []
	for genFunc in genFuncs:
		for data in genFunc():
			datas.append(data)
	print len(datas)
	for (num, den, prec) in datas:
		print num
		print den
		print prec

if __name__ == "__main__":
	main()
