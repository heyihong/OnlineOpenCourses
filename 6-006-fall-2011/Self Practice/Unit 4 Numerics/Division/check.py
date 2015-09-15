import sys

def loadInput(inputName):
    inputFile = open(inputName, "r")
    lines = inputFile.read().split("\n")
    numCases = int(lines[0])
    inputData = []
    for i in range(numCases):
        num = int(lines[i * 3 + 1])
        den = int(lines[i * 3 + 2])
        pre = int(lines[i * 3 + 3])
        inputData.append((num, den, pre))
    inputFile.close()
    return inputData

def loadAnswer(answerName):
    answerFile = open(answerName, "r")
    answerData = []
    for line in answerFile.read().split("\n"): 
        if len(line) > 0:
            parts = line.split(" ")[2].split(".")
            intPart = parts[0] 
            floatPart = parts[1]
            answerData.append(int(intPart + floatPart))
    answerFile.close()
    return answerData

def check(num, den, ans):
    tmp = ans * den
    return tmp <= num and tmp + den > num

def shiftLeft10(x, n):
    return int(str(x) + "".join(["0" for i in range(n)]))

def main():
    inputData = loadInput(sys.argv[1])
    answerData = loadAnswer(sys.argv[2]) 
    caseId = 0
    for ((num, den, pre), ans) in zip(inputData, answerData): 
        caseId += 1
        if not check(shiftLeft10(num, pre), den, ans):
            print "Case #%d" % caseId, "cannot pass the check!"  

if __name__ == "__main__":
    main()
