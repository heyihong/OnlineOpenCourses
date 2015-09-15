import imagematrix

class ResizeableImage(imagematrix.ImageMatrix):
    def best_seam(self):
        dp = [[0 for j in range(self.height)] for i in range(self.width)]
        path = [[0 for j in range(self.height)] for i in range(self.width)]
        for i in range(self.width):
            dp[i][0] = self.energy(i, 0);
        for j in range(1, self.height):
            for i in range(self.width):
                    minValue = dp[i][j - 1]
                    if i > 0 and minValue > dp[i - 1][j - 1]:
                        minValue = dp[i - 1][j - 1]
                        path[i][j] = -1
                    if i + 1 < self.width and minValue > dp[i + 1][j - 1]:
                        minValue = dp[i + 1][j - 1]
                        path[i][j] = 1
                    dp[i][j] = minValue + self.energy(i, j)
        result = []
        point = (0, self.height - 1)
        minValue = dp[0][self.height - 1]
        for i in range(1, self.width):
            if minValue > dp[i][self.height - 1]:
                point = (i, self.height - 1)
                minValue = dp[i][self.height - 1]
        while point[1] != -1:
            result.append(point)
            i, j = point
            point = (i + path[i][j], j - 1)
        result.reverse()
        return result

    def remove_best_seam(self):
        self.remove_seam(self.best_seam())
