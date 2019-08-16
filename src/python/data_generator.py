import cv2
import numpy as np
import random

mat = cv2.imread("C://Users//Administrator//Desktop//DBSCAN_Dynamic//data//dbscan_test.png")
#print(mat)
img_gray = cv2.cvtColor(mat, cv2.COLOR_RGB2GRAY)
print(img_gray)

data = []
for line in img_gray:
    tmp = []
    for item in line:
        if item < 120:
            tmp.append(0)
        else:
            tmp.append(255)
    data.append(tmp)
#data = np.mat(data)
print(data)

flush = []
for i in range(len(data)):
    for j in range(len(data[i])):
        if data[i][j] < 120 and random.randint(0, 10)>5:
            flush.append((i,j))

f = open("cluster5.data", "w")
for item in flush:
    f.write("%d %d\n" % (item[0], item[1]))
f.close()
#cv2.imshow("gray", data)
#cv2.waitKey()