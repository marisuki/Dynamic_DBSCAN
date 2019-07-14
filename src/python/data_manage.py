import numpy as np

f = open("C://Users//Administrator//Desktop//DBSCAN_Dynamic//data//iris.data")

dt = {}

for line in f:
    line = line.split(",")
    tmp = []
    for i in range(len(line)-1):
        tmp.append(float(line[i]))
    if line[-1] in dt:
        dt[line[-1]].append(tmp)
    else:
        dt[line[-1]] = [tmp]

for key in dt:
    tmp = []
    for it1 in dt[key]:
        for it2 in dt[key]:
            ans = 0.0
            for i in range(len(it1)):
                ans += (it1[i]-it2[i])**2
            tmp.append(np.sqrt(ans))
    sum = 0
    for item in tmp:
        sum += item
    print(sum/len(tmp))
    print(tmp)
