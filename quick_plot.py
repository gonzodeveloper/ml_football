import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

file = "data/8d_results_svc.csv"
xvar = 'c'
yvar = 'score'
df = pd.read_csv(file)

means = df.groupby(['c'], as_index=False)['score'].mean()
sds = df.groupby(['c'], as_index=False)['score'].std()


xx = means.as_matrix(['c'])
yy = means.as_matrix(['score'])
sd = sds.as_matrix(['score'])


fig = plt.figure(1)
ax = fig.add_subplot(111)
ax.scatter(xx, yy)
ax.plot(xx, yy, linestyle="-", marker="o", c='r')

for i in np.arange(0, len(sd)):
    ax.scatter([xx[i], xx[i]], [yy[i] + sd[i], yy[i] - sd[i]],
               marker="_", c='y')
plt.title("Testing soft margin with Support Vector Classifier")

ax.set_xlabel("c value")
ax.set_ylabel("accuracy")
plt.show()