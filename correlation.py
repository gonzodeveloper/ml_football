import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

file = "data/game_stats_8d.csv"
xvar = 'team_pass_avg'
yvar = 'team_pass_weak'
df = pd.read_csv(file).dropna()
coef= np.corrcoef(df[xvar], df[yvar])

cor_str = "Correlation Coeficient: " + str(coef[0][1])
xx = df.as_matrix([xvar])
yy = df.as_matrix([yvar])


fig = plt.figure(1)
ax = fig.add_subplot(111)
ax.scatter(xx, yy)


ax.text(0.05, 0.95, cor_str, transform=ax.transAxes, fontsize=12,
        verticalalignment='bottom')
plt.title("Rush Strength vs Pass Strength")

ax.set_xlabel("Rush Strength")
ax.set_ylabel("Pass Strength")
plt.show()