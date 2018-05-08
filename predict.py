import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

def print_progress_bar (iteration, total, prefix='Simulation', suffix='Complete', decimals=2, length=50, fill='█'):
    '''
    Auxillary function. Gives us a progress bar which tracks the completion status of our task. Put in loop.
    :param iteration: current iteration
    :param total: total number of iterations
    :param prefix: string
    :param suffix: string
    :param decimals: float point precision of % done number
    :param length: length of bar
    :param fill: fill of bar
    :return:
    '''
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()

if __name__ == "__main__":
    df = pd.read_csv("data/game_stats_8d.csv")
    df = df.dropna()

    data = df[['team_rush_avg', 'team_pass_avg', 'opp_rush_avg', 'opp_pass_avg',
               'team_rush_weak', 'team_pass_weak', 'opp_rush_weak', 'opp_pass_weak']]
    labels = df['result'].tolist()

    normal_data = normalize(data)
    labeled_data = list(zip(normal_data, labels))

    results = []

    iteration = 0
    total = 10000

    for c in np.arange(0.1, 4, 0.1):
        for i in range(100):
            # train test split according to ratio
            train_dat, test_dat = train_test_split(labeled_data, train_size=0.2, test_size=0.8)

            # Separate train points from labels
            train_points = [x[0] for x in train_dat]
            train_labels = [x[1] for x in train_dat]

            # Separate test points from their labels
            test_points = [x[0] for x in test_dat]
            test_labels = [x[1] for x in test_dat]

            #model = KNeighborsClassifier(n_neighbors=80, p=p, weights='uniform', algorithm='kd_tree', n_jobs=-1)
            model = SVC(kernel='rbf', C=c)
            model.fit(train_points, train_labels)
            score = model.score(test_points, test_labels)

            results.append([c, score])
            print_progress_bar(iteration, total)
            iteration += 1
    df = pd.DataFrame(results, columns=['c', 'score'])
    df.to_csv('data/8d_results_svc.csv', index=False)
