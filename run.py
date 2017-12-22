import sys
import pickle
import pandas as pd

from evaluator import build_and_save_model

def build_movie_model(filepath):
    """
    Build a classifier for our data and test its accuracy
    """

    movie_df_location = 'data/imdb_movie_data_1513892143.xlsx'
    movie_df = pd.read_excel(movie_df_location)

    # Only pick the Return-on-Investment and Plot fields of our data
    subset_df = movie_df.dropna(subset=['roi', 'plot'])[['roi', 'plot']]

    # There are multiple plots for each movie.
    # subset_df = subset_df.reset_index().groupby('index').first()
    subset_df = subset_df.sample(frac=1)

    X = subset_df['plot'].values
    y = ['lose_money' if x < 2 else 'make_mediocre_returns' if x < 7 else 'be_a_box_office_success' for x in subset_df['roi'].values]

    model = build_and_save_model(X, y, filepath)

if __name__ == '__main__':

    filepath='model.pickle'

    if len(sys.argv) > 1 and sys.argv[1] == 'build_model':
        build_movie_model(filepath)
    elif len(sys.argv) > 1 and sys.argv[1] == 'analyze':
        with open(filepath, 'rb') as f:
            model = pickle.load(f)
        plot = raw_input("What's your pitch for the next big box-office hit? ") # Ask user to pitch their movie plot
        yhat = model.predict([plot]) # Run plot in our model to predict the class
        print('You will most likely ' + ' '.join(model.labels_.inverse_transform(yhat)[0].split('_')) + '.')
    else:
        print("Incorect Arguments")
