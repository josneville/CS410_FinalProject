from __future__ import division

import sys
import sqlalchemy as sa
import pandas as pd
import requests
import time
import numpy as np
from urllib import quote
from django.utils.http import urlquote
import json
import ast
import difflib

computer_username = "josneville" # Replace this with your computer's username
db_name = "imdb" # Replace this with your db name

engine = sa.create_engine("postgres://" + computer_username + "@localhost/" + db_name)
    conn = engine.connect()

def get_search_results(search_results_location, movie_df):
    """
    Download and save the entire TMDB movie list to make searches faster
    """

    # If search_results file already exists, read that
    # Else, create search results file
    if search_results_location is not None:
        search_results_df = pd.read_excel(search_results_location)
    else:
        search_result_df_ids = []
        search_result_df_json = []

        counter = 0
        reset_time = None
        progress = 0

        movie_df_len = len(movie_df)
        for i, row in movie_df.iterrows():
            counter = counter + 1
            progress = progress + 1

            sys.stdout.write("Download progress: %d/%d   \r" % (progress, movie_df_len) )

            """
            TMDB limits API calls to 40 calls every 10 seconds. They provide the time they reset the limitation through the
            header `X-RateLimit-Reset` in their API calls.

            We take the current time - reset time + 3 seconds (buffer for any time mismatch) to wait until sending the next
            40 api calls
            """

            if counter > 39:
                current_time = int(time.time())
                time.sleep(reset_time - current_time + 3)
                counter = 0

            r = requests.get('https://api.themoviedb.org/3/search/movie?api_key=8d24244904269a246930ec594ed4a428&query=' + urlquote(row['title']))
            search_results = r.json()
            reset_time = int(r.headers['X-RateLimit-Reset'])

            if 'results' in search_results and len(search_results['results']) > 0:
                search_result_df_ids.append(i)
                search_result_df_json.append(search_results['results'])

        # Save search_results to excel
        search_results_df = pd.DataFrame(data=search_result_df_json, index = search_result_df_ids)
        writer = pd.ExcelWriter('data/tmdb_search_results.xlsx')
        search_results_df.to_excel(writer,'Search Results')
        writer.save()
    return search_results_df

def fill_movies_from_tmdb(movie_df, search_results_df):
    """
    Using the search results file from TMDB, download the following metadata for each movie (if available):
    - Budget
    - Revenue
    - Ratings
    - MetaCritic Ratings
    - IMDB Id (for joining with the IMDB database)
    """

    budgets = []
    revenues = []
    rois = []
    ratings = []
    metascores = []
    imdb_ids = []

    counter = 0
    reset_time = None
    progress = 0

    movie_df_len = len(movie_df)
    for i, row in movie_df.iterrows():
        counter = counter + 1
        progress = progress + 1

        sys.stdout.write("Download progress: %d/%d   \r" % (progress, movie_df_len) )

        if counter > 39:
            current_time = int(time.time())
            time.sleep(reset_time - current_time + 3)
            counter = 0

        original_uni_id = row['title'] + str(row['production_year'])

        best_match = {'id': None, 'score': 0, 'title': None}
        if i in search_results_df.index:

            """
            The code below iterates through the search results and finds the best match based on title of the movie
            and production year
            """
            for row in search_results_df.loc[i].dropna():
                row_dict = ast.literal_eval(row)
                search_results_id = row_dict['title'] + row_dict['release_date'].split('-')[0]

                distance = difflib.SequenceMatcher(None, original_uni_id, search_results_id).ratio()
                if distance > best_match['score']:
                    best_match['score'] = distance
                    best_match['id'] = row_dict['id']
                    best_match['title'] = row_dict['title']


            r = requests.get('https://api.themoviedb.org/3/movie/' + str(best_match['id']) + '?api_key=8d24244904269a246930ec594ed4a428')

            movie_info = r.json()
            reset_time = int(r.headers['X-RateLimit-Reset'])

            budget = movie_info['budget'] if 'budget' in movie_info and movie_info['budget'] != 0 else np.nan
            revenue = movie_info['revenue'] if 'revenue' in movie_info and movie_info['revenue'] != 0 else np.nan
            roi = revenue / budget
            budgets.append(budget)
            revenues.append(revenue)
            rois.append(roi)
            ratings.append(movie_info['vote_average'] if 'vote_average' in movie_info else np.nan)
            imdb_ids.append(movie_info['imdb_id'] if 'imdb_id' in movie_info else np.nan)
        else:
            # If no search results, set everything to NaN or None depending on type
            budgets.append(np.nan)
            revenues.append(np.nan)
            rois.append(np.nan)
            ratings.append(np.nan)
            imdb_ids.append(None)

    movie_df['budget'] = budgets
    movie_df['revenue'] = revenues
    movie_df['roi'] = rois
    movie_df['tmdb_rating'] = ratings
    movie_df['imdb_id'] = imdb_ids
    return movie_df

def fill_movies_from_imdb(movie_df):
    """
    Fill the following from the IMDB database that is available locally:
    - Genre
    - Directors
    - Actors
    - IMDB Votes
    - IMDB Rating
    - Runtime
    - Countries
    - Production Companies
    - Distributors
    - Copyright Holders
    - Keywords
    - Plot
    """

    movie_id_tuple = tuple(movie_df.index.tolist())
    if 'genres' in movie_df.columns:
        del movie_df['genres']
    genre_df = pd.read_sql('select movie_id, info as genres from movie_info where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 3, ), conn)
    genre_df = genre_df.groupby('movie_id', as_index=True)['genres'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(genre_df)

    if 'directors' in movie_df.columns:
        del movie_df['directors']
    director_df = pd.read_sql(('select cast_info.movie_id as movie_id, name.name as directors from cast_info join name on cast_info.person_id = name.id where cast_info.movie_id in %s and cast_info.role_id = %s' % (movie_id_tuple, 8, )), conn)
    director_df = director_df.groupby('movie_id', as_index=True)['directors'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(director_df)

    if 'actors' in movie_df.columns:
        del movie_df['actors']
    actor_df = pd.read_sql(('select cast_info.movie_id as movie_id, name.name as actors from cast_info join name on cast_info.person_id = name.id where cast_info.movie_id in %s and (cast_info.role_id = %s or cast_info.role_id = %s)' % (movie_id_tuple, 1, 2, )), conn)
    actor_df = actor_df.groupby('movie_id', as_index=True)['actors'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(actor_df)

    if 'imdb_votes' in movie_df.columns:
        del movie_df['imdb_votes']
    if 'imdb_rating' in movie_df.columns:
        del movie_df['imdb_rating']
    votes_df = pd.read_sql('select movie_id, info as imdb_votes from movie_info_idx where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 100, ), conn, index_col=['movie_id'])
    rating_df = pd.read_sql('select movie_id, info as imdb_rating from movie_info_idx where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 101, ), conn, index_col=['movie_id'])
    movie_df = movie_df.join(votes_df)
    movie_df = movie_df.join(rating_df)

    if 'runtime' in movie_df.columns:
        del movie_df['runtime']
    runtime_df = pd.read_sql('select distinct on (movie_id) movie_id, info as runtime from movie_info where movie_id in %s and info_type_id = %s and info not like \'%s\' and coalesce(note, \'\') = \'\'' % (movie_id_tuple, 1, "%%:%%", ), conn, index_col=['movie_id'])
    movie_df = movie_df.join(runtime_df)

    if 'countries' in movie_df.columns:
        del movie_df['countries']
    countries_df = pd.read_sql('select movie_id, info as countries from movie_info where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 8, ), conn)
    countries_df = countries_df.groupby('movie_id', as_index=True)['countries'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(countries_df)

    if 'production_companies' in movie_df.columns:
            del movie_df['production_companies']
    production_companies_df = pd.read_sql('select movie_companies.movie_id, company_name.name as production_companies from movie_companies join company_name on movie_companies.company_id = company_name.id where movie_companies.movie_id in %s and movie_companies.company_type_id = %s' % (movie_id_tuple, 2, ), conn)
    production_companies_df = production_companies_df.groupby('movie_id', as_index=True)['production_companies'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(production_companies_df)

    if 'distributors' in movie_df.columns:
            del movie_df['distributors']
    distributor_df = pd.read_sql('select movie_companies.movie_id, company_name.name as distributors from movie_companies join company_name on movie_companies.company_id = company_name.id where movie_companies.movie_id in %s and movie_companies.company_type_id = %s' % (movie_id_tuple, 1, ), conn)
    distributor_df = distributor_df.groupby('movie_id', as_index=True)['distributors'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(distributor_df)

    if 'copyright_holders' in movie_df.columns:
        del movie_df['copyright_holders']
    copyright_holder_df = pd.read_sql('select movie_id, info as copyright_holders from movie_info where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 103, ), conn)
    copyright_holder_df = copyright_holder_df.groupby('movie_id', as_index=True)['copyright_holders'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(copyright_holder_df)

    if 'keywords' in movie_df.columns:
            del movie_df['keywords']
    keyword_df = pd.read_sql('select movie_keyword.movie_id, keyword.keyword as keywords from movie_keyword join keyword on movie_keyword.keyword_id = keyword.id where movie_keyword.movie_id in %s' % (movie_id_tuple, ), conn)
    keyword_df = keyword_df.groupby('movie_id', as_index=True)['keywords'].apply(lambda x: ' : '.join(x))
    movie_df = movie_df.join(keyword_df)

    if 'plot' in movie_df.columns:
        del movie_df['plot']
    votes_df = pd.read_sql('select movie_id, info as plot from movie_info where movie_id in %s and info_type_id = %s' % (movie_id_tuple, 98, ), conn, index_col=['movie_id'])
    movie_df = movie_df.join(votes_df)

    return movie_df

if __name__ == '__main__':
    movie_df_location = 'data/imdb_movie_data_1513892143.xlsx' # Replace with imdb_movie_data file name if you have the file
    search_results_location = 'data/tmdb_search_results.xlsx' # Replace with None if you do not have the file

    # If movie information already downloaded, use that. Else recreate minimal information from postgres
    if movie_df_location is not None:
        movie_df = pd.read_excel(movie_df_location)
        movie_id_tuple = tuple(movie_df.index.tolist())
    else:
        query = 'select DISTINCT(title.id) as title_id from title join movie_info on title.id = movie_info.movie_id where title.production_year > 1975 and title.kind_id = 1 and movie_info.info_type_id = 107'
        movie_df = pd.read_sql(query, conn, index_col=['title_id']) # Set title_id as main index

        movie_id_tuple = tuple(movie_df.index.tolist())
        title_df = pd.read_sql(('select id, title, production_year from title where id in %s' % (movie_id_tuple, )), conn, index_col=['id'])
        movie_df = movie_df.join(title_df)

    search_results_df = get_search_results(search_results_location, movie_df)

    # Only need to do this if there wasn't already a movie data file downloaded
    if movie_df_location is None:
        movie_df = fill_movies_from_tmdb(movie_df, search_results_df)

    movie_df = fill_movies_from_imdb(movie_df)

    writer = pd.ExcelWriter('data/imdb_movie_data_'+str(int(time.time()))+'.xlsx')
    movie_df.to_excel(writer,'Movie Data')
    writer.save()
