# Box Office Returns Project

The goal of this project is to be able to predict box office returns for a movie given its plot details. This will allow for people to gauge the quality of a movie and whether it is worth taking the time to make.

The model used for prediction will be based on 40 years of movie data supplied from IMDB (https://imdb.com) in conjuction with data from TMDB (https://www.themoviedb.org/?language=en), who provides accurate box office and revenue numbers that IMDB does not.

## Implementation

The main focus of the implementation is split up into two parts:

1) Collect all the data necessary and clean it in such a way that it is usable for machine learning
2) Create a model that could process the data and be able to predict box office returns

### Collecting the data

The hardest part of the entire project was collecting the data. IMDB provides raw text files with almost no formatting but luckily I found a tool that can process that information. Afterward, I realized that IMDB did not provide box office or revenue data (but it did provide plot data). I looked around and found that TMDB provided the box office numbers but they had an API limit of 40 requests per every 10 seconds. This made the TMDB pull take approximately 2 hours. I save intermediate steps to reset if any part failed.

At the end, I created a excel dump of the data in `/data` to use as the backbone for the model

To recreate my steps
- run `./download_imdb_files.sh`
  - This will download the several raw imdb files from one of their ftp servers
- run `psql postgres`
  - `CREATE DATABASE imdb`
  - ctrl-c to exit
- run `python imdbpy2sql.py -d data -u postgres://<insert username for computer>@localhost/imdb`
  - Courtesy of: https://github.com/alberanid/imdbpy/blob/master/bin/imdbpy2sql.py
  - This will ingest all the imdb files and create tables in postgres that are queryable
  - Allow several hours for this to run
    - It is a highly inefficient piece of code due to the horrible structure provided by IMDB
- run `python download_data.py`
  - This connects the IMDB data with the TMDB data and produces an excel file that can be used to generate the model

### Creating the model

As an initial test, I wrote python code that split each plot into several words, used the most common words with a Random Forest Classifier and tried to predict box office returns. The accuracy was an abysmal 30% (slightly worse than guessing). After doing a lot of research and several iterations, I got a model that increased its accuracy to 70%. Those steps included:
 - Preprocessing the plot data
   - Tokenizing
   - Removing stopwords and punctuation
   - POS tagging
   - Lemmatizing
   - Filtering
 - Trying different classifiers
   - After iterating through a few, I found that the Stachostic Gradient Descent performed the best

![Accuracy](https://i.imgur.com/ADxhZxc.png)

After creating a model, I dumped the model into a file `model.pickle` which can be later retrieved and reapplied to new data

To recreate the model:
- run `python run.py build_model`

## Prerequisites
- Python 2.7
- pip
- Jupyter
- Postgres

## Setup
- run `pip install -r requirements.txt`
  - This ensures you have all the necessary libraries to run the codebase

## Run the code
- run `python run.py analyze`
  - The code will prompt the user to enter in the plot of the movie they want to be analyzed
  - Upon entering the plot, the program will spit out how much money it thinks that movie will make
