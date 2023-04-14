# NFL Project

This project analyses and models NFL data to predict NFL player fantasy performance for the purpose of competing in Daily Fantasy Sports competitions. While that may seem mundane, the contests are unique in that they require competitors to consider the full distribution of outcomes when choosing their team. To accomplish this I used Bayesian Generalized Linear Mixed Models (GLLMs) and Bayesian Neural Networks, which were built using the Bambi package (a wrapper for PyMc3) and TensorFlow Probability.

These models capture the inherent uncertainty of NFL player production, which is essential for success in NFL Daily Fantasy contests. They were also fun to build.

# Data Source
The data is sourced from the [nflfastR package](https://github.com/nflverse/nflverse-data/releases). 

In the [db_insert notebook](https://github.com/yaobviously/nfl_project/blob/main/notebooks/db_insert.ipynb) I upsert the data to an SQL database using the utilities defined in [db_utils](https://github.com/yaobviously/nfl_project/blob/main/db_utils.py).

# Packages

SQLalchemy and psycopg2 are used to interact with the PostgreSQL hosted on Heroku. SQL, pandas, and sklearn are used for data preparation and preprocessing. The models are built using sklearn, bambi, and tensorflow. 

# Note

This project is being adapted from Notebooks I had in Google Colab. Bear with me as I consolidate it into what I hope will become a package with model-building templates by the beginning of NFL preseason. 