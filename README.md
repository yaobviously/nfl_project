# NFL Project

This project provides a comprehensive analysis of NFL data to predict offensive player fantasy performance. To accomplish this task, I utilized advanced modeling techniques, specifically Bayesian Generalized Linear Mixed Models (GLLMs) and Bayesian Neural Networks, which were built using the Bambi package (a wrapper for PyMc3) and TensorFlow Probability.

I chose these models to capture the inherent uncertainty of NFL player production, which is essential for success in NFL Daily Fantasy contests.

# Data Source
The data is sourced from the [nflfastR package](https://github.com/nflverse/nflverse-data/releases). 

In the [db_insert notebook](https://github.com/yaobviously/nfl_project/blob/main/notebooks/db_insert.ipynb) I upsert the data to an SQL database using the utilities defined in [db_utils](https://github.com/yaobviously/nfl_project/blob/main/db_utils.py).

# Packages

I ise sqlalchemy and psycopg2 to interact with my PostgreSQL hosted on Heroku. SQL, pandas, and sklearn are used for data preparation and preprocessing. The models are built using sklearn, bambi, and tensorflow. 

# Note

This project is being adapted from Notebooks I had in Google Colab. Bear with me as I consolidate it into what I hope will become a package with model-building templates by the beginning of NFL preseason. 