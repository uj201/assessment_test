import logging
import pandas as pd
import numpy as np
import pandasql as ps
from tabulate import tabulate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PREPARATION OF DATA
def movie_analysis():
    # User data read from data file
    logging.info("Inside movie_analysis")
    user_data = pd.DataFrame(columns=['user_id', 'age', 'gender', 'occupation', 'zip'])
    file = open("dags/scripts/movie_analysis/ml-100k/u.user", "response")
    for each_row in file:
        user_data = pd.concat([user_data, pd.DataFrame([each_row.split('|')],
                                                      columns=['user_id', 'age', 'gender', 'occupation', 'zip'])],
                              ignore_index=True)
    user_data['zip'] = [each_zip.rstrip("\n") for each_zip in user_data['zip']]
    user_data['age'] = [int(age) for age in user_data['age']]

    # Reading survey data
    survey_data = pd.read_csv("dags/scripts/movie_analysis/ml-100k/u.data", delimiter="\t", names=['user_id', 'item_id', 'rating', 'timestamp'])

    # Reading item data
    item_columns = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action',
                    'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
                    'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    item_data = pd.DataFrame(columns=item_columns)
    genres = ['unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
              'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
              'Western']
    item_file = open("dags/scripts/movie_analysis/ml-100k/u.item", "response", encoding='iso-8859-1')
    for x in item_file:
        item_data = pd.concat([item_data, pd.DataFrame([x.split('|')], columns=item_columns)], ignore_index=True)
    item_data['Western'] = [x.rstrip("\n") for x in item_data['Western']]
    item_data['movie_id'] = [int(x) for x in item_data['movie_id']]
    for genre in genres:
        item_data[genre] = [int(x) for x in item_data[genre]]

    # QUERIES TO FIND THE DESIRED RESULTS

    # 1. Find the mean age of users in each occupation
    avg_ages = user_data.groupby('occupation').agg({'age': 'mean'}).rename(columns={'age': 'avg_age'}).reset_index()
    print("\n\nFind the mean age of users in each occupation")
    print(tabulate(avg_ages, headers='keys', tablefmt='grid'))

    # 2. Find the names of top 20 highest rated movies. (at least 35 times rated by Users)
    query2 = """
        with mv_tbl as (
            select itm.movie_title,
            sum(suy.rating) as sum_rating,
            count(suy.rating) as count_rating
        from survey_data suy
        left join item_data itm
        on suy.item_id = itm.movie_id
        group by 1)
    select 
        movie_title,
        sum_rating*1.0/count_rating as avg_rating
    from mv_tbl 
        where count_rating>=35
    order by 2 desc 
    limit 20
    """
    top_20_movies = ps.sqldf(query2, locals())
    print("\n\nFind the names of top 20 highest rated movies. (at least 35 times rated by Users)")
    print(tabulate(top_20_movies, headers='keys', tablefmt='grid'))

    # 3.Find the top genres rated by users of each occupation in every age-groups. age-groups can be defined as 20-25, 25-35, 35-45, 45 and older
    def get_genre(row):
        genre = []
        for each in genres:
            if row[each] == 1:
                genre.append(each)
        return genre

    item_data['all_genres'] = item_data.apply(get_genre, axis=1)
    item_xplode = item_data.explode('all_genres')

    query3 = """
        with users as (
            select user_id,
            case when age between 20 and 25 then '20 to 25'
                when age between 26 and 35 then '26 to 35'
                when age between 36 and 45 then '36 to 45'
                when age > 45 then '45 and older' end as age_groups,
            occupation
        from user_data
        where age_groups is not null
        ),
        age_occ as (
            select b.age_groups,
                b.occupation,
                c.all_genres as genre,
                sum(a.rating)*1.0/count(a.rating) as avg_rating
            from survey_data a 
            left join users b 
            on a.user_id = b.user_id 
            left join item_xplode c 
            on a.item_id = c.movie_id
            group by 1,2,3
        ),
        rnking as (
            select age_groups,
            occupation,
            genre,
            avg_rating,
            row_number() over(partition by occupation, age_groups order by avg_rating desc) rnk
        from age_occ 
        )

    select age_groups,
        occupation,
        genre,
        avg_rating
    from rnking
    where rnk=1
    and age_groups is not null
    order by 1,4 desc
    """

    topgenres = ps.sqldf(query3, locals())
    print("\n\nFind the top genres rated by users of each occupation in every age-groups. age-groups can be defined as 20-25, 25-35, 35-45, 45 and older")
    print(tabulate(topgenres, headers='keys', tablefmt='grid'))
