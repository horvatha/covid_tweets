from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter  # https://geopy.readthedocs.io/en/stable/#usage-with-pandas
import logging
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format='{asctime} {message}', style='{')
df = pd.read_csv("covid19_tweets.csv")


us_url = 'https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States'
us_table = pd.read_html(us_url)[0]
us_states = us_table.iloc[:, 0].tolist()
us_cities = us_table.iloc[:, 1].tolist() + us_table.iloc[:, 2].tolist() + \
            us_table.iloc[:, 3].tolist()
us_Federal_district = pd.read_html(us_url)[1].iloc[:, 0].tolist()
us_Inhabited_territories = pd.read_html(us_url)[2].iloc[:, 0].tolist()
us_Uninhabited_territories = pd.read_html(us_url)[3].iloc[:, 0].tolist()
us_Disputed_territories = pd.read_html(us_url)[4].iloc[:, 0].tolist()

us = us_states + us_cities + us_Federal_district + us_Inhabited_territories + us_Uninhabited_territories + us_Disputed_territories

in_url = 'https://en.wikipedia.org/wiki/States_and_union_territories_of_India#States_and_Union_territories'
india_table = pd.read_html(in_url)
in_states = india_table[3].iloc[:, 0].tolist() + india_table[3].iloc[:, 4].tolist() + \
            india_table[3].iloc[:, 5].tolist()
in_unions = india_table[4].iloc[:, 0].tolist()
ind = in_states + in_unions

usToStr = ' '.join([str(elem) for elem in us]).lower()
indToStr = ' '.join([str(elem) for elem in ind]).lower()


def split_location(user_loc):
    return user_loc.strip().lower().replace(",", " ").split()


def split_location2(user_loc):
    TSplit_space = [x.lower().strip() for x in user_loc.split()]
    TSplit_comma = [x.lower().strip() for x in user_loc.split(',')]
    TSplit = list(set().union(TSplit_space, TSplit_comma))
    return TSplit


def checkl(T):
    TSplit_space = [x.lower().strip() for x in T.split()]
    TSplit_comma = [x.lower().strip() for x in T.split(',')]
    TSplit = list(set().union(TSplit_space, TSplit_comma))
    res_ind = [ele for ele in ind if (ele in T)]
    res_us = [ele for ele in us if (ele in T)]

    if 'india' in TSplit or 'hindustan' in TSplit or 'bharat' in TSplit or T.lower() in indToStr.lower() or bool(
            res_ind) == True:
        T = 'India'
    elif 'US' in T or 'USA' in T or 'United States' in T or 'usa' in TSplit or 'united state' in TSplit or T.lower() in usToStr.lower() or bool(
            res_us) == True:
        T = 'USA'
    elif len(T.split(',')) > 1:
        if T.split(',')[0] in indToStr or T.split(',')[1] in indToStr:
            T = 'India'
        elif T.split(',')[0] in usToStr or T.split(',')[1] in usToStr:
            T = 'USA'
        else:
            T = "Others"
    else:
        T = "Others"
    return T


def checkl1(T):
    TSplit = T.strip().lower().replace(",", " ").split()
    res_ind = [ele for ele in ind if (ele in T)]
    res_us = [ele for ele in us if (ele in T)]

    if 'india' in TSplit or 'hindustan' in TSplit or 'bharat' in TSplit or T.lower() in indToStr.lower() or len(
            res_ind) > 0:
        T = 'India'
    elif 'US' in T or 'USA' in T or 'United States' in T or 'usa' in TSplit or 'united state' in TSplit or T.lower() in usToStr.lower() or len(
            res_us) > 0:
        T = 'USA'
    elif len(T.split(',')) > 1:
        if T.split(',')[0] in indToStr or T.split(',')[1] in indToStr:
            T = 'India'
        elif T.split(',')[0] in usToStr or T.split(',')[1] in usToStr:
            T = 'USA'
        else:
            T = "Others"
    else:
        T = "Others"
    return T


def checkl2(T):
    geolocator = Nominatim(user_agent='geonames@googlegroups.com')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    location = geocode(T)
    if location is None:
        country = 'Others'
    else:
        country = location.address.split(',')[-1].strip()
        if 'United States' in country :
            country = 'USA'
        elif 'India' in country :
            country = 'India'
        elif 'Bangladesh' in T or 'Bangladesh' in country :
            country = 'Bangladesh'
        else:
            country = 'Others'
    return country

# result = df['user_location'].dropna().apply(checkl2)
# result.to_csv('result.csv')
# print(result)


user_locations = set(df.user_location)
user_locations.remove(np.nan)
user_locations = list(user_locations)

logging.info(f"# rows in df {len(df)}")
logging.info(f"unique user locations in df {len(user_locations)}")


# uloc_dict = {uloc: checkl2(uloc) for uloc in user_locations}

def get_location_from_streetmap():
    chunk_size = 100
    for i in range(0, len(user_locations), chunk_size):
        base_name = f'locations_{i}_{chunk_size}'
        logging.info(f"Creating {base_name}")
        uloc_dict = {uloc: checkl2(uloc) for uloc in user_locations[i:i + chunk_size]}
        logging.info(f"Writing {base_name}")
        with open(f'{base_name}.py', 'w') as f:
            print(uloc_dict, file=f)

        with open(f'{base_name}.yaml', 'w') as f:
            data = yaml.dump(uloc_dict, f)


def get_locations_from_yaml(file='all_locations.yaml'):
    with open(file) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def get_differences(location_dict):
    f = open("diff.txt", 'w')
    for user_loc, category in location_dict.items():
        checkl_result = checkl(user_loc)
        if category != checkl_result:
            # print(f"{checkl_result:6} {checkl1(user_loc):6} {category:6} <= {user_loc}", file=f)
            print(f"{checkl_result:6} {category:6} <= {user_loc}", file=f)
    f.close()


if __name__ == '__main__':
    location_dict = get_locations_from_yaml()
    get_differences(location_dict)

    # print(len(location_dict))
    # result = df.user_location.dropna().apply(lambda x: location_dict[x])
    # result2 = df.user_location.dropna().apply(checkl)
    # print(result2.value_counts())
    # print(result.value_counts())
