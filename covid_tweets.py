from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter  # https://geopy.readthedocs.io/en/stable/#usage-with-pandas
import logging
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format='{asctime} {message}', style='{')
df = pd.read_csv("covid19_tweets.csv")


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

logging.info(f"# rows in df {len(user_locations)}")
logging.info(f"unique user locations in df {len(user_locations)}")


# uloc_dict = {uloc: checkl2(uloc) for uloc in user_locations}

chunk_size = 100
for i in range(0, len(user_locations), chunk_size):
    base_name = f'locations_{i}_{chunk_size}'
    logging.info(f"Creating {base_name}")
    uloc_dict = {uloc: checkl2(uloc) for uloc in user_locations[i:i+chunk_size]}
    logging.info(f"Writing {base_name}")
    with open(f'{base_name}.py', 'w') as f:
        print(uloc_dict, file=f)

    with open(f'{base_name}.yaml', 'w') as f:
        data = yaml.dump(uloc_dict, f)

# for uloc in df.user_location[:99]:
#     print(f"{uloc} => {checkl2(uloc)}")