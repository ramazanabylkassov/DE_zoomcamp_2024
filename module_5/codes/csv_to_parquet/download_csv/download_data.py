import os

colors = ['yellow', 'green']
years = ['2020', '2021']

for color in colors:
    for year in years:
        os.system(f'./download_data.sh {color} {year}')
