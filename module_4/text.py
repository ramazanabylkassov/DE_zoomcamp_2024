months = [
    '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'
]
years = ['2019', '2020']
urls = []

for year in years:        
    for month in months:
        urls.append(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz')

for url in urls:
    print(url)