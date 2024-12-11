import json
from subprocess import run

import pandas as pd
import polars as pl
from ratelimit import limits, sleep_and_retry

start_urls = {
    "3K": "https://myschools.nyc/en/api/v2/schools/process/2/?page={page}&district=84&district=85&district=86&district=87&district=88",
    "Pre-K": "https://myschools.nyc/en/api/v2/schools/process/5/?page={page}&district=84&district=85&district=86&district=87&district=88",
    "Kindergarten": "https://myschools.nyc/en/api/v2/schools/process/4/?page={page}&district=84&district=85&district=86&district=87&district=88&other_features=301&other_features=302&other_features=303&other_features=304&other_features=305",
}


@sleep_and_retry
@limits(calls=1, period=1)
def do_request(url, page):
    response = run(
        f"curl '{url.format(page=page)}'",
        shell=True,
        capture_output=True,
        check=True,
        text=True,
    )
    return json.loads(response.stdout)


for entry, url in start_urls.items():
    print(f"Downloading schools for {entry}")
    all_results = []
    page = 1
    while True:
        print(f"...requesting page {page}")
        data = do_request(url, page)
        results = data["results"]
        all_results.extend(results)
        if data["next"] is None:
            break
        page += 1
    df = pl.DataFrame(pd.json_normalize(all_results))
    print(f"Downloaded {len(all_results)} schools for {entry}")
    simple_types = set([pl.Boolean, pl.String, pl.Float64, pl.Int64])
    simple_columns = [col for col in df.columns if df.schema[col] in simple_types]
    df.select(simple_columns).select(
        [
            "id",
            "name",
            "overview",
            "subway",
            "bus",
            "email",
            "telephone",
            "independent_website",
            "secondary_website",
            "start_time",
            "end_time",
            "uniform",
            "contact_name",
            "school.name",
            "school.district.name",
            "school.address.address_1",
            "school.address.city",
            "school.address.state",
            "school.address.zip_code",
            "school.address.latitude",
            "school.address.longitude",
        ]
    ).write_csv(f"{entry}.csv")
