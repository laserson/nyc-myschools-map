# nyc-myschools-map
Extracting school data from NYC MySchools API and mapping it

If you open

```
https://myschools.nyc/en/schools/3k/
```

you can see the API requests using devtools. They look something like this:

```
https://myschools.nyc/en/api/v2/schools/process/2/?page=1&district=85
```

The district numbering/args are a little weird. You can see the mapping if you
look at the returned results.

```
python download.py
```

Then upload the csv files to Google MyMaps.
