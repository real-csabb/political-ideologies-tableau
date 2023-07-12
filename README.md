# Orchestrating Political Ideologies Tableau with a Data Pipeline
## Created by Chris Sabb

[Link to the WIP Tableau visualization](https://public.tableau.com/app/profile/chris.sabb/viz/PoliticalIdeologiesMapview/ClassicObservable)

In Dr. Mark McClure's *Dynamic Data Visualization* class last semester, we explored voteview.com's political ideologies data as implemented by Keith T. Poole, Howard Rosenthal, and Boris Shor. I found the data particularly interesting, so I've continued to explore it. Ideological positions (as explored in this project) are calculated using the DW-NOMINATE (Dynamic Weighted NOMINAl Three-step Estimation) and is a "scaling procedure" representing legislators on a spatial map--effectively, representing the closeness of two legislators based on how similar their voting records are.

I originally orchestrated [this project on Obsevable](https://observablehq.com/d/f318751be649b1fd?collection=@real-csabb/notable-ddv-assignments). This version however is ochrestrated on Tableau. This version I had wanted to implement using an Apache Airflow pipeline and a MySQL database all orchestrated on Amazon Web Services. However, as far as I know, Tableau *does not* support Apache Airflow for fetching data. Therefore, I implemented a [rudimentary data pipeline](https://github.com/real-csabb/political-ideologies-tableau/blob/main/political-ideologies-tableau.py) using a GCP service account, the Google Drive and Google Sheets APIs, and [GitHub Actions workflows](https://github.com/real-csabb/political-ideologies-tableau/blob/main/.github/workflows/politicalIdeologiesDataPipeline.yaml).

## How to Setup 
Some prerequisites for this is to enable a service account in GCP and to create a Google Spreadsheets database in Google Drive. [Clare Gibson's similar implementation in R](https://github.com/clarelgibson/tableau-public-autorefresh/wiki/2.-Set-up-GCP-services) was invaluable in setting these things up.

After that, simply clone this repository and add your Google authentication JSON and your Spreadsheet ID (e.g in https://docs.google.com/spreadsheets/d/1ITH6oNHsIlVHo2LJnR92wP5LEKiON0k2rZJ82YbYaB0/, the 1ITH6oNHsIlVHo2LJnR92wP5LEKiON0k2rZJ82YbYaB0) to your repostiory secrets. Then pull your repository down, activate the virtual environment in venv/bin/activate, and run `$ pip install -r requirements.txt`. 

After that, it's just a matter of running the Python script and watching your Google Spreadsheet database update!
