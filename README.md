# Glassdoor-Crawler
Build a Glassdoor Job Crawler and set up a Task Scheduler to run it on a daily basis.

## Import Packages
Simply use `requests` and `beautifulsoup4` to crawl jobs from Glassdoor.
```python
import requests
import random
import sqlite3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
from bs4 import BeautifulSoup
```

## User Agent
A user agent is a string that a browser or application sends to each website you visit.
I collected a list of user agent for later use.
```python
def get_user_agent(filename="headers.txt"):
    with open(filename) as f:
        USER_AGENT_LIST = f.readlines()
    USER_AGENT_LIST = [x.strip() for x in USER_AGENT_LIST]
    return USER_AGENT_LIST
```

## Main Code
Let the rubber hit the road!
```python
def crawl_glassdoor(queries):
    """
    Args:
        queries (:obj: list):
            List of search queries.

    Returns:
        data (:obj: pd.DataFrame):
            DataFrame object contains four columns.
            "title", "location", "company", "link"
    """
    job_list = []
    for query in queries:
        pages = find_total_page(query)
        for page in tqdm(range(pages), desc=query):
            base = "https://www.glassdoor.co.uk"
            query = query.replace(" ", "-")
            url = base + f"/Job/{query}-jobs-SRCH_KO0,{len(query)}_IP{page+1}.htm"
            USER_AGENT_LIST = get_user_agent()
            USER_AGENT = random.choice(USER_AGENT_LIST)
            headers = {"user-agent": USER_AGENT}
            res = requests.get(url, headers=headers)
            soup = BeautifulSoup(res.text, "html.parser")
            job_section = soup.find("article", attrs={"id": "MainCol"}).find("ul")
            for job in job_section.find_all("li"):
                try:
                    title = job.get("data-normalize-job-title")
                    location = job.get("data-job-loc")
                    company = job.find("div", attrs={"class": "jobHeader"})
                    company = company.find("a").find("span").text
                    link = job.find("a", attrs={"class": "jobTitle"}).get("href")
                    link = "https://www.glassdoor.co.uk" + link
                    job_list.append([title, location, company, link])
                except:
                    pass
    data = pd.DataFrame(
        job_list, columns=["title", "location", "company", "link"])
    return data


def find_total_page(query):
    base = "https://www.glassdoor.co.uk"
    query = query.replace(" ", "-")
    url = base + f"/Job/{query}-jobs-SRCH_KO0,{len(query)}.htm"
    USER_AGENT_LIST = get_user_agent()
    USER_AGENT = random.choice(USER_AGENT_LIST)
    headers = {"user-agent": USER_AGENT}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    footer = soup.find("div", attrs={"id": "ResultsFooter"}).find_all(text=True)
    total_page = footer[0].split("of ")[-1]
    return int(total_page)
```

## Daily Crawler
Create a crawler for Glassdoor on a daily basis, and insert the dataframe into a database.
```python
def daily_glassdoor_crawler(queries):
    data = crawl_glassdoor(queries)
    database_path = 'sqlite:///data/data.db'
    engine = create_engine(database_path, echo=False)
    data.to_sql("glassdoor", con=engine, if_exists='replace')
    engine.dispose()
```

Also, you can read the dataframe from the database.
```python
def read_glassdoor_from_db():
    """
    Read stock table from database using sqlalchemy.
    """
    database_path = 'sqlite:///data/data.db'
    engine = create_engine(database_path, echo=False)
    data = pd.read_sql_table("glassdoor", database_path)
    engine.dispose()
    data.drop("index", axis=1, inplace=True)
    return data
```

## Run Code
```python
def main():
    queries = [
        "nlp", "machine learning", "deep learning", "data scientist", 
        "natural language processing", "data engineer", "data analyst"
    ]
    daily_glassdoor_crawler(queries)


if __name__ == "__main__":
    main()
```

## Setup Task Scheduler
If you are a Windows user, you can crawl jobs from Glassdoor everyday using Task Scheduler.
1. Open Task Scheduler.
2. `Actions > Create Task`
3. In `General` tab: 
    - Name: Glassdoor Crawler
    - Choose `Run whether the user is logged on or not`
    - Hit `Run with highest privileges `
    - Hit `Hiddden`
4. In `Triggers` tab and hit `New` button:
    - Depends on your preference.
    - Hit `Enable`
5. In `Actions` tab and hit `New` button:
    - Action: `Start a program`
    - Program/script: `C:\path-to-your-py-file\glassdoor.py`
    - Add arguments (optional): `C:\path-to-your-py-file`
    - Start in (optional): `glassdoor.py`
    - Click `OK`
6. In `Conditions` tab:
    - Uncheck `Start the task only if the computer is on AC power`
    - Hit `Wake the computer to run this task`
    - Hit `Start only if the following network connection is available`
7. In `Settings` tab (optional): it is the setting when you meet the task fail.
