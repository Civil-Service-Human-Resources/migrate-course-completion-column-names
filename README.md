# Migrate course completion column names

A simple script to update course completions data with the full organisation name and the grade name

## Running

As always, first run `pip install -r requirements.txt`

Then set the environment variables (in a `.env` preferrably):

```
MYSQL_HOST
MYSQL_USER
MYSQL_PASSWORD
PG_HOST
PG_PASSWORD
PG_USER
```

Finally, run the script `python script.py`