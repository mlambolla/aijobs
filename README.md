This project is intended to test data engineering skills and tools.

Conceptually, the process consists of generating a program that extracts detailed information from available jobs of an artificial intelligence job board website on a daily basis.

Then a sub-process compares new items added from 'current' data (t) versus the previous one (t-1). Finally the differences are recorded and the data is available to be consumed from a GUI platform. In this case I used Streamlit to quickly display aggregated information [here](https://share.streamlit.io/mlambolla/aijobs/main/.streamlit/st_aijobs.py).

## Tech
- Python
- Apache Airflow
- Pandas
- Selenium
- AWS (S3 and Redshift)
- PostgreSQL
- Streamlit


Technical steps are described in the Apache Airflow sub-project folder.
