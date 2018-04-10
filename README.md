# Social network data retrieval and analysis

This repository contains accompanying code to the research paper titled "Social media comparison and analysis: The best data source for research?" that will appear in [RCIS 2018](http://www.rcis-conf.com/) proceedings. The work was done by Dr. Neli Blagus.

## Installation & run

1. Install required Python 2.7 libraries: `pip install requirements.txt`.

2. Install MySQL database using scripts in folder `database_schema`.

3. Go to python console and issue the following commands:

    ```
    import nltk
    nltk.download("stopwords")
    nltk.download("names")
    ```
4. Download the Slovene stopwords (`stopwords-sl.txt`) from [https://github.com/stopwords-iso/stopwords-sl](https://github.com/stopwords-iso/stopwords-sl) and save it as a file in `%HOME-FOLDER%/nltk-data/corpus/stopwords/slovenian`.

5. Update paths and credentials in `config.py`

6. Run `main.py` and wait for the server to start.

7. Navigate to [http://localhost:5000](http://localhost:5000) to retrieve your custom data, analyze or export it!
