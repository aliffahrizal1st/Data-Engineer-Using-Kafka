# Data-Engineer-Using-Kafka

In this data engineer project using Kafka, it aims to meet the needs of the final project in the Data Engineer course in 2020. The basic concept of this project is that I want to create an end-to-end system in data coverage, with data engineer steps such as taking data that has been processed. exists (extract), transforms the data as desired (transform) and saves it into an existing database (load) and the machine learning (ML) creation process in the end. The data that I use in this whole process is a [pulsar star classification dataset](https://www.kaggle.com/colearninglounge/predicting-pulsar-starintermediate?select=pulsar_data_train.csv) that I got from Kaggle.

There are several program files that I use, such as:

- Ingestion and Producer
    - In this Ingestion and Producer file there is a data collection process that is entered into Kafka, by taking all variables related to the existing data.
- Target and Consumer
    - In this Target and Consumer file, there is the process of retrieving data that has previously been collected into Kafka and the process of transforming the entire existing data. For the stages there are:
        - Reads data according to group id in Kafka with JSON format and puts it into 'consumer' variable.
        - Extract the data in the 'consumer' variable into a df variable with the format of a pandas dataframe table.
        - Perform data transformations in the form of replacing empty values with nan values and changing the schema of the data type of each column.
        - Putting all the results into the pyspark dataframe so that it can be inserted into the database that has been created.
        - After being entered into the database, then the data is extracted again into a file with csv format.
- ML_Projek_Data_Engineering
    - And lastly, the ML_Projek_Data_Engineering file is a file that is intended to create machine learning processes using data that has already been processed. In this file I do the classification process with the Logistic Regression library and the MinMaxScaler normalization library library
