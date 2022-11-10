<CapstoneProject>
This projects, which is the final project of the Udacity Data Engineering Nanodegree, aims to build a data pipeline by gathering, transforming and uploading data. The used datasources were:
    
    * US I94 immigration data 
    * US demographics  
    * Airport Code Table
    * Temperature data
    
**What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?**
    > The goal is to create a DWH to have accessible data for having a greater basis for analysis.
    > Spark is and Airflow could be incorporated for updating the databases on regular basis with new data
    > Since the immigration data contains fact information, the star schema was evident, but with additional data sources it could also be changed to a snowflake schema
    

**Clearly state the rationale for the choice of tools and technologies for the project.**
> In the first stage PySpark is used to ease data exploration and preprocessing. It is usefull to efficiently load and
    manipulate huge datasets on a fast basis because its easy to scale horizontally. Instead of pandas dataframes, I'd recommend using Spark dataframes to allow distributed processing using for example Amazon Elastic Map Reduce (EMR), at a later stage. Also in order to perform continuos updates I'd recommend implement this ETL pipeline into an Airflow DAG.

> Furthermore a Jupyter Notebook was used to visualize the data structure and the need for data cleaning. PySpark is one of the most common programming language and was used for this project.
    
**Propose how often the data should be updated and why.**
    > Since the immigration data is on a monthly base, updating the data on a monthly base is recommended.
    
**Include a description of how you would approach the problem differently under the following scenarios:**
** If the data was increased by 100x. ** 
       > Instead of Pandas use Spark to process the data efficiently utilizing the distributed computation ability of Spark. For
    write heavy operations, I'd recommend using Casandra instead of PostgreSQL
**If the pipelines were run on a daily basis by 7am.**
    > In this case its recommended to use AIRFLOW to create a DAG that executes every morning at 7am.
**If the database needed to be accessed by 100+ people.**
    > For this pupose Amazon S3 in combination with Amazon Redshift is recommended to make the data efficiently accessible for +100 people.
