# RDF_Extractor

This open source library was created in order to generate rdf tuples from a text corpus. It aims at extracting tuples from twitter's data (mostly) which must be stored in a specific way. It can also be used for any other dataset which fullfil the below description of indices.

- We have extracted entities from text (tweets) using FEL implementation (threshold was set to -3).
- For sentiment annotation of the text we have used SentiStrength implementation.  

The implementation was done in scala and Java for distributed environment: Apache Spark and Apache Hadoop. 

# Dataset Description

Each instance of the dataset must be stored in a new line. Each attribute of an instance has to be separated by tab character ("\t").
Below is the list with the indices for each attribute:

0. Document Id: Long
1. Username: String. In our case we have encrypted this field for privacy issues.
2. timestamp: Format ( "EEE MMM dd HH:mm:ss Z yyyy" )
3. #Followers: Integer
4. #Friends: Integer
5. #Retweets: Integer
6. #Favorites: Integer
7. Entities: String. For each entity we aggregated the original text, the annotated entity and the produced score from FEL library. Each entity is separated from another entity by char ";". Also each entity is separated by char ":" in order to store "original_text:annotated_entity:score;". If FEL did not find any entiites then we have stored "null;"
8. Sentiment: String. Sentistrength produces a score for positive (1 to 5) and negative (-1 to -5) sentiment. We splited these two numbers by whitespace char " ". Positive sentiment was stored first and then negative sentiment (i.e "2 -1").
9. Mentions: String. If text contains mentions we remove the char "@" and concatenate the mentions with whitespace char " ". If no mentions appear on text then we have stored "null;"
10. Hashtags: String: If text contains hashtags we remove the char "#" and concatenate the hashtags with whitespace char " ". If no hashtags appear on text then we have stored "null;"
  
  
# Deployment

After you generate the jar file you can deploy the code on spark distributed system.

We provide a class for generating the tuples and also some other classes for generating statistics from the dataset.

Class 'RdfExtractor' is responsible for generating the tuples. As arguments it receives:

0. Directory of your dataset (String)
1. Percentage of the dataset's input float (0, 1] (if you want full dataset set to 1)
2. output directory

i.e "spark-submit --class RdfExtractor file_name.jar input_directory/ 100 output_directory/"

