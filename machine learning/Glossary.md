# Glossary

---

**.merge()** — A relational-style merge that matches rows using one or more key columns.

**.filter()** — A method for filtering by row or column labels, including partial matches (like) and regular expressions (regex).

**.iloc()** — A method of a Pandas DataFrame used to refer to an entire row based on the RangeIndex number of the row.

**.loc()** — A method of a Pandas DataFrame used to refer to an entire row based on the labeled index value of the row.

**.read_json()** — Pandas method to read JSON formatted data.

**.to_json()** — Pandas method to write JSON formatted data.

**5% Rule** — A common guideline in data preprocessing stating that each category should represent at least five percent of the dataset to support meaningful analysis.

**Absolute Path** — The complete path from the base of your file system to the file that you want to load.

**Accuracy** — The proportion of all predictions that are correct: (TP + TN) / (TP + TN + FP + FN).

**Adjusted R-squared** — A version of R-squared that accounts for the number of predictors in the model.

**Analytics Bias** — Occurs when a data set is incomplete because it is lacking features that would logically explain important variance in the label of interest.

**Application Programming Interfaces (APIs)** — A structured interface that allows one software system to request and receive data from another system in a standardized way.

**Association Rules (a.k.a. Market Basket Analysis)** — Find relationships between sets of items from transactions.

**AUC** — Area Under the ROC Curve; the probability that the model assigns a higher risk score to a randomly chosen positive case than to a randomly chosen negative case.

**Autocorrelation** — Correlation between observations caused by time ordering or sequence dependence.

**Automation** — The process of replicating human effort and decision-making in programming code.

**Bag of Words** — A simplified document representation that treats text as a collection of tokens, typically ignoring grammar and word order, and often using token counts for modeling.

**Classification Modeling** — Assigning items in a collection to predefined target categories or classes with the goal of accurately predicting the class membership for each case in the data.

**Cluster Analysis** — A form of unsupervised machine learning that assigns cases to groups such that the distance between cases from the center of their assigned group is minimized while the distance between cluster groupings is maximized.

**Coefficients (β)** — Estimated weights that quantify the independent effect of each feature on the label after controlling for all other features in the model.

**Coherence** — The degree to which a set of words or phrases are semantically related and interpretable as a unified concept.

**Collaborative Filtering** — A recommender system technique that makes automated predictions about consumer preferences for new products or services which they have never tried or rated, based on their ratings or preferences from a subset of products which they have tried/used/rated.

**Confidence** — In association analysis, the count of item combination occurrences divided by the number of occurrences of one of the items in the combination; calculated for each item in the combination.

**Confirmation Bias** — Occurs when people only examine or consider data that fits their own expectations, desires, or beliefs and ignores data that is contrary to their hypotheses.

**Confusion Matrix** — A table that compares predicted classes to actual classes to summarize all correct and incorrect classification outcomes.

**Constructor** — A special type of subroutine called to create an object. It prepares the new object for use, often accepting arguments that the constructor uses to set required member variables.

**Corpus** — A collection of documents used as the input for text analytics and NLP tasks such as topic modeling.

**CRISP-DM** — A methodology for understanding how business problems are solved using data-based solutions.

**Cross-validation** — A resampling-based evaluation technique that repeatedly splits the training data into different train/validation folds to produce multiple performance estimates, allowing you to measure both typical model performance and its variability.

**Data Preparation Phase** — The third phase of CRISP-DM, which covers all activities used to construct the final dataset that will be fed into modeling tools.

**Data Understanding Phase** — The second phase of CRISP-DM, which focuses on initial data collection and activities designed to help you become familiar with the data, identify data quality problems, discover early insights, and detect interesting subsets for hypothesis generation.

**Deployment Phase** — The sixth phase of the CRISP-DM process, in which analytical results are delivered as decision-support artifacts or operationalized as models embedded within production systems.

**Dictionary** — A mapping from each unique token (word or n-gram) to an integer ID used by the topic model.

**Document** — Any single unit of unstructured text to be analyzed.

**Dummy Codes** — Binary (0/1) variables that represent category membership for a categorical feature.

**Dynamic** — In software, code that will work uninterrupted regardless of the amount or type of inputs that are provided.

**Endpoint** — The location from which each of the functions offered by REST Web Service APIs can be accessed.

**Enumerate()** — Python function that adds a counter to an iterable object (e.g., list, dictionary, DataFrame) so that you can keep track of the index.

**Error-resistant** — In the context of software, refers to code that will work uninterrupted even if the user attempts to submit invalid inputs, either by (1) specifying a better form for the inputs or (2) adapting or modifying the inputs to an acceptable form.

**Euclidean Distance** — A distance measure calculated as the square root of the sum of squared differences across each dimension of two observations.

**Evaluation Phase** — The fifth phase of CRISP-DM, which focuses on determining whether candidate models meet technical, business, and operational criteria required to justify deployment.

**Exploratory Data Analysis (EDA)** — The process of performing initial data investigations to discover patterns, spot anomalies, test hypotheses, and check assumptions using summary statistics and visualizations.

**F1-score** — The harmonic mean of precision and recall: 2 × (precision × recall) / (precision + recall).

**Faulty Interpretations** — Occurs when people make interpretations from data analyses that are better explained by either a simpler interpretation or one that has fewer logical leaps.

**Feature Scaling** — A transformation that adjusts the numeric range of feature values so they are comparable across predictors.

**Feature Selection** — The process of choosing which available features to include in a model and which to remove in order to improve model performance, prevent overfitting, or ensure valid interpretation of results.

**Hybrid Recommender Systems** — Recommendation algorithms that combine elements from both collaborative- and content-based concepts to best address all scenarios.

**JSON Package** — A Python package used to convert strings into JSON dictionaries.

**Latent Dirichlet Allocation (LDA)** — A generative probabilistic model that explains documents as mixtures of hidden topics, where each topic is a probability distribution over words.

**Latent Structure** — Patterns or relationships in data that are not directly observed but are inferred from statistical regularities.

**Lemma** — The base or dictionary form of a word.

**Lift** — In association analysis, the confidence of a given combination divided by the ratio of how often a potential item to add to that combination occurs in all transactions.

**Log Loss** — A probabilistic classification metric that penalizes confident wrong predictions more than uncertain predictions; lower values indicate better probability estimates.

**Mean Absolute Error (MAE)** — The average absolute difference between predicted and actual values.

**Model** — A formula — typically composed of a set of weights, a constant, and an error term — that estimates the expected value of an outcome given input features.

**Model Drift** — The gradual degradation of model performance as data patterns change over time.

**Modeling** — The process of developing mathematical or computational functions that quantify the relationship between multiple input features and an outcome of interest.

**Modeling Phase** — The fourth phase of CRISP-DM, which involves applying statistical and machine learning algorithms to prepared data in order to predict, classify, or discover patterns in an outcome of interest.

**Multicollinearity** — The presence of strong correlations among independent variables.

**Multiple Linear Regression (MLR)** — A modeling technique that estimates the relationship between a single label and two or more features simultaneously.

**N-grams** — Phrases made of n tokens that appear together in order within a corpus (for example: bigrams, trigrams, and fourgrams).

**Named Entity Recognition (NER)** — A natural language processing technique that identifies spans of text that refer to real-world entities and classifies them into categories such as PERSON, ORG, GPE, DATE, and TIME.

**Natural Language Processing (NLP)** — A collection of algorithms and techniques used to analyze, interpret, and generate human language.

**Normalization** — A general term for rescaling features to a common numeric range.

**OAuth** — An open standard for delegated access that allows clients to authenticate securely without placing secrets in the URL.

**Optical Character Recognition (OCR)** — The identification of printed characters using photoelectric devices and computer software.

**Out-of-sample Predictions** — Predictions for new cases that were not used during model training.

**Overfitting** — A modeling problem where the model learns patterns specific to the training data that don't generalize to new data, resulting in good training performance but poor test performance.

**Permutation Feature Importance (PFI)** — A model-agnostic technique that measures feature importance by randomly shuffling each feature's values and observing how much model performance degrades. Larger degradation indicates higher importance.

**Perplexity** — A statistical measure of how well a probability model predicts a sample.

**POS (Part of Speech)** — A coarse-grained grammatical category such as noun, verb, or adjective.

**Precision** — The proportion of predicted positive cases that are truly positive: TP / (TP + FP).

**Recall** — The proportion of actual positive cases that are correctly identified: TP / (TP + FN).

**Recommendation Engines** — A sub-class of machine learning models that identify items that a user may prefer — but has not yet tried or experienced — based on their ratings of other items (collaborative filtering) and/or the feature attributes of items they have previously rated (content filtering).

**Relative Path** — The path to a file if you start from your current working directory.

**Request Parameter** — Data sent to a URL in the querystring in a dictionary (key=value) format.

**RESTful Web Services** — A web-based API style that uses standard HTTP requests and URLs to access structured data resources.

**ROC Curve** — A plot showing the tradeoff between true positive rate (recall) and false positive rate as the classification threshold varies.

**Root Mean Squared Error (RMSE)** — The square root of the average squared prediction error.

**Satisficing** — A decision-making concept in which an option is selected not because it is optimal, but because the additional cost of improving it outweighs the additional value gained.

**Sentiment** — The emotional tone expressed in text, often summarized as negative, neutral, and positive intensity.

**Sort** — An action that changes the order of records in a dataset based on rules.

**Sort_index()** — Pandas DataFrame method used to sort objects by labels along either rows or columns.

**Sort_values()** — Pandas DataFrame method used to sort objects by the actual data values in one or more columns (or rows).

**Sparse** — When referring to a vector or matrix, most of the values are null or zero while a few of the values are high or filled in.

**Standard Error** — The estimated standard deviation of a coefficient's sampling distribution, reflecting the precision of the coefficient estimate.

**Standardization** — A specific form of scaling that transforms values into z-scores.

**Stationarity** — The assumption that the statistical properties of a time series, such as its mean and variance, remain constant over time.

**Supervised** — Machine learning algorithms that require a label.

**Support** — In association analysis, the count of item combination occurrences divided by the total number of transactions.

**T-SNE Clustering** — A non-linear dimensionality reduction technique that projects high-dimensional data into a low-dimensional space while preserving local neighborhood similarity.

**Term Frequency - Inverse Document Frequency (TF-IDF)** — A statistic used in natural language processing and information retrieval that measures how important/unique a term is within a document relative to the overall document collection (i.e., corpus).

**Text Analytics** — The process of deriving structured, high-quality information from raw text.

**Text Hashing** — The process of transforming raw text into standardized numeric features suitable for modeling.

**Tokenization** — The process of splitting unstructured text documents into individual tokens, such as words, punctuation, numbers, symbols, or emojis, according to language- and model-specific rules.

**Topic** — A probability distribution over words that frequently occur together across documents.

**Topic Modeling** — A technique used to discover recurring themes in a collection of documents without predefined labels.

**Topic Modeling Corpus** — A list where each document is represented by (token_id, count) pairs derived from the dictionary.

**Tukey Box Plot** — A particular case of a box plot intended for skewed distributions where the max/min "whiskers" of the plot are defined as the lowest/highest data point still within 1.5 × interquartile range (Q3 - Q1).

**Under-represented Populations** — Sub-populations that are under-sampled in the dataset used to form analyses, conclusions, and/or predictions.

**Unsupervised** — Machine learning algorithms that do not require a label.

**Unsupervised Modeling Technique** — A modeling approach that discovers structure in data without using labeled outcomes.

**Vectorized Calculation or Operation** — A calculation that performs an element-wise operation on an entire column (or array) without explicitly looping over each row in Python.

**Zip()** — Python function that binds two columns together as an iterable tuple so that the index of each list matches up to represent common attributes of a single case.
