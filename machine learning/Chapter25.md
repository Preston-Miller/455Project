# Chapter 25: Recommendation: Collaborative Filtering

## Learning Objectives

- Students will be able to apply association rules (support, confidence, lift) to identify product affinities in transaction data
- Students will be able to build user-item rating matrices and normalize ratings to account for user bias
- Students will be able to implement memory-based collaborative filtering using similarity metrics (Pearson correlation, cosine similarity) and k-nearest neighbors
- Students will be able to generate and evaluate user-based recommendations from collaborative filtering models

---

## 25.1 Introduction

**Recommendation engines** — a sub-class of machine learning models that identify items that a user may prefer--but has not yet tried or experienced--based on their ratings of other items (a.k.a. "collaborative filtering") and/or the feature attributes of items they have previously rated (a.k.a. "content filtering"). are one of the most useful machine learning models for business applications. Recommendations models are used by Amazon and many others to predict which products a consumer may prefer even if they haven't used those products before. Social media and news outlets use recommender models to predict which posts or stories you will be most interested in or that will keep your attendtion the longest. They can be used to predict which songs, movies, books you may enjoy. Recommenders can predict which jobs you might be interested or which candidates would fit a company culture. There are 2-3 types of recommendation models that can be implemented with a variety of algorithms: collaborative, content, and hybrid (both collaborative and content).

---

## 25.2 Association Analysis (optional)

### Introduction

Before learning collaborative filtering in python, it may help to understand association rules. **Association rules (a.k.a. market basket analysis)** — find relationships between sets of items from transactions identify relationships among sets of elements based on sets of transactions. Association rules can work on transactions without tying together a set of transactions from the same user or consumer--which is typically the distinction of where what we call "collaborative filtering" begins. This section will give you a conceptual understanding of the process.

Watch and follow along with the video below. You may also want to download and open the Excel file after the video so that you can experiment with the model yourself.

#### How Does It Work?

Association analysis, or market basket analysis, identifies the strength of association between pairs of items, events, or people in order to identify common patterns of co-occurrence. A co-occurrence is when two or more things take place together. This could be products purchased together, social network relationships (of any type), or other connections between pairs.

The resulting association analysis creates a set of if-then scenario rules. For example, if item A is purchased then item B is likely to be purchased. The rules are probabilistic in nature or, in other words, they are derived from the frequencies of co-occurrence in the observations. Frequency is the proportion of baskets that contain the items of interest. This results in a set of trained rules that can then be used to make decisions. In the past, these rules took the shape of product development strategies, product placement, and various types of cross-selling strategies. However, because of the ease of automating association analysis into machine learning apps, websites, and software, filtering rules get created for online product recommendation, online social network connection recommendations, and more.

In order to make it easier to understand, think of association rules in terms of shopping at a store. Association analysis takes data at transaction level, which lists all the items bought by a customer in a single purchase. Let's use a simple example of a store that sells root beer, diapers, bananas, and Oreos exclusively. We will begin by calculating the total number of product combinations that can be purchased. To do this we use the formula for combination (without replacement)—(n!/r!\*(n - r)!)—to sum the number of possible combinations for every number of possible items in a purchase.

Therefore, in the case above, there are 14 possible product combinations. The next step is to analyze the historical purchase data to determine the count of each product combination that has occurred. We use that data to calculate the **support** — in association analysis, it is the count of item combination occurrences divided by the total number of transactions, for each combination, which refers to the count of item combination occurrences divided by the total number of transactions.

![Historical Shopping Cart Data](../Images/Chapter25_images/recommender_shopping_carts.jpg)

**Formula for Support:**

Support

=

A

- B

Total

"What percent of the time does this combination appear across all carts?"

Support essentially tells us how often a product combination occurs relative to all other combinations. The next step is to calculate the **confidence** — in association analysis, it is the count of item combination occurrences divided by the number of occurrences of one of the items in the combination; thus, it is calculated for each item in the combination, which refers to the count of item combination occurrences divided by the number of occurrences of one of the items in the combination; thus, it is calculated for each item in the combination.

**Formula for Confidence:**

Confidence

=

A

- B

A

"What percent of A purchases include B?"

Confidence is the first of two calculations used to create a set of if-then rules for each product combination. Confidence is used to estimate the likelihood that a particular item will be added to that combination. The second calculation is **lift** — in association analysis, it is the confidence ((A + B) / A) of a given combination divided by the ratio of how often a potential item to add to that combination occurs in all transactions.. List is the confidence ((A + B) / A) of a given combination divided by the ratio of how often a potential item to add to that combination occurs in all transactions. Therefore, as with confidence, it is calculated for every possible if-then rule.

**Formula for Lift:**

"What percent of the time does B appear with A compared to all the other possible products that might appear with A?"

![Table of Rules, Confidence, and Lift](../Images/Chapter25_images/recommender_rules_confidence_lift.jpg)

With the rules in place, recommendations can be determined for every combination of items. In the context of our simple four-item store, the recommendations for each combination are sorted by the lift score of each item from highest to lowest.

Furthermore, we can calculate the accuracy of these recommendations by comparing the recommended rank-sorted list for each combination against the actual items in each occurrence (i.e., shopping cart or receipt). In this case, we will assign a score of 3 if the second item in the cart was the first item recommended, 2 if the second item was the second recommended, and 1 if it was the third recommendation. For carts that contain three items, the third item will receive a similar rating. Therefore, the accuracy score is calculated as the (sum of awarded points) / (sum of potential points) = 32 / 39 = 82.1%.

![Recommendation Accuracy](../Images/Chapter25_images/recommender_accuracy.jpg)

---

## 25.3 Collaborative Filtering

Association analysis and the resulting rules are ideal for predicting individual shopping selections within larger transactions (which include multiple products) in order to assist with product placement on shelves. Although we demonstrated in our example that these rules may be futher improved by recording each users transactions over time, that is typically how association rules differ from what we call "collaborative filtering". In other words, association rules typically do not consider users/consumers; instead, it focuses on individual products represented in transaction combinations. As a result, association rules make the most sense as a technique for determining product placement on "brick and mortar" store shelves. Recommender systems are a more modern technique (but very related to, and based upon, the association analysis technique) that takes the user/consumer history into consideration.

![Collaborative Filtering](../Images/Chapter25_images/cf_vs_aa.png)

The image above vizualizes the high-level differnce between association analysis and collaborative filtering. As you can see, the primary distinction of collaborative filtering is that ut includes the identification of the user or consumer which allows us to differentiate between products or services that have been purchased, tried, or consumed versus those that have not for each person.

As indicated in the introduction, there are two primary types of recommender systems. The type we will cover in this chapter, **collaborative filtering** — a recommender system technique that makes automated predictions about consumer preferences for new products or services which they have never tried or rated based on their ratings or preferences from a sub-set of products which they have tried/used/rated., makes automated predictions about consumer preferences for new products or services which they have never tried or rated based on their ratings or preferences from a sub-set of products which they have tried/used/rated. Because of the ease of collecting and aggregative user/consumer purchase histories over time, collaborative filtering is perfect for online or electronic accounts where purchase history can be used.

There are many ways to implement a collaborative filtering recommender system. We will only demonstrate one example in this chapter, but we can at least conceptually explain the two general approaches here: memory-based and model-based.

#### Memory-Based Approach

The memory-based approach is perhaps the oldest and most common for collaborative filtering. It involves identifying similar sets of users (which we will use from here on out to refer to consumers of the product or servies to be recommended) and similar sets of items (which we will use from here on out to refer to the products, services, media, or other objects to be recommended).

Collaborative filtering assumes that people who buy, consume, or rate some items similarly will likely buy, consume, or rate other items similarly. Different algorithms differ primarily in how we calculate and interprete what "similarly" means. But before we can perform those calculations, we have to generate a sparse matrix of which items have been bought, consumed, or rated by which people. A **sparse** — when referring to a vector or matrix, "sparse" refers to the fact that most of the values are null or zero while a few of the values are high or filled in matrix (or vector) is simply one where most of the values are null or zero. This applies to recommendation matrices because, when there are many users and many items, most users have only tried or experienced a relatively small proportion of the items available. Therefore, our matrix will have mostly null values.

The first step in creating a memory-based collaborative filtering model is to generate the user-item matrix. For example, let's say that we have five users and 10 movies, but each user has only watched 2-4 movies. We would generate a matrix that would look something like this:

In this table, people (i.e. users) are listed as p# down the rows and movies are m# across the columns. Once this matrix is generated for every movie and user in the dataset, we normalize all of their scores by subtracting the mean of the row from all of the scores. This prevents biases based on differences in user biases. For example, some people tend to _usually_ give very high ratings whereas others will balance their ratings or usually give very low ratings. The table below visualizes this normalization.

Next, we calculate a similarity score for each pair of rows to find those that are most similar to each other. This similarity score can be calculated in many ways including a simple Pearson correlation coefficient r, Jaccard coefficient, mutual information, or a vector cosine among others. In practice, you would select only the most accurate similarity statistic of those four (or something else entirely).

We won't actually calculate those scores here because there are so few movies and users above that the results wouldn't be meaningful. But the result would be an analysis of which users and items are most similar. For example, you can see by comparing the ratings that p1 is most similar to p2 and least similar to p3 and p4. We use these scores within a k-nearest neighbors (KNN) clustering algorithm to identify the k users who are most similar to each user. With these results, predictions can be made based on movies that each user has not seen based on the ratings of the k users who are most similar to them. This is called a user-item recommendation. We can also make item-item recommendations by simply grouping together the items that are rated most similarly. We will provide a full example of this in python in the next section.

The primary advantage of the memory-based approach to collaborative filtering is that it has a very high rate of accuracy if there are enough ratings. No domain knowledge or data about user or item features/characteristics is needed to achieve reliable recommendations. One of the primary disadvantages to the memory-based approach of collaborative filtering is that items that have not been rated by many users cannot be validly recommended. In other words, the "sparseness" of the user-item matrix depicted in the tables above creates an issue with calculating similarity scores.

#### Model-Based Approach

Model-based approaches solve the issue that memory-based approaches have with sparseness. Model-based approaches have been developed with machine learning algorithms to predict users' preferences for unrated items. It involves a technique known as dimensionality reduction to handle the sparseness of data based on methods like principal component analysis which allow many items to be reduced down to smaller set of items; thus, eliminating the sparseness issue. This has led to a "hybrid" approach where memory- and model-based approaches are used together to create the most accurate recommendations possible for items that have been newly introduced in addition to well-known and often-rated items.

We will not cover a model-based approach here because it would require a deeper discussion of some methods that we haven't learned in this book. Also, the content filtering technique we will cover in another chapter can fill the same need. Perhaps the primary takeaway from this discussion is that there are **many** techniques for recommendation modeling and the variety of options available could easily fill a thick book. We will cover the most common techniques here.

Next, let's learn how to implement a basic modeling-based approach to collaborative filtering in Python.

---

## 25.4 Problem Definition and Data Import

Let's build a simple, memory-based collaborative filtering algorithm in Python. To do this, we will use a movie ratings dataset that is readily available online from many places. The goal here is to recommend movies that someone would most likely want to see. We should have an option for recommending movies that are similar to a movie of interest (e.g. think of viewing a movie details page and finding recommendations below that movie's description). We would also like an option for the the "splash screen" when a user logs in before they are vieweing any movie details. This would be a list of movies they would want to see which a) they have never seen before, and b) is based on one or many prior movies that they have watched.

![Problem Definition and Data Import](../Images/Chapter25_images/df_netflix_cosine_matrix.png)

#### Data Import

Begin by downloading and decompressing the files below which include a user-item-rating triple (userId-movieId-rating) and another dataset of movie titles and genres. Import both datasets into separate DataFrames and examine the first five records of each as done below.

```python
# Don't forget to mount Google Drive if you haven't already:
# from google.colab import drive
# drive.mount('/content/drive')

import pandas as pd

df_triple = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/movies-small/ratings.csv')
df_triple.sort_values(by=['userId', 'movieId', 'timestamp'], inplace=True) # This sorting will matter later when we clean the data
df_triple.head()
```

![Problem Definition and Data Import](../Images/Chapter25_images/df_movies.png)

As we learned in the prior section, collaborative filtering requires having data in this format of having a user-item-rating triple--meaning that each row represents an instance of a particular user (i.e. customer) indicating preference for a particular product (i.e. movie) based on some sort of rating. Ideally, this would be an actual rating as it is in this case. However, we can use other measures as a proxy for a rating like the quantity of items purchase which we used in the prior section or time spent vieweing an item or the number of times visiting a web page. Sometimes you have to be creative to identify a rating, but are many good ideas.

Notice that we also have a timestamp. This can be useful if the data represents something that a user can rate repeatedly. For example, the same customer could rate a restaurant every time they eat there. In our case, a movie-goer could easily see the same movie twice and rate it twice. In these cases, which rating should we use? The first or the last? It depends on the context. If we want to recommend movies that a customer has never seend, then we should probably use everyone's first rating of a given movie to capture their initial impressions. If we want to recommend restaurants, then a customer might have a better idea of how much they like it after they have tried more of the options. In that case, I would keep their final rating. We will come back to this later when we clean the data.

Next, let's import the movie titles:

```python
df_movies = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/movies-small/movies.csv', index_col='movieId')
df_movies.head()
```

![Problem Definition and Data Import](../Images/Chapter25_images/df_movie_titles.png)

As you can see, this dataset is pretty straight-forward. We only need this dataset to give us movie titles to make the interpretation of our model a bit easier. We will ignore the genres for now. These are useful for content filtering models. But we will use a more complete dataset for that section that includes movie description, title, and more. For now, just ignore genres.

---

## 25.5 Data Understanding

#### Data Understanding

It helps to explore the data a bit to understand how it may need to be cleaned and what to expect. First, let's print out a histogram of the rating feature.

```python
import seaborn as sns
import matplotlib.pyplot as plt

sns.histplot(data=df_triple, x='rating', kde=True);
```

![Data Understanding](../Images/Chapter25_images/df_movies_explore_ratings.png)

The ratings are not fully continuous and only have 9 unique values. So the histogram KDE overlay does not look quite right. However, this data follows a typical distribution for ratings which is negatively skewed. This is common because people tend to only try things that they believe they will like: movies, restaurants, products, experiences. Therefore, the negative skewness is natural. Thankfully, the collaborative filter we will build does not depend on the assumption of a normal distribution.

Next, let's generate some statistics including the number of movies, users, ratings, average ratings per user, and average ratings per movie:

```python
n_ratings = len(df_triple)
n_movies = len(df_triple['movieId'].unique())
n_users = len(df_triple['userId'].unique())

print(f"Number of ratings: {n_ratings}")
print(f"Number of unique movieId's: {n_movies}")
print(f"Number of unique users: {n_users}")
print(f"Average ratings per user: {round(n_ratings/n_users, 2)}")
print(f"Average ratings per movie: {round(n_ratings/n_movies, 2)}")

# Output:
# Number of ratings: 100836
# Number of unique movieId's: 9724
# Number of unique users: 610
# Average ratings per user: 165.3
# Average ratings per movie: 10.37
```

These statistics are important because we need to have many ratings per item and per user in order to build an accurate recommender model. Movies that have not been rating many times will be difficult to recommend. Let's print out the value_counts() of movieId to see how many times each movie has been rated

```python
df_triple['movieId'].value_counts()

# Output:
# 356       329
# 318       317
# 296       307
# 593       279
# 2571      278
#          ...
# 86279       1
# 86922       1
# 5962        1
# 87660       1
# 163981      1
# Name: movieId, Length: 9724, dtype: int64
```

As you can see, many movies have only one rating. Let's identify the quartiles of the dataset to help us identify where to make the cutoff.

```python
print('Min:\t\t', df_triple['movieId'].value_counts().min())
print('Quartile 1:\t', df_triple['movieId'].value_counts().quantile(.25))
print('Median:\t\t', df_triple['movieId'].value_counts().quantile(.5))
print('Quartile 3:\t', df_triple['movieId'].value_counts().quantile(.75))
print('Max:\t\t', df_triple['movieId'].value_counts().max())

# Output:
# Min:		 1
# Quartile 1:	 1.0
# Median:		 3.0
# Quartile 3:	 9.0
# Max:		 329
```

Half of the movies have only received 3 or fewer ratings. We could run this analysis with that few. But it will be more accurate using only the movies in Quartile 3--those with 9 ratings or more. Keep in mind, this is not a "rule" we must follow. If the model we develop does not recommend relatively new movies, then our customers may not find our recommendations very useful. However, it should still be easy to find movies that that people haven't seen to recommend with only 9 ratings so let's proceed with data preparation next.

---

## 25.6 Data Preparation

#### Data Preparation

```python
# Make a list of movieIds that are not in the 3rd quartile

# Store the count of ratings for each movie
value_counts = df_triple['movieId'].value_counts()

# Make a list of those with more than 9 ratings
keep_list = value_counts[value_counts >= 9]
print(len(keep_list), 'movies with 9 or more ratings\n')
keep_list

# Output:
# 2441 movies with 9 or more ratings

# 356      329
# 318      317
# 296      307
# 593      279
# 2571     278
#     ...
# 3754       9
# 5247       9
# 37731      9
# 1300       9
# 1734       9
# Name: movieId, Length: 2441, dtype: int64
```

This is a more reliable data set. We have gone from 9724 movies down to 2441 with only those that have 9 or more ratings. Remember, if we want to take a chance recommending more movies that don't have as many ratings, then just change that '9' on like 7 to something lowers.

Next we need to filter df_triple down to only those records that appear in the keep_list object we just created.

```python
# Filter the original df_triple DataFrame down to only those movies in that list

# We can use the .isin() method to check if the movieId (which is the index of the keep_list) is in our drop_list
df_triple = df_triple.loc[df_triple['movieId'].isin(keep_list.index)]
print(df_triple.shape)
print("Ratings per movie:\t", df_triple.shape[0]/len(keep_list))

# Output:
# (82664, 4)
# Ratings per movie:	 33.86480950430152
```

Our df_triple DataFrame is now down to 82,664 records with an average of almost 34 reviews per movie. This dataset is almost ready for modeling. There is one more task at hand. Recall our discussion above about duplicate records. If the same person watches and rates the same movie more than once, what should we do with those repeat records? The three most obvious options are 1) calculate an average of all ratings of a given movie for a given user, 2) keep the first rating, or 3) keep the last rating. If we want to recommend movies that a user has never seen, then I suggest that we keep the first rating of each user to capture their first impressions of a movie. This is not too difficult using the .drop_duplicates() method of pandas DataFrames.

```python
# Check for duplicate rows but based only on userId and movieId
print(f"Duplicate ratings: {df_triple.duplicated(subset=['userId', 'movieId']).sum()}")

# Output:
# Duplicate ratings: 0
```

Apparently, this dataset did not have any duplicates. But now you know how to check for them. Let's go ahead and write code to clean duplicates and keep the first record in case the need ever arises with other datasets.

```python
# This is how we would clean the data if there were duplicates:

print(f'Total rows before dropping duplicates: {df_triple.shape[0]}')

# keep='last' if you want to keep the last record
# keep=False if you want to drop all duplicates
df_triple.drop_duplicates(subset=['userId', 'movieId'], keep='first', inplace=True)

# These print statements are just a 'sanity check' to make sure we truly didn't have duplicates
print(f'Total rows after dropping duplicates: {df_triple.shape[0]}')

# Output:
# Total rows before dropping duplicates: 82664
# Total rows after dropping duplicates: 82664
```

#### Data Understanding Revised after Cleaning

Now that we have done the basic cleaning, let's explore the data just a little more by generating bar charts of the most and least popular movies as well as the count of ratings for those records.

```python
# Visualize the average rating for each movie

# Group the rows into individual titles and calculate the mean rating for each row
movie_stats = df_triple.groupby(by=['movieId'])[['rating']].agg(['count', 'mean'])
movie_stats.columns = ['ratings_count', 'ratings_mean'] # Rename the columns to simplify the index

# Join with df_movies to get the titles
movie_stats = movie_stats.join(df_movies['title'])

# Sort the ratings
movie_stats = movie_stats.sort_values(by=['ratings_mean'], ascending=False)

# Create a smaller dataset of the top n and bottom n rated movies
df_reduced = pd.concat([movie_stats.head(20), movie_stats.tail(5)])

plt.figure(figsize=(10,4)) # Set the size of the figure
sns.barplot(data=df_reduced, x='title', y='ratings_mean')
plt.xticks(rotation=90)
plt.show()

plt.figure(figsize=(10,4)) # Set the size of the figure
sns.barplot(data=df_reduced, x='title', y='ratings_count')
plt.xticks(rotation=90)
plt.show()
```

![Data Preparation](../Images/Chapter25_images/movies_top_rated.png)

![Data Preparation](../Images/Chapter25_images/movies_count.png)

These carts above are mildly useful in just "sanity-checking" the data to see if the top and bottom rated movies are those that we would expect. More interesting is the fact that the ratings don't seem to correlate with how often they are watched. The second chart above shows that with the exception of "The Shawshank Redemption", the bottom rated movies seem to be reviewed more often. Perhaps this is because people are more likely to write a review if they didn't like a movie than if they did.

---

## 25.7 Modeling

#### Modeling Preparation

The modeling phase of recommender engines is much like all others in that there is a bit of modeling-specific data preparation required. For example, for supervised machine learning models, we have to divide the dataset into the X features and y label, dummy-code the categorical features, and also split the datasets into training and testing. Technically, those are "data preparation" tasks but I like to include them in the modeling phase because they are required **only** when performing modeling. Similarly, we have a data prep task next that is specific to recommendation modeling. We must build the user-item matrix along with dictionaries to map the user and item IDs in the datasets to the user-item matrix and then back again when we're ready to generate recommendations.

If you read the explanation of collaborative filtering in a prior section, then you recognize the user-item matrix as simply a table with the movies across the columns and the users down the rows. It seems like a Pandas DataFrame would get that job done, right? Well, yes, but--there is an object in Scipy that is similar, but much faster to use for sparse matrices called scipy.sparse.csr_matrix(). Let's create that matrix next using the cleaned, reduced version of the data that we created during the data preparation phase.

```python
import numpy as np
from scipy.sparse import csr_matrix

U = df_triple['userId'].nunique()   # Number of users for the matrix
I = df_triple['movieId'].nunique()  # Number of items for the matrix

# Map user and movie IDs to matrix indices
user_mapper = dict(zip(np.unique(df_triple['userId']), list(range(U))))
item_mapper = dict(zip(np.unique(df_triple['movieId']), list(range(I))))

# Map maxtrix indices back to IDs
user_inv_mapper = dict(zip(list(range(U)), np.unique(df_triple['userId'])))
item_inv_mapper = dict(zip(list(range(I)), np.unique(df_triple['movieId'])))

# Create a list of index values for the csr_matrix for users and movies
user_index = [user_mapper[i] for i in df_triple['userId']]
item_index = [item_mapper[i] for i in df_triple['movieId']]

# Build the final matrix which will look like: (movieId, userId) rating
X = csr_matrix((df_triple['rating'], (item_index, user_index)), shape=(I, U))

# Print each item out to see how they all work together.
# Each object is used to create the next object.
# We don't have to print these out; this is just to help you understand.
print(f'Number of users: {U}')
print(f'Number of items: {I}')
print(f'user_mapper:\t {len(user_mapper.keys())}   keys:values {user_mapper}')
print(f'item_mapper:\t {len(item_mapper.keys())}  keys:values {item_mapper}')
print(f'user_index:\t {len(user_index)} values\t   {user_index}')
print(f'item_index:\t {len(item_index)} values\t   {item_index}')
print(f'user_inv_mapper: {len(user_inv_mapper.keys())}   keys:values {user_inv_mapper}')
print(f'item_inv_mapper: {len(item_inv_mapper.keys())}  keys:values {item_inv_mapper}')
print(f'X: {X}')

# Output:
# Number of users: 610
# Number of items: 2441
# user_mapper:	 610   keys:values {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7,...
# item_mapper:	 2441  keys:values {1: 0, 2: 1, 3: 2, 5: 3, 6: 4, 7: 5, 9: 6, 10: 7,...
# user_index:	 82664 values	   [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,...
# item_index:	 82664 values	   [0, 2, 4, 35, 37, 45, 58, 62, 74, 77, 83, 110, 113,...
# user_inv_mapper: 610   keys:values {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,...
# item_inv_mapper: 2441  keys:values {0: 1, 1: 2, 2: 3, 3: 5, 4: 6, 5: 7, 6: 9, 7: 10,...
# X:   (0, 0)	4.0
#      (0, 4)	4.0
#      (0, 6)	4.5
#      (0, 14)	2.5
#      (0, 16)	4.5
#      :     :
#      (2440, 330)	4.0
#      (2440, 337)	1.0
#      (2440, 379)	3.0
#      (2440, 513)	3.5
#      (2440, 585)	4.0
```

Okay, this is a lot to take in at first. But it's actually not that complicated. Let's break it down. First, we have to calculate the number of users and items. That part isn't too hard to understand. We store those values in U and I. Next, we have to map the userIds and movieIds to a 0 to n range without any skipped values. Our df_triple DataFrame has userIds and movieIds that range in values from 1-610 and 1-187593 respectively. However, for the sparse matrix to work, we need to know what index each of those Ids will be in the matrix. So we create a user_mapper and item_mapper to indicate that userId 1 will be index 0 of the csr_matrix() object. Similarly, movieId 1 will be index 0 in the csr_matrix(). But because not all userIds and movieIds are represented in the data (remember we removed those movies that hadn't been rated at least 9 times), movieId 187593 will actually be index 2441 in the csr_matrix. So, we must map those numbers together to create the size of the matrix.

In addition, we need to create reversed (i.e. inversed) versions of those same matrices (called user_inv_mapper and item_inv_mapper) so that after we have calculated the similarity scores for each pair of rows in the matrix, we can map the recommended movies back to their original Ids so that we can look up the movie titles. The variables U, I, user_index, and item_index were created only to help us set the size of X, the csr_matrix. We don't need them for any other purpose.

Clear as mud? It will take some time looking through those ID numbers and following the logic explained above in order to fully understand the process. The nice thing is, even if you don't understand completely, we can still turn this matrix creation into a function and make use of it.

```python
def create_matrix(df, user, item, rating):
  import numpy as np
  from scipy.sparse import csr_matrix

  U = df[user].nunique()  # Number of users for the matrix
  I = df[item].nunique()  # Number of items for the matrix

  # Map user and movie IDs to matrix indices
  user_mapper = dict(zip(np.unique(df[user]), list(range(U))))
  item_mapper = dict(zip(np.unique(df[item]), list(range(I))))

  # Map matrix indices back to IDs
  user_inv_mapper = dict(zip(list(range(U)), np.unique(df[user])))
  item_inv_mapper = dict(zip(list(range(I)), np.unique(df[item])))

  # Create a list of index values for the csr_matrix for users and movies
  user_index = [user_mapper[i] for i in df[user]]
  item_index = [item_mapper[i] for i in df[item]]

  # Build the final matrix which will look like: (movieId, userId) rating
  X = csr_matrix((df[rating], (item_index, user_index)), shape=(I, U))

  return X, user_mapper, item_mapper, user_inv_mapper, item_inv_mapper
```

This function requires a DataFrame with the user-item-rating triple as rows and the names of the user, item, and rating column. It returns the csr_matrix() object, the user and item mappers, and the inverse mappers. Now, let's call the function, store those objects, and print them out for verification.

```python
# Call the function and store the objects needed to calculate similarity and make recommendations
X, user_mapper, item_mapper, user_inv_mapper, item_inv_mapper = create_matrix(df_triple, 'userId', 'movieId', 'rating')

print(X) # (movieId, userId)   rating
print(user_mapper)
print(user_inv_mapper)
print(item_mapper)
print(item_inv_mapper)

# Output:
# user_mapper: 610 keys:values {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8,...
# item_mapper: 2441 keys:values {1: 0, 2: 1, 3: 2, 5: 3, 6: 4, 7: 5, 9: 6, 10: 7,...
# user_inv_mapper: 610 keys:values {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,...
# item_inv_mapper: 2441 keys:values {0: 1, 1: 2, 2: 3, 3: 5, 4: 6, 5: 7, 6: 9, 7: 10,...
# X:   (0, 0)	4.0
#      (0, 4)	4.0
#      (0, 6)	4.5
#      (0, 14)	2.5
#      (0, 16)	4.5
#      :     :
#      (2440, 330)	4.0
#      (2440, 337)	1.0
#      (2440, 379)	3.0
#      (2440, 513)	3.5
#      (2440, 585)	4.0
```

Now we have a nice function that can be reused across a varity of collaborative filtering tasks that creates the sparse matrix required to calculate similarity next and make recommendations. By the way, just how sparse is this matrix? Let's take a quick look out of curiosity.

```python
# How sparse is this matrix?
# sparsity = round(1.0 - len(df_triple) / float(len(user_mapper) * len(item_mapper)), 3)
# print('The sparsity level of this matrix is {}%'.format(sparsity * 100))

# Output:
# The sparsity level of this matrix is 94.39999999999999%
```

This means that each user has rated 5.61% (1 - 94.39) of the movies in this database. Given that there are about 2440 movies, that is quite a few movies: 137 per user (2440 \* .0561). This is a good time to note that this dataset comes from a machine learning project started at the University of Minnesota. We download their dataset from Kaggle.com per their request (see details here and full reference: F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 19:1–19:19. ).

#### Model Fitting: Calculate Similarity

As you might remember from earlier in this chapter, the next step is to calculate how similar each pair of user records are in the user-item matrix (X). We typically do this using a k-nearest neighbors (KNN) clustering algorithm. The KNN algorithm provided by sklearn is appropriate for this task and it provides the following metrics options for determining similarity:

- 'cityblock': metrics.pairwise.manhattan_distances
- 'cosine': metrics.pairwise.cosine_distances
- 'euclidean': metrics.pairwise.euclidean_distances
- 'haversine': metrics.pairwise.haversine_distances
- 'l1': metrics.pairwise.manhattan_distances
- 'l2': metrics.pairwise.euclidean_distances
- 'manhattan': metrics.pairwise.manhattan_distances
- 'nan_euclidean': metrics.pairwise.nan_euclidean_distances

Obviously, it helps to understand clustering distances to help choose one of these. Generally, if your ratings are all on a standard, moderatly continuos scale, then a euclidean distance is generally a safe approach. However, if you have discrete data with few values (e.g. 1, 2, 3), then a manhattan or cityblock distance may be better. Our data is sort of in between. We have decimals in our ratings, but only 0.5. For example, we have 4, 4.5, and 5 as possible ratings, but not 4.13 or 5.79. Of the options that the sklearn KNN algorithm provides, cosine has been well-established in prior research for being a good measure of similarity for the types of ratings we have for these movies we let's use that.

The function below takes a single movieId (e.g. perhaps a user is viewing a particular movie details), the X user-item matrix, the item_mapper and item_inv_mapper, the k number of recommendations the user wants returned, the metric to use for the sklearn KNN algorithm to calculate similarity, and a boolean indicating how strong each recommendation is in the form of the distance between each recommendation with the movieId passed in (show_distance).

```python
def recommend(itemId, X, item_mapper, item_inv_mapper, k, metric='cosine', messages=True):
  from sklearn.neighbors import NearestNeighbors

  rec_ids = []                # Make a list for the recommended item IDs we'll get later
  item = item_mapper[itemId]  # Get the index of the movie ID passed into the function
  item_vector = X[item]       # Get the vector of user ratings for the movie ID passed into the function

  # Fit the clustering algorithm based on the user-item matrix X
  knn = NearestNeighbors(n_neighbors=k+1, algorithm="brute", metric=metric).fit(X)

  # Call the trained knn cluster model to return the nearest neighbors of the item_vector passed in
  rec = knn.kneighbors(item_vector.reshape(1,-1), return_distance=True)
  rec_indeces = rec[1][0]     # Parse out the list of indeces of the recommended items
  rec_distances = rec[0][0]   # Parse out the recommendation strength calculated as the distance from the cluster center
  rec_distances = np.delete(rec_distances, 0) # Drop the first number in the list because it is the distance of itemId from itself

  # We need to replace the recommended item indeces with their original item IDs
  for i in range(1, knn.n_neighbors): # n_neighbors is the number of neighbors to return
    rec_ids.append(item_inv_mapper[rec_indeces[i]])

  # It may help to see what this is. The distance list is first and the recommended item indeces are second
  if messages:
    print(f'List of recommended item indeces:\n{rec_indeces}\n')
    print(f'List of recommended item IDs:\n{rec_ids}\n')
    print(f'List of recommended item similarity to selected item:\n{rec_distances}\n')

  # Return two lists: the original item IDs of the recommendations and their similarity scores
  return rec_ids, rec_distances
```

This function is fairly well commented to help you understand what is going on inside. However, you may find it worthwhile to print out each object like we did when generating the user-item matrix (csr_matrix) earlier to help you understand exactly what is happening. If you feel like you understand the function, then let's proceed with calling the function, returning a list of 10 recommended movieIds, attaching movie titles to those Ids, and then print out each recommendation one-at-a-time.

```python
# Use any movie id here to get recommendations
# Use any movie id here to get recommendations
movie_id = 10
rec_ids, rec_distances = recommend(movie_id, X, item_mapper, item_inv_mapper, k=10)

print(f"If you like {df_movies.loc[movie_id, 'title']}, you may also enjoy:\n")

df_recommendations = pd.DataFrame(columns=['movieId', 'Recommendation', 'Strength (lower is better)'])
df_recommendations.set_index('movieId', inplace=True)

for i in rec_ids:
  df_recommendations.loc[i] = [df_movies.loc[i, 'title'], rec_distances[rec_ids.index(i)]]

df_recommendations

# Output:
# List of recommended item indeces:
#  [  7  85 201 179 199 287 162 238  75 304 285]
#
#  List of recommended item IDs:
#  [165, 380, 349, 377, 592, 316, 480, 153, 648, 589]
#
#  List of recommended item similarity to selected item:
#  [0.38420636 0.42382514 0.4263503  0.42829621 0.43685234 0.43741995
#   0.44307966 0.44395506 0.44934722 0.44979336]
#
#  If you like GoldenEye (1995), you may also enjoy:
```

![item rec results](../Images/Chapter25_images/item_rec_results.png)

Pretty cool, right? This is how not only movies are recommended on Netflix, but also how products are recommended on Amazon and how social media feeds are determined and how news articles are customized on Google News and Apple News and others. Of course, this is just the simplest form of collaborative filtering, but it's amazingly accurate if you have enough data.

The example above is how item recommendation is accomplished. But we will customize this uniquely for each user next.

Making recommendations for users is only slightly different. The use case for user recommendations is to get a list of recommendations when a user is logged in and not viewing any particular movie details. Rather, they are viewing some sort of "home screen". We still want to give them recommendations on this home screen. But if they aren't vieweing a particular movie, then what criteria will we use to get item recommendations? We will use the same recommend() function we created above, but we need to add some additional logic. The diagram below represents the various options we have for user recommendation:

There are three primary decision points in this process. The first decision point (Option A vs B) is to get either the top rated movie from the logged in user or a list of the top n movies to use for individual item recommendations. If we select only their top rated movie, then what if they have rated multiple movies with the same max score? Which should we use? This is where the next decision point is relevant--do we randomly select one of the top max rated movies (or top n rated) to retrieve recommendations for? Or do we select recommendations for all of the top movies and then blend the results together?

Finally, regardless of what we decide for the first two decision points, our third decision point is whether to show the user all recommendations or just those that they have not rated previously? This question depends on the context. If this were a restaurant recommendation or a recommendation for consumable, non-durable products (e.g. toilet paper), then I would suggest including recommendations for items they have already rated before. But many people (like me) rarely want to watch the same movie twice. In our context, I recommend filtering out rated movies from the final recommendations list.

What does someone like Netflix actually do? Let's take a look at an example home screen of a logged in user:

![netflix user example](../Images/Chapter25_images/netflix_user_example.png)

The top row of recommendations is likely some combined recommendation from all of the movies/shows you have watched as well as those you are planning to watch in "My List" (the third list in the screenshot). The second list titled, "Because you liked Avatar: The Last Airbender" is a single item recommendation based on the user rating that show/movie very high. As you can see, in practice, Netflix uses several types of recommendation as described above in order to cover every base.

In the example below, I will use Option B for the first decision point--meaning I will select the top n ratings for a given user rather than just the movie(s) with the max rating. In the second decision point, I will randomly select one of their top n rated movies to base their recommendations on rather than make a combination of all top n rated movies. Finally, I will filter out movies that have already been rated to avoid giving them recommendations for movies they have already seen.

```python
# Get a list of recommendations based on a single randomly selected movie from all of their top rated movies

user_id = 150   # Select a user
k = 20          # Select the number of recommendations to give them; movies they've seen will be removed from this total

# Filter the data by only those movies rated by this user
df_user_ratings = df_triple[df_triple['userId']==user_id]

# Find the movies with the max ratings for this user
max_rating = df_user_ratings['rating'].max()
df_favorites = df_user_ratings[df_user_ratings['rating'] == max_rating]['movieId']

# Randomly select one of their top rated movies
movie_id = df_favorites.sample(n=1).iloc[0]

# Get a list of recommendations based on their top rated movie
rec_ids, rec_distances = recommend(movie_id, X, item_mapper=item_mapper, item_inv_mapper=item_inv_mapper,
                                   k=k, messages=False)

print(f"Since you liked {df_movies.loc[movie_id, 'title']}, consider these:\n")
for i in rec_ids:
  if not i in list(df_user_ratings['movieId']): # Make sure we don't recommend movies they have already seen
    print(f"\t{df_movies.loc[i, 'title']}")

# Output:
# Since you liked Twelve Monkeys (a.k.a. 12 Monkeys) (1995), consider these:

#  	Pulp Fiction (1994)
#  	Terminator 2: Judgment Day (1991)
#  	Seven (a.k.a. Se7en) (1995)
#  	Fugitive, The (1993)
#  	Usual Suspects, The (1995)
#  	Jurassic Park (1993)
#  	Star Wars: Episode IV - A New Hope (1977)
#  	Toy Story (1995)
#  	Batman (1989)
#  	Silence of the Lambs, The (1991)
#  	Shawshank Redemption, The (1994)
#  	Mars Attacks! (1996)
#  	Terminator, The (1984)
#  	Trainspotting (1996)
#  	Braveheart (1995)
#  	Blade Runner (1982)
```

Run the code cell above multiple times and notice that the list of recommendations changes each time based on which of this user's (user_id = 150) top n rated movies was used for the item recommendation. Also notice that the number of recommended movies changes each time because we are filtering out anything that shows up in df_user_ratings.

This is enough modeling for now. There are many more techniques available in modern reserach and practice, but this is enough to give you a good idea of how collaborative filtering works. There is one problem though. You may have noticed that we must train a new KNN model every time we call the recommend() function in order to make a prediction. This isn't like our regression and classification algorithms where we train a model once (on a schedule) and then we deploy a .sav or .pkl file that make lightening fast predictions from. So let's talk through ways to deploy a collaborative filtering-based recommender model in the next section.

---

## 25.8 Deployment

As discussed at the end of the last section, recommendation models are deployed differently from supervised regression and classification models. In those cases, we saved the trained model file, deployed that file to an app, website, or API, and then made predictions based off the saved trained model. But recommendation predictions require that we recreate the user-item-matrix each time a model needs to be retrained, and then we generated the recommend() function to train a new KNN clustering model based on the k number of recommendations we wanted.

#### Option 1: Separate Model Training and Storage

There are two ways that we can deploy a recommender model. First, if the k can be determined just once each time we retrain or rerun the pipeline, then we can still store the trained cluster model in a .sav file just as we did with the supervised model pipelines. Then we can make predictions off of that trained model on the fly. Let's convert our recommend() function to work that way by separating the KNN cluster model training from the user-item matrix lookup.

```python
# Create a function to fit the cluster model; allow the caller to specify the matrix, k, and metric
def fit_cluster(X, k, metric='cosine'):
  from sklearn.neighbors import NearestNeighbors

  # Fit the clustering algorithm based on the user-item matrix X
  knn = NearestNeighbors(n_neighbors=k+1, algorithm="brute", metric=metric).fit(X)
  return knn

# These functions for deployment are the same ones we created and use earlier: lg3sz
def dump_pickle(model, file_name):
  import pickle
  pickle.dump(model, open(file_name, "wb"))

def load_pickle(file_name):
  import pickle
  model = pickle.load(open(file_name, "rb"))
  return model
```

The fit_cluster() function does just that. It fits a KNN cluster model using the n_neighbors, algorithm, and metric of our choice. The dump_pickle() and load_pickle() functions are the same ones that we first created and used in Chapter Not Found. Run this cell and load those functions in memory. Next, let's create a "light" version of the recommend() function that accepts a parameter for the trained and loaded knn model.

```python
def recommend_light(itemId, knn, item_mapper, item_inv_mapper, messages=True):
  rec_ids = []                # Make a list for the recommended item IDs we'll get later
  item = item_mapper[itemId]  # Get the index of the movie ID passed into the function
  item_vector = X[item]       # Get the vector of user ratings for the movie ID passed into the function

  # Call the trained knn cluster model to return the nearest neighbors of the item_vector passed in
  rec = knn.kneighbors(item_vector.reshape(1,-1), return_distance=True)
  rec_indeces = rec[1][0]     # Parse out the list of indeces of the recommended items
  rec_distances = rec[0][0]   # Parse out the recommendation strength calculated as the distance from the cluster center
  rec_distances = np.delete(rec_distances, 0) # Drop the first number in the list because it is the distance of itemId from itself

  # We need to replace the recommended item indeces with their original item IDs
  for i in range(1, knn.n_neighbors): # n_neighbors is the number of neighbors to return
    rec_ids.append(item_inv_mapper[rec_indeces[i]])

  # It may help to see what this is. The distance list is first and the recommended item indeces are second
  if messages:
    print(f'List of recommended item indeces:\n{rec_indeces}\n')
    print(f'List of recommended item IDs:\n{rec_ids}\n')
    print(f'List of recommended item similarity to selected item:\n{rec_distances}\n')

  # Return two lists: the original item IDs of the recommendations and their similarity scores
  return rec_ids, rec_distances
```

Run that cell to get the new function in memory. Next, let's train and store the KNN cluster model based on the user-item matrix we already created:

```python
# Train and store the model
knn = fit_cluster(X, k=10)
dump_pickle(knn, 'knn.pkl')
```

Finally, let's replicate how this work in practice later when we need to load the model to generate recommendations from it by loading the saved KNN model and then running our regular code to generate predictions for user 11.

```python
# Load and make predictions against the model
knn = load_pickle('knn.pkl')

# Use any movie id here to get recommendations
movie_id = 11
rec_ids, rec_distances = recommend_light(movie_id, knn, item_mapper, item_inv_mapper, messages=False)

print(f"If you like {df_movies.loc[movie_id, 'title']}, you may also enjoy:\n")

df_recommendations = pd.DataFrame(columns=['movieId', 'Recommendation', 'Strength (lower is better)'])
df_recommendations.set_index('movieId', inplace=True)

for i in rec_ids:
  df_recommendations.loc[i] = [df_movies.loc[i, 'title'], rec_distances[rec_ids.index(i)]]

df_recommendations

# Output:
# If you like American President, The (1995), you may also enjoy:
```

![item rec deploy1](../Images/Chapter25_images/item_rec_deploy1.png)

That is not so bad. The real question though is how much time does that save to make predictions? Let's time this recommend_light() function compared to recommend():

```python
# How much time does this save us?
%timeit recommend_light(movie_id, knn, item_mapper, item_inv_mapper, messages=False)
%timeit recommend(movie_id, X, item_mapper, item_inv_mapper, k=10, messages=False)

# Output:
# 2.7 ms ± 363 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
# 3.2 ms ± 531 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
```

So we saved half of a millisecond. That doesn't seem like much. But this was a very small dataset. That speed will improve much more as the dataset grows.

#### Option 2: Make Recommendations Upfront

Another way that we can deploy recommender models is to go ahead and generate all item recommendations and store them in an operational database so they can simply be queried when the view is loaded. Let's do that for every movie in the dataset and store the recommendations in a DataFrame.

```python
# How many recommendations per movie would you like?
k = 5

# Get a list of recommendations for all movies; you can store this list as a "trained model" of sorts
df_recommendations = pd.DataFrame(columns=['If you watched'], index=item_mapper)
for i in range(1, k):
  df_recommendations[f'Recommendation {i}'] = None

for row in df_recommendations.itertuples():
  # Get a ranked list of recommendati ons
  rec_ids, rec_distances = recommend(row[0], X, item_mapper, item_inv_mapper, k=k, messages=False)

  # Get the title of the 'watched' movie in this row
  df_recommendations.at[row[0], 'If you watched'] = df_movies.at[row[0], 'title']

  # Get the titles of the recommended movies
  for i, r in enumerate(rec_ids):
    df_recommendations.at[row[0], f'Recommendation {i+1}'] = df_movies.at[r, 'title']

# Store df_recommendations in an operational DB.
#
# from sqlalchemy import create_engine
# engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
#                       .format(user="root",
#                               pw="12345",
#                               db="employee"))
# df_recommendations.to_sql('book_details', con = engine, if_exists = 'append', chunksize = 1000)

df_recommendations.tail()
```

Nice...you can now build all of these steps into functions that work together in an ML pipeline when you're ready.

---

## 25.9 Practice

Practice your skills working on some of these tasks below:

The goal of this practice is to replicate the entire process we performed with collaborative filtering-based recommendation on a new dataset. The dataset below is based on a set of Amazon product reviews. Create a collaborative filtering model that will predict 5 products based on a given product selected and demonstrate the recommendations with a sample product.

In this chapter on collaborative filtering, we demonstrated how to make a random selection from a user's top n rated movies (using the Movie Lens database) and give them recommendations based on that movie.

Extend that logic by getting recommendations for all of the user's top n rated movies and combine the results into a single list sorted by the support, or similarity, score for each recommended movie's distance from the movie it was recommended based upon. Sort the entire table and return a list of recommendations for the user across each of their top n rated movies.

If the user has more max-rated movies than the n specified, expand n so that it includes all of the users max-rated movies. For example, if we specify that the top 5 movies rated by a user should be used to make their overall recommendation list, but that user has rated 12 movies with the max rating of 5, then expand n to 12 so that it includes each of their top n favorite movies.

The chapter code required to import and clean the data as well as generate the user-item matrix is repeated below to get you started.

```python
# Don't forget to mount Google Drive if you haven't already:
from google.colab import drive
drive.mount('/content/drive')

import pandas as pd

df_triple = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/movies-small/ratings.csv')
df_movies = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/movies-small/movies.csv', index_col='movieId')

# Store the count of ratings for each movie
value_counts = df_triple['movieId'].value_counts()

# Reduce the user-item-rating triple dataset to only those with than 9+ ratings
keep_list = value_counts[value_counts >= 9]
df_triple = df_triple.loc[df_triple['movieId'].isin(keep_list.index)]

# Drop duplicates, if any
df_triple.drop_duplicates(subset=['userId', 'movieId'], keep='first', inplace=True)

# Bring in the function required to generate the user-item matrix
def create_matrix(df, user, item, rating):
  import numpy as np
  from scipy.sparse import csr_matrix

  U = df[user].nunique()  # Number of users for the matrix
  I = df[item].nunique()  # Number of items for the matrix

  # Map user and movie IDs to matrix indices
  user_mapper = dict(zip(np.unique(df[user]), list(range(U))))
  item_mapper = dict(zip(np.unique(df[item]), list(range(I))))

  # Map matrix indices back to IDs
  user_inv_mapper = dict(zip(list(range(U)), np.unique(df[user])))
  item_inv_mapper = dict(zip(list(range(I)), np.unique(df[item])))

  # Create a list of index values for the csr_matrix for users and movies
  user_index = [user_mapper[i] for i in df[user]]
  item_index = [item_mapper[i] for i in df[item]]

  # Build the final matrix which will look like: (movieId, userId) rating
  X = csr_matrix((df[rating], (item_index, user_index)), shape=(I, U))

  return X, user_mapper, item_mapper, user_inv_mapper, item_inv_mapper

# Call the function to generate the matrix and mapper dictionaries
X, user_mapper, item_mapper, user_inv_mapper, item_inv_mapper = create_matrix(df_triple, 'userId', 'movieId', 'rating')

def recommend(itemId, X, item_mapper, item_inv_mapper, k, metric='cosine', messages=True):
  from sklearn.neighbors import NearestNeighbors
  import numpy as np

  rec_ids = []                # Make a list for the recommended item IDs we'll get later
  item = item_mapper[itemId]  # Get the index of the movie ID passed into the function
  item_vector = X[item]       # Get the vector of user ratings for the movie ID passed into the function

  # Fit the clustering algorithm based on the user-item matrix X
  knn = NearestNeighbors(n_neighbors=k+1, algorithm="brute", metric=metric).fit(X)

  # Call the trained knn cluster model to return the nearest neighbors of the item_vector passed in
  rec = knn.kneighbors(item_vector.reshape(1,-1), return_distance=True)
  rec_indeces = rec[1][0]     # Parse out the list of indeces of the recommended items
  rec_distances = rec[0][0]   # Parse out the recommendation strength calculated as the distance from the cluster center
  rec_distances = np.delete(rec_distances, 0) # Drop the first number in the list because it is the distance of itemId from itself

  # We need to replace the recommended item indeces with their original item IDs
  for i in range(1, knn.n_neighbors): # n_neighbors is the number of neighbors to return
    rec_ids.append(item_inv_mapper[rec_indeces[i]])

  # It may help to see what this is. The distance list is first and the recommended item indeces are second
  if messages:
    print(f'List of recommended item indeces:\n{rec_indeces}\n')
    print(f'List of recommended item IDs:\n{rec_ids}\n')
    print(f'List of recommended item similarity to selected item:\n{rec_distances}\n')

  # Return two lists: the original item IDs of the recommendations and their similarity scores
  return rec_ids, rec_distances
```

```python
# Click the Colab link to the right to see a potential solution
```

---

## 25.10 Assignment

Complete the assessment below:

---
