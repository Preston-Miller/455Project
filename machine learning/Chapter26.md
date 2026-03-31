# Chapter 26: Recommendation: Content Filtering

## Learning Objectives

- Students will be able to engineer features from item metadata including text descriptions, genres, and categorical attributes
- Students will be able to apply TF-IDF vectorization to text descriptions and compute cosine similarity between items
- Students will be able to generate content-based recommendations and compare content filtering versus collaborative filtering approaches

---

## 26.1 Introduction

Content filtering is another way to generate recommendations that works quite differently from collaborative filtering. But it can be very effective in situations where you don't have user-item-ratings data yet. For example, if you have just built an app, and you don't have ratings data yet to use for collaborative filtering, you can use the details about users or the details about items to group or cluster similar records together in order to recommend related items or related users. This process is fundamentally different, but provides the same results--recommendations. Let's learn how this process works.

---

## 26.2 Content Filtering

As mentioned in the prior section, content filtering does not depend on user ratings. But content filtering does depend on some of the same assumptions as collaborative filtering. For example, both types of filtering assume that similar people like similar things and that similar items will be similarly preferred by users. However, each techniques measures "similarity" quite differently. Collaborative-based filtering measures user similarity based on the ratings that users give to items. If they rate the same items similarity, then they are similar users. Content-based filtering measures similarity based on the characteristics of the user and items.

For example, movie descriptions with the same words and phrases are similar. Movies with the same director, actors, or genre are similar. Users with the same demographic characteristics are similar. Or, users who have the same stated preferences for certain item features are similar. The diagram below visualizes the data required for collaborative vs content filtering

![content vs collaborative](../Images/Chapter26_images/content_vs_collaborative.png)

Let's summarize the advantages and disadvantages of both approaches.

The great advantage of content-based filtering is that you don't need user-item-rating historical data to generate recommendations. This means it will generate equally valid recommendations whether the items have been thoroughly rated or are brand new with no ratings. You only need item characteristics for item-based recommendations. The downside is that you can't generate user-based recommendations (e.g. recommendations for the user's home screen view that are specific to that user, but do not depend on a specific item like you would get from viewing an item's detailed view). Having said that, if you have theoretically valid features about users, then you can identify similar users, but you would have to pair the content-based model with a collaborative model in order to know which items to recommend to new users. It is actually quite common to combine collaborative and content models together into **hybrid recommender systems** — recommendation algorithms that combine elements from both collaborative- and content-based concepts to best address all scenarios. which combine elements from both collaborative- and content-based concepts to best address all scenarios.

If you want to dive into hybrid recommendation in more detail, you can find a useful discussion of various ways to combine collaborative- and content-based approaches here: https://medium.com/analytics-vidhya/7-types-of-hybrid-recommendation-system-3e4f78266ad8

It may still be difficult to understand these differences between collaborative and content filtering without just digging into the code and seeing how it works. For this example, we will switch to a similar, but unique, dataset based on Netflix data which includes many more details about the movies and series available on the platform. Download the dataset below which can also be found here.

---

## 26.3 Data Import

Let's begin by importing the data. Nothing too complex here.

```python
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

# Don't forget to mount Google Drive if you haven't already:
# from google.colab import drive
# drive.mount('/content/drive')

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/netflix_titles.csv')
df.head()
```

![The first five rows of the netflix dataset. Includes the following columns: show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description.](../Images/Chapter26_images/df_netflix.png)

As you can see, we have several useful features about the shows including the type (movie or TV show), title, director, cast, country date added, release year, rating, duration, list of genres each record appears in, and a full description. This type of data will be useful to identify similar movies. Theoretically, people often like movies that come from a particular director, cast, or genre.

---

## 26.4 Data Understanding

The primary issue to address in data exploration for this process is to see if we have any missing data.

```python
print(df.shape)
df.isna().sum()

# Output:
# (8807, 12)
# show_id            0
# type               0
# title              0
# director        2634
# cast             825
# country          831
# date_added        10
# release_year       0
# rating             4
# duration           3
# listed_in          0
# description        0
# dtype: int64
```

Looks like we do have some missing data to address. We'll take care of that in the next phase. In addition, in other supervised machine learning contexts, we often examined the distributions of categorical features to see if there were groups that were under-represented. That requirement does not apply to the type of algorithm we will be using. Therefore, we won't worry about any other data exploration right now.

---

## 26.5 Data Preparation

#### Address Missing Data

Let's address that missing data now. The best way to fix this would be to look up each movie or TV show that is missing data on the imdb.com website. I checked the first couple of records with missing data and their full details were available online. However, let's say hypothetically that information is unavailable for some reason. Maybe there is no cast because it's a nature documentary with no narration. If that were the case, I'd recommend that we create a category to represent all of these issues. In the code below, I'm replacing missing directors, casts, and countries with the text "unknown". Alternatively, you could name it "not applicable". Either way, it wouldn't matter. Those values would have the same importance/usefulness regardless of what you decide to call it.

Next, I'm substituting date_added and duration with the mode of those columns. Finally, I'm dropping any row that is missing rating. Is this the right thing to do? Maybe, maybe not. You can decide. I just wanted to remind you of some of the various options. You may also remember from a prior chapter that we can also predict missing values using sklearn's IterativeImputer or KNN Imputer.

```python
df.director.fillna('unknown', inplace=True)
df.cast.fillna('unknown', inplace=True)
df.country.fillna('unknown', inplace=True)
df.date_added.fillna(df.date_added.mode()[0], inplace=True)
df.duration.fillna(df.date_added.mode()[0], inplace=True)
df.dropna(subset=['rating'], inplace=True)

# Very important step
df.reset_index(inplace=True)

print(df.isna().sum(), '\n')
df.shape

# Output:
# show_id         0
# type            0
# title           0
# director        0
# cast            0
# country         0
# date_added      0
# release_year    0
# rating          0
# duration        0
# listed_in       0
# description     0
# dtype: int64

# (8803, 12)
```

Okay, missing data addressed. But what is with that line that resets the df index with the comment "Very important step"? Remember during the collaborative filtering example when we had to create an itemID to matrix index mapping dictionary? Then we created an item_inv_mapper object that reversed the mapping? We did that because we had to drop a bunch of movie IDs which caused there to be gaps in the movie ID list (e.g. 1, 2, 5, 10, 23, etc). However, the user-item matrix needed to be consequtively ordered. Well, the same is true for the matrix we are about to create. We could, once again, create an item_mapper and item_inv_mapper set of dictionaries to handle this. Or, we could simply reset the index of the DataFrame so that each movie/show ID is consequtively numbered. I wanted you to see that both were valid options for creating the similarity matrix.

Let's proceed with some of the modeling-specific data preparation that needs to happen before we can make recommendations.

---

## 26.6 Modeling

#### Modeling Preparation

Similar to the collaborative filtering context, we have to get the data in a particular format before we can establish similarity scores. You might remember that we generated a sparse user-item-rating matrix for collaborative filtering that was basically a table inidcating the rating each each user gave each item. We are going to do something similar again. The difference is that we don't have ratings with this data. Instead of a rating, we are going to generate a tokenized version of the movie/show description column. Then we are going to calcualte a score that represents how importance, or unique, each word is and sum up the score each descrition gets for having those words. Understand? Probably not yet. Let's run the code and then it will be a bit clearer.

```python
from sklearn.feature_extraction.text import TfidfVectorizer

# Create a TfidfVectorizer and Remove stopwords
tfidf = TfidfVectorizer(stop_words='english')

# Fit and transform the data to a tfidf matrix
tfidf_matrix = tfidf.fit_transform(df['description'])

# Print the shape of the tfidf_matrix
print(tfidf_matrix.shape)

# Preview the matrix by placing it into a DataFrame (which we won't need later)
df_tfidf = pd.DataFrame(tfidf_matrix.T.todense(), index=tfidf.get_feature_names_out(), columns=df['description'])
df_tfidf.iloc[2221:2226]

# Output
# (8803, 18891)
```

![Modeling](../Images/Chapter26_images/df_netflix_tfidf_matrix.png)

So what exactly are you looking at? This table shows the the tfidf_matrix we generated in a Pandas DataFrame. To be clear, we don't need this DataFrame. I just printed it out so that you could see what the tfidf_matrix looks like. Each of the 8803 columns contains one of the unique movie/show descriptions. The rows contain the unique 18891 words (minus stopwords) that appear in the corpus of those descriptions. The row indices represent the unique tokenized words from the corpus of movie descriptions. The index is sorted alphabetically. I've highlighted the index for the word "boy" because it represents an n-gram that appears in the description of the farthest right movie in the image above. That is why the cell for that row and column combination has a positive score which we call a TF-IDF score.

The score in the table is the **term frequency - inverse document frequenty (TF-IDF)** — a statistic used in natural language processing and information retrieval that measures how important/unique a term is within a document relative to the overall document collection (i.e. corpus). TF-IDF is a common statistic used in natural language processing that measures how important/unique a term is within a document relative to the overall document collection. It is the product of two measures: term frequency (TF) and inverse document frequency (IDF). TF is measured as the count of the number of times a word or n-gram appears in a specific document divided by the number of words in the document. IDF is also calculated uniquely for each n-gram as: ln((the number of documents) / (the total number of documents containing the n-gram)). For example, a word that appears only once across all descriptions would have the highest TF-IDF score. The table below gives the exact TF, IDF, and TF-IDF scores for four sample documents:

Let's break down the first row of data. In the document, "The dog ran", the word "The" appears 1 time in this doc and there are 3 words total making the TF score 0.3333 repeated. There are 4 total docs and all 4 include the word "The" making the IDF score zero because ln(4/4) = 0. Therefore, the TF-IDF score is 0.3333 \* 0 = 0. Again, the purpose of TF-IDF is to represent a relative measure of how unique a word is across all documents and within the document the score is being calculated against. As a result, the word "The" has no importance because it appears in every document. That is no different from a word that doesn't appear in a particular document at all. They both get a TF-IDF score of zero. The best scores come from words that appear in fewer documents in documents with fewer words.

Hopefully that makes a bit more sense and helps you understand what the tfidf_matrix object holds that we created above. the next step is to calculate a similarity matrix based on the TF-IDF scores. Just as with the collaborative filtering model, there are many similarity scores that we can use here. For a complete review, see 25.7. For this demonstration, we will stick with a cosine-based similarity matrix just like that example. The code below creates a matrix and prints it out in a temporary DataFrame just for viewing purposes.

```python
from sklearn.metrics.pairwise import linear_kernel

# Compute the cosine similarity between each movie description
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

# For easier viewing, put it in a dataframe
pd.DataFrame(cosine_sim)
```

![A matrix table containing cosine similarity scores from zero to one for each pair of movies/shows](../Images/Chapter26_images/df_netflix_cosine_matrix.png)

To be clear, this is an 8803 by 8803 matrix that contains a cosine "similarity" score representing how similar the movie/show descriptions are between each pair of movies/shows. The cosine similarity score was calculated using the vectors of TF-IDF scores in the prior step. Essentially, higher scores in the matrix above indicate that the two movies/shows reprented by the row and column ID numbers have descriptions with more of the same words. If these movies/shows have useful descriptions that truly represent what the content is about, then these cosine similarity scores will accurately indicate which items to recommend based on an item of interest.

You may notice that the cosine similarity scores generated in this matrix are different than those we calculated for the collaborative filtering example. That is because we used the linear_kernel object from sklearn to compute them. For a more thorough discussion of how the linear_kernel() object calculates cosine similarity, you can read more here. But essentially, it reverses the scale so that higher numbers mean "more similar" as opposed to the opposite in the collaborative filtering example in the prior chapter where a lower score represented a smaller angle and, thus, greater similarity. As a result of how linear_kernel() calculates cosine similarity, the diagonal of that table contains all 1.0000 values (the max possible) because that is the cosine similarity of a movie description with itself.

This cosine_sim matrix object is basically the trained model we need to make recommendations. For any movie/showID, we only need to rank sort (descending) the cosine similarity scores and return the top n as recommendations. In fact, let's try it to see if that works. The code below sorts by column 0 descending and shows that movie 4877, 1066, 7503, and 5047 have the most similar descriptions to movie 0.

```python
df_sorted = pd.DataFrame(cosine_sim).sort_values(by=[0], ascending=False)

for id in df_sorted.index[0:4]:
  print(id, '\t', df.loc[id, 'title'])

display(df_sorted)

# Output:
# 0    	 Dick Johnson Is Dead
# 4877 	 End Game
# 1066 	 The Soul
# 7503 	 Moon
# 5047 	 The Cloverfield Paradox
```

You may or may not recognize these shows, but if you read their descriptions, you'll find that they truly do have the most similar text to movie ID 0, Dick Johnson is Dead. Let's explore some good ways to deploy these recommendations in the next section.

---

## 26.7 Deployment

So how should we deploy this recommender model? As usual, let's make a function that accepts a movie/item and returns the top n recommendations for that item. To make this function as fast as possible, we won't put the matrix in a DataFrame. Instead, it makes a list of the highest similarity-ranked item indices and returns them in a python dictionary alongn with their similarity scores.

```python
def get_recommendations(item_id, sim_matrix, n=10, messages=True):
  if not item_id in sim_matrix[:]:  # Add some error checking for robustness
    print(f"Item {item_id} is not in the similarity matrix you provided")
    return

  # Get the pairwise similarity scores of all movies with that movie
  sim_scores = list(enumerate(sim_matrix[item_id]))

  # Sort the items based on the similarity scores
  sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

  # Get the scores of the n most similar items; start at 1 so that it skips itself
  top_similar = sim_scores[1:n+1]

  # Put the recommended item indices and similarity scores together in a dictionary using comprehension
  rec_dict = {i[0]:i[1] for i in top_similar}

  if messages:
    print(f"The top recommended item IDs are: {list(rec_dict.keys())}")
    print(f"Their similarity scores are:\t  {list(rec_dict.values())}")

  # Return the top n most similar items
  return rec_dict
```

Take a look through the code in this function. It requires an item_id to make recommendations for. It also needs the similarity matrix. But it allows the scores in the matrix to be calculated any way that you want to provide. It also wants to know how many recommendations you need (n). Finally, it will print out some details on how it processes the results if you want them.

Inside the function, it first checks to make sure you've provided a valid item_id. Then, it selects all of the similarity scores for that item_id. Then, it sorts the scores descending so the best scores are first. Then, it selects only the top n from that list and adds the recommended item_ids and their similarity scores to a dictionary using comprehension. It will print out the details of that dictionary if unless you tell the function not to (messages=False). Finally, it returns the results.

Now that we have the function, let's practice calling it. We'll add the results to a DataFrame that we join with the original df DataFrame so that we can view the title, cast, release_year, rating, and listed_in (i.e. genres) along with the similarity scores.

```python
# Change this value to any title you'd like to get recommendations
title = "Dick Johnson Is Dead"

# Check if the title is valid; if not, suggest alternatives and use the last one for recommendations
if title in df['title'].to_list():
  id = df.index[df['title']==title][0] # Convert the title to an index (i.e. item ID)
else:
  print(f"\"{title}\" is not in the data set. Try one of these:\n")
  for row in df.sample(n=10).itertuples():  # Get a random 10 titles
    id = row[0]
    title = row.title
    print(f'\t{title}')

print(f"\nIf you like \"{title},\" then you may also like:\n")

# Call the function and return the dictionary; print out the dictionary if you want to see what it is
recommend_dict = get_recommendations(id, cosine_sim, n=10, messages=False)

# Add the dictionary to a new DataFrame; this isn't necessary, but it helps to see what movies are recommended
df_similarity = pd.DataFrame(data=recommend_dict.values(), columns=['similarity'], index=recommend_dict.keys())

# Create a subset of the original df DataFrame with only the recommended movies
df_recommendations = df.loc[df.index.isin(recommend_dict.keys()), ['title', 'cast', 'release_year', 'rating', 'listed_in']]

# Join the original df results with the recommended movie similarity scores so that we can sort the list and view it
df_recommendations.join(df_similarity).sort_values(by=['similarity'], ascending=False)
```

Again, we didn't really need to view the results in a DataFrame along with the similarity scores in order for the function to work. We just did that as a sanity check to see if the results looked valid. The function only returns a dictionary of movie IDs and similarity scores which is all we would need to deploy this model in an app or website.

You may find it useful to try this content-filtering recommender model with some other titles. You can print out the entire list from the df DataFrame and copy/paste any movie title into the function all above. Here is a sample list you can try.

- Solo: A Star Wars Story
- Spider-Man: Into the Spider-Verse
- The Blue Planet: A Natural History of the Oceans
- The Lord of the Rings: The Return of the King
- The Time Traveler's Wife
- Zombieland
- The Boss Baby: Get That Baby!
- PJ Masks
- The Karate Kid
- My Little Pony: Friendship Is Magic

However, you'll notice that I wrote the code above to allow you to specify an incorrect title name. If you do, then it will randomly select 10 valid titles or you to choose from.

So how should you use this content-filtering recommender algorithm in an app or website? You have the same two general options that were introduced to you in the prior chapter in 25.8. I think Option 2 from that chapter is the simplest; meaning that you run your Jupyter Notebook file to include exporting the recommendation results for every item in the database that your app or website uses on a regular schedule so that the recommendations are constantly updated to reflect the lastest list of titles. How regular? Well, since content filtering doesn't depend on user ratings, you would only have to update the recommendations any time the database list of items is updated. So, if Netflix updates their catalog monthly, then you would need to rerun this code once a month just after the titles list is updated.

---

## 26.8 Practice

Try working through these practice problems below.

As you know, functions create reusable code that can save us time and enable smooth pipelines. You already have a function from this chapter for making content filtering-based recommendations. For this practice, you will create functions for all remaining steps in the pipeline.

First, create a function that performs the same cleaning steps that we performed in the chapter for the netflix_titles.csv dataset. This function does not need to be particularly dynamic. It should just return a dataset with no missing values that has had the index reset so that all movie IDs are consecutive in the index.

Next, create a function that calculates TF-IDF scores and generates the similarity matrix. It should require the DataFrame that was cleaned from the prior function and also allow, as a parameter, the name of the feature that has unstructured text that will be used to calculate TF-IDF scores in the matrix. Call this function tfidf_matrix().

Then, either use the existing function from the chapter called get_recommendations() or create a new one to accomplish the same purpose. It should accept an item_id, the similarity matrix, and the number of recommendations you want returned. It should return the recommended movie IDs (not titles) in the order of similarity as well as the similarity scores. Again, you could just copy the function from the chapter.

Then, create a method (not a function because it won't have a return statement) that allows you to pass in a movie title and anything else you need to make this method work well. This method will print out the n titles recommended for the submitted title. In other words, this function will call get_recommendations() to return movie IDs, but then it will convert them into titles. If you enter an invalid title, it should inform you that it was invalid and it should then suggest 10 random other titles to try instead. This function should print out the following text, "If you liked [movie name] staring [cast], then you may like these other movies including a similar cast:" Then it should print a DataFrame of the recommended movie titles, casts, and the similarity score.

Finally call these functions in the proper order using the netflix_titles.csv dataset once again. But use the "cast" feature instead of the "description" feature to calculate the similarity scores. Get 5 recommendations for the movie, "The Polar Express". Try spelling the movie title incorrectly to see if your logic works in the last method.

Click the Colab icon to the right to see one possible solution to this practice problem.

Using the same Netflix dataset as found in the chapter, and the pipeline you created in the prior practice problem, expand the content filtering to include all of the following features together: type, title, director, cast, country, listed_in, and description. One way to do this is to merge/concatenate each of those string features into a single column and base your model on that combined column. However, be sure to clean the data first by addressing all missing data.

There are many ways to accomplish this task using the pipeline you have already created in Practice #1. Feel free to modify any of the prior functions to make this work.

Click the Colab icon to the right to see one possible solution to this practice problem.

---

## 26.9 Assignment

Complete the assignment below:

---
