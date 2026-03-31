# Chapter 22: NLP: Topic Modeling

## Learning Objectives

- Students will be able to clean and preprocess text data systematically by removing noise, normalizing tokens, and extracting meaningful terms
- Students will be able to build n-gram models to capture multi-word phrases and improve topic coherence
- Students will be able to implement Latent Dirichlet Allocation (LDA) and interpret topics as probability distributions over words
- Students will be able to visualize and evaluate topic model results using pyLDAvis and coherence metrics

---

## 22.1 Introduction

![Introduction](../Images/Chapter22_images/topic_modeling.png)

In the previous chapter, you learned how text analytics can convert unstructured text into structured features such as parts of speech, named entities, and sentiment. In this chapter, we take the next step by using models that attempt to uncover the _latent themes_ that run throughout a collection of documents. This class of techniques is known as **topic modeling** — A type of statistical modeling technique used to discover abstract themes that occur across a collection of text documents.. Rather than focusing on individual words or counts, topic modeling seeks to identify hidden semantic structure—patterns of language that tend to appear together and represent coherent ideas.

Topic modeling is especially useful when working with large collections of text where manual review is impractical. In this chapter, you will learn one of the most widely used topic modeling approaches: _Latent Dirichlet Allocation (LDA)_. LDA assumes that each document is composed of a mixture of topics and that each topic is defined by a probability distribution over words. While LDA is mathematically simpler than modern neural language models, the underlying idea is similar: both attempt to learn structure from patterns of word usage across large corpora of text.

This makes topic modeling a helpful conceptual bridge to understanding modern _large language models (LLMs)_. LDA does not generate text, reason, or understand context the way LLMs do. However, it introduces the core idea that meaning can be inferred statistically from language patterns without explicit labeling. As you work through this chapter, keep in mind that LLMs build on these same foundational insights—using vastly more data, more complex architectures, and far greater computational power to model language at scale.

Images in this section were created using DALL·E from OpenAI.

---

## 22.2 From Words to Topics

![Wide diagram illustrating the progression from topic modeling to modern language models. On the left, documents are broken into words and grouped into latent topics using probabilistic methods. In the center, topics are represented as numerical embeddings capturing semantic similarity. On the right, a large language model uses these learned representations to generate and understand text, showing how topic modeling concepts evolve into modern LLMs.](../Images/Chapter22_images/topic_model_header.png)

The goal of topic modeling is to help us extract meaning from large amounts of text dynamically without human intervention. When working with large collections of text, a central challenge is scale: humans can read and interpret a handful of documents, but not thousands or millions of them. Topic modeling exists to help machines identify recurring themes and patterns in text so analysts can explore, summarize, and reason about large document collections.

Unlike traditional predictive models, topic models do not attempt to predict a single label. Instead, they aim to uncover **latent structure** — Patterns or relationships in data that are not directly observed but are inferred from statistical regularities. within text by analyzing how words tend to appear together across documents.

A key idea in topic modeling is that documents rarely belong to just one theme. Instead, each document can be thought of as a mixture of multiple topics expressed in different proportions. For example, a news article might be mostly about economics while also referencing politics and technology.

In topic modeling, a **topic** — A probability distribution over words that frequently occur together across documents. is not a label or category chosen in advance. Instead, a topic is defined by the likelihood that certain words appear together, and humans interpret the meaning of that topic after the model is trained.

This means topic modeling is an **unsupervised modeling technique** — A modeling approach that discovers structure in data without using labeled outcomes.. There is no single correct answer, no ground-truth topic list, and no definitive number of topics. Different modeling choices can lead to different but still useful interpretations.

Because topic models propose structure rather than confirm it, they are best viewed as exploratory tools. Analysts must evaluate whether the discovered topics are coherent, interpretable, and useful for the problem they are trying to solve, much like clustering or feature engineering.

Modern language models build on these same foundational ideas. While topic models represent documents as mixtures of topics, newer models represent text as sequences of words whose probabilities depend on context. The underlying goal remains the same: to model meaning by learning patterns in how language is used.

In this chapter, we focus on topic modeling as a practical and interpretable way to discover themes in text. Understanding these ideas will make it much easier to learn more advanced natural language models in later chapters.

Ask an AI assistant: How is representing a document as a mixture of topics similar to predicting the next word in a sentence based on prior context?

Why is topic modeling considered exploratory rather than predictive, and why can multiple topic solutions all be considered valid?

### Case Study: Discovering Topics in Customer Reviews

Imagine you work for an online retailer with 50,000 written customer reviews. You know customers are talking about many different things, but no one has labeled the reviews by topic, and reading them all manually is impossible.

Your goal is not to predict an outcome, but to explore what customers tend to talk about. This makes the problem well suited for **topic modeling** — A technique used to discover recurring themes in a collection of documents without predefined labels..

After cleaning the text and training a topic model, the algorithm produces several topics. Each topic is represented by a set of high-probability words, such as: delivery, shipping, late, fast, package for one topic and price, cost, value, deal, expensive for another.

At this point, the model does not know what these topics mean. Humans interpret them and may label the first topic as shipping experience and the second as pricing concerns. These labels are applied after modeling, not before.

Each review is assigned a mixture of topics rather than a single category. One review might be mostly about shipping with a smaller emphasis on price, while another might be evenly split between product quality and customer service.

This case illustrates why topic modeling is exploratory. The discovered topics depend on modeling choices, and different reasonable solutions may reveal different but still useful views of the same text data.

Why would topic modeling be more appropriate than classification for this review dataset, and why does each document belong to multiple topics instead of just one?

---

## 22.3 Text Cleaning

#### Import Data and Packages

Let’s begin by importing the necessary packages and the dataset that we want to build topics from. We will use a dataset of social media posts:

```python
# Don't forget to mount Google Drive if you need to:
# from google.colab import drive
# drive.mount('/content/drive')

import pandas as pd

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/tweets_aws.csv')
df.head()
```

![Text Cleaning](../Images/Chapter22_images/df_tweets.png)

#### Remove Duplicates

Human language is messy and highly variable, so topic modeling usually benefits from cleaning steps that standardize the text before we extract topics. We will start by removing reposts and duplicate posts. This step is optional in many contexts, but it is especially helpful for social media data where reposts can overrepresent identical phrasing and distort topics.

```python
original_count = len(df)
print(f'Total tweets (including retweets/duplicates): {original_count}')

# Remove retweets (these can duplicate content and overweight certain phrases)
df = df.loc[~df['text'].str.contains(r"^RT\s+@", na=False)].copy()

# Remove exact duplicate posts (keep the first occurrence)
df = df.drop_duplicates(subset=['text']).copy()

remaining_count = len(df)
removed_count = original_count - remaining_count
print(f'Remaining tweets after filtering: {remaining_count}')
print(f'Removed tweets: {removed_count}')

# Output
# Total tweets (including retweets/duplicates): 1000
# Remaining tweets after filtering: 979
# Removed tweets: 21
```

In this dataset, 21 posts were removed because they were retweets or duplicates.

One of the major problems with social media posts is that a large volume of them are posted by bots. Bots are software programs designed to drive social media trends by posting automated content. Bots are a gray area when it comes to social media ethics. Many bots are useful and used for public services, such as emergency alerts. Others are used by organizations as part of a marketing strategy. There are also government-sponsored bots that, while operating on publicly available social media platforms and within legal boundaries, can influence politics (Aral and Eckles, 2019), impact elections (Linvil et al., 2019), and sow public discord concerning vaccinations (Walter et al., 2020).1

#### Remove Emails, URLs, and Other Unnecessary Characters

Next, we will remove line breaks, single quotes, email addresses, and URLs. More generally, we want to remove text patterns that (1) are highly unique from post to post and (2) rarely represent meaningful “topics” in the corpus. For example, email addresses and most URLs add variance without helping the model discover stable themes. The function below uses regular expressions (RegEx) to clean these patterns.

```python
import re

EMAIL_RE = re.compile(r"\S+@\S+\s?")
URL_RE = re.compile(r"http\S+")
WHITESPACE_RE = re.compile(r"\s+")

def re_mod(doc):
  doc = URL_RE.sub("url", doc)               # replace URLs with a stable token
  doc = EMAIL_RE.sub("", doc)                # remove emails
  doc = doc.replace("'", "")                 # remove single quotes
  doc = WHITESPACE_RE.sub(" ", doc).strip()  # normalize whitespace/newlines
  return doc

# Convert each tweet to a list of cleaned documents (faster than working inside the full DataFrame)
docs = df['text'].map(re_mod).tolist()

# Print the first five records to see what they look like
for doc in docs[:5]:
  print(doc)

# Output
# Amazon Web Services is becoming a nice predictable profit engine
# Announcing four new VPN features in our Sao Paulo Region.
# Are you an user? Use #Zadara + #AWS to enahnce your storage just one click away!
# AWS CloudFormation Adds Support for Amazon VPC NAT Gateway Amazon EC2 Container Registry and More  via
# AWS database migration service now available:
```

Here we map a cleaning function across the _text_ column to create a Python list of cleaned documents. This is often faster and simpler for NLP workflows than repeatedly transforming the entire DataFrame, especially as datasets grow.

At this point, _docs_ is a list of cleaned strings (one string per tweet). Next, we will tokenize each string and perform additional standardization steps that typically improve topic model quality.

#### Remove Stop Words and Puncuation; Lemmatization

Next, we will remove stop words and punctuation and lemmatize tokens to reduce variance and help the model discover more stable topics. There are several standard stop word lists available. For example, NLTK provides a list with 179 English stop words:

```python
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')

stop_words_nltk = set(w.lower() for w in stopwords.words('english'))

print(f'Stopwords in NLTK:\t{len(stop_words_nltk)}')
print(sorted(list(stop_words_nltk))[:40], '...')  # print a sample for readability

# After reviewing the LDA, return to add words you want to eliminate (case-insensitive)
stop_words_nltk |= {'aws', 'amazon', 'web', 'services', 'url'}

# Output:
# Stopwords in NLTK:	198
# ['a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'couldn', "couldn't", 'd', 'did', 'didn', "didn't", 'do', 'does', 'doesn', "doesn't", 'doing', 'don'] ...
# [nltk_data] Downloading package stopwords to /root/nltk_data...
# [nltk_data]   Unzipping corpora/stopwords.zip.
```

There is an even longer list available in spaCy, which we will use in this chapter because it removes more common filler terms by default:

```python
import spacy

# For cleaning/tokenization, we can disable heavier pipeline components for speed
nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])

stop_words_spacy = set(w.lower() for w in nlp.Defaults.stop_words)

print(f'Stopwords in spaCy:\t{len(stop_words_spacy)}')
print(sorted(list(stop_words_spacy))[:40], '...')  # print a sample for readability

# After reviewing the LDA, return to add words you want to eliminate (case-insensitive)
stop_words_spacy |= {'aws', 'amazon', 'web', 'services', 'url'}

# Output:
# Stopwords in spaCy:	326
# ["'d", "'ll", "'m", "'re", "'s", "'ve", 'a', 'about', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'amount', 'an', 'and', 'another', 'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are', 'around', 'as', 'at', 'back'] ...
```

Both packages allow you to modify stop word lists easily by adding or removing terms. For this dataset, we will use spaCy’s longer list and add AWS-related terms because every post is about AWS, so those words do not help distinguish topics. The functions below remove stop words and punctuation while lemmatizing tokens. The second version uses list comprehension and is typically faster. We will then process documents using _nlp.pipe()_ for better performance on larger datasets.

```python
# These two functions do the same thing; the second uses list comprehension (usually faster)
def docs_lemma_stop_loop(doc, stop_words):
  unigrams = []
  for token in doc:
    token_text = token.text.lower()
    token_lemma = token.lemma_.lower()
    if token.is_punct or token.is_space:
      continue
    if token_text in stop_words or token_lemma in stop_words:
      continue
    unigrams.append(token.lemma_)
  return unigrams

def docs_lemma_stop_comp(doc, stop_words):
  return [
    token.lemma_
    for token in doc
    if (not token.is_punct)
    and (not token.is_space)
    and (token.text.lower() not in stop_words)
    and (token.lemma_.lower() not in stop_words)
  ]

# Tokenize, lemmatize, and remove stop words efficiently using nlp.pipe
docs = [docs_lemma_stop_comp(doc, stop_words_spacy) for doc in nlp.pipe(docs, batch_size=1000)]

# Print the first five records to see what they look like
for doc in docs[:5]:
  print(doc)

# Output:
# ['nice', 'predictable', 'profit', 'engine']
# ['announce', 'new', 'vpn', 'feature', 'Sao', 'Paulo', 'Region']
# ['user', 'use', 'Zadara', '+', 'enahnce', 'storage', 'click', 'away']
# ['CloudFormation', 'add', 'Support', 'VPC', 'NAT', 'Gateway', 'EC2', 'Container', 'Registry', 'More']
# ['database', 'migration', 'service', 'available']
```

At this point, each document has been converted into a simple token list (a **bag of words** — A simplified document representation that treats text as a collection of tokens, typically ignoring grammar and word order, and often using token counts for modeling.) after cleaning, stop word removal, and lemmatization. A dataset-wide collection of documents is called a **corpus** — A collection of documents used as the input for text analytics and NLP tasks such as topic modeling.. In the next sections, we will build structures from this corpus (such as a dictionary and document-term representation) so that LDA can identify topics.

---

## 22.4 N-Grams

Now that we have a cleaned list of tokens for each document, we can optionally add multi-word phrases that appear together in the corpus. We call these phrases **n-grams** — Phrases made of n tokens that appear together in order within a corpus (for example: bigrams, trigrams, and fourgrams).. An n-gram is not an arbitrary combination of words; it must appear in the dataset in that order. For example, if a tweet becomes “like aws portal” after cleaning, then the bigrams “like aws” and “aws portal” could be created (if they occur often enough across the corpus), but “like portal” would not be created because those words were not adjacent in the original text. If the full phrase appears, a trigram like “like aws portal” can also be created.

```python
# !pip install gensim <- you may need to install this first
import gensim

# gensim.models.Phrases learns multi-word expressions that occur together frequently across the corpus.
# min_count controls how many times a candidate phrase must appear to be considered.
# threshold controls how strong the association must be to keep the phrase (higher = fewer phrases).
bigram = gensim.models.Phrases(docs, min_count=5, threshold=10)

# Train higher-order models using the transformed text from the prior model.
trigram = gensim.models.Phrases(bigram[docs], min_count=5, threshold=10)
fourgram = gensim.models.Phrases(trigram[docs], min_count=5, threshold=10)

# Convert to Phraser for faster, memory-efficient application.
bigram_mod = gensim.models.phrases.Phraser(bigram)
trigram_mod = gensim.models.phrases.Phraser(trigram)
fourgram_mod = gensim.models.phrases.Phraser(fourgram)
```

The objects _bigram_mod_, _trigram_mod_, and _fourgram_mod_ store the learned phrases and can be applied to documents to merge tokens into a single n-gram token (e.g., “Sao*Paulo”). The \_min_count* parameter requires that a phrase appears multiple times in the corpus before it can be kept. The _threshold_ parameter makes phrase selection stricter as it increases, which usually results in fewer n-grams.

The threshold score is computed using a scoring function described in the gensim documentation. If you want the exact formula and its interpretation, see gensim’s documentation. Practically, think of threshold as a “how confident should we be that these tokens belong together as a phrase?” setting.

- **gensim.models.phrases.original_scorer**(worda_count, wordb_count, bigram_count, len_vocab, min_count, corpus_word_count)
- **Formula:** ((bigram*count - min_count) * len*vocab) / (worda_count * wordb_count)
- Parameters:
- **Returns:** Score for a candidate phrase. Can be negative.
- **Return type:** float

A simple way to understand what these models learned is to list the phrases that made the cut under the current _min_count_ and _threshold_ settings.

```python
import pandas as pd

# Extract learned phrase keys (stored as bytes) and decode for clean display.
def decode_phrases(phrase_keys):
  decoded = []
  for k in phrase_keys:
    if isinstance(k, bytes):
      decoded.append(k.decode("utf-8"))
    else:
      decoded.append(str(k))
  return decoded

bigram_list = decode_phrases(bigram_mod.phrasegrams.keys())
trigram_list = decode_phrases(trigram_mod.phrasegrams.keys())
fourgram_list = decode_phrases(fourgram_mod.phrasegrams.keys())

all_ngrams = sorted(set(bigram_list + trigram_list + fourgram_list))

df_ngrams = pd.DataFrame(index=all_ngrams, columns=['bigrams', 'trigrams', 'fourgrams'])
df_ngrams.loc[bigram_list, 'bigrams'] = 'x'
df_ngrams.loc[trigram_list, 'trigrams'] = 'x'
df_ngrams.loc[fourgram_list, 'fourgrams'] = 'x'

pd.set_option('display.max_rows', None)
df_ngrams.sort_index()
```

![A table with bigrams, trigrams, and fourgrams listed that were generated based on the code above](../Images/Chapter22_images/df_ngrams.png)

The screenshot above shows only a portion of the learned n-grams. You can view the complete list in your notebook. Next, we will apply these phrase models to each document so that qualifying bigrams, trigrams, and fourgrams are merged into single tokens (for example, “Sao_Paulo”). This gives the topic model a better chance of treating a true phrase as a single concept instead of splitting it across multiple topics.

```python
def ngrams(docs, min_count=5, threshold=10):
  from gensim.models import Phrases
  from gensim.models.phrases import Phraser

  # Train n-gram models
  bigram = Phrases(docs, min_count=min_count, threshold=threshold)
  trigram = Phrases(bigram[docs], min_count=min_count, threshold=threshold)
  fourgram = Phrases(trigram[docs], min_count=min_count, threshold=threshold)

  # Freeze models for faster application
  bigram_mod = Phraser(bigram)
  trigram_mod = Phraser(trigram)
  fourgram_mod = Phraser(fourgram)

  # Apply models (order matters: bigram -> trigram -> fourgram)
  docs = [bigram_mod[doc] for doc in docs]
  docs = [trigram_mod[doc] for doc in docs]
  docs = [fourgram_mod[doc] for doc in docs]

  return docs

# Call the function
docs = ngrams(docs)

# Print some samples to see what happened
for doc in docs[:5]:
  print(doc)

# Output:
# ['nice', 'predictable', 'profit', 'engine']
# ['announce', 'new', 'vpn', 'feature', 'Sao_Paulo', 'Region']
# ['user', 'use', 'Zadara', '+', 'enahnce', 'storage', 'click', 'away']
# ['CloudFormation', 'add', 'Support', 'VPC', 'NAT', 'Gateway', 'EC2_Container', 'Registry', 'More']
# ['database', 'migration', 'service', 'available']
```

When you apply a phrase model to a document, gensim scans for token sequences that match learned phrases and merges them into a single token joined by an underscore (for example, “Sao_Paulo”). In the output above, only a couple of phrases were merged because our current settings require phrases to occur at least five times in the corpus and meet the threshold requirement.

One more optional refinement is to filter tokens by part of speech. Many analysts keep nouns, verbs, adjectives, and adverbs because they often carry the most topic meaning. We will also keep proper nouns to preserve names, products, and organizations that may define a topic.

```python
def filter_pos(docs, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV', 'PROPN']):
  docs_filtered = []

  # Use nlp.pipe for speed; each doc is re-joined so spaCy can assign POS tags
  for doc in nlp.pipe((" ".join(d) for d in docs), batch_size=1000):
    docs_filtered.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])

  return docs_filtered

# Call the function
docs = filter_pos(docs)

# Print some samples to see what happened
for doc in docs[:5]:
  print(doc)

# Output:
# ['nice', 'predictable', 'profit', 'engine']
# ['announce', 'new', 'vpn', 'feature', 'Sao_Paulo', 'Region']
# ['user', 'use', 'Zadara', 'enahnce', 'storage', 'click', 'away']
# ['CloudFormation', 'add', 'Support', 'VPC', 'NAT', 'Gateway', 'EC2_Container', 'Registry', 'more']
# ['database', 'migration', 'service', 'available']
```

Even after extensive cleaning, you will usually still see misspellings (for example, “enahnce”) and acronyms. That is normal. After we run the topic model, we will revisit our stop word list and remove tokens that do not help produce interpretable topics.

Now that the documents include relevant n-grams and optional POS filtering, we are ready to build topic models.

---

## 22.5 LDA Topic Modeling

Now that we have clean text—stop words removed, words lemmatized, and each tweet converted into a list of unigrams, bigrams, trigrams, and (optionally) fourgrams—we can use topic modeling to turn unstructured language into structured features for machine learning. One of the most widely used topic modeling techniques is **Latent Dirichlet allocation (LDA)** — A generative probabilistic model that explains documents as mixtures of hidden topics, where each topic is a probability distribution over words.. LDA helps us discover the major themes being discussed across a collection of documents, without needing labeled training data.

The goal of topic modeling (including LDA) is to discover a small set of topics such that (1) documents that discuss similar ideas receive similar topic mixtures, and (2) the topics themselves are as distinct as possible so they are easy to interpret. Study the diagram below to understand the conceptual process.

![Latent Dirichlet Allocation Process product reviews example. Corpus: the entire body of text represented by all records of a text column(s) in the dataset. Step 1: generate the corpus and hash it into features. There are 4 review boxes. Review 1: arrows point to the words ‘great’ and ‘gadget.’ Review 2: arrows point to the words ‘terrible’ and ‘support.’ Review 3: arrows point to the words ‘cool’ and ‘toy.’ Review 4: arrows point to the words ‘poor’ and ‘instructions.’ Step 2: identify latent topics that emerge. There are two boxes, topic 1: product and topic 2: customer service. Arrows connect topics to related words. Step 3: score each record across all topics. A table shows each review receiving topic scores; one review scores on both topics. A note explains that LDA allows each record to score on all or none of the extracted topics.](../Images/Chapter22_images/LDA_process_revised_2.png)

At a high level, LDA works like this: we first preprocess text so that each document becomes a standardized list of meaningful tokens (this is what we did in the prior sections). Next, we build a vocabulary across all documents (a dictionary) and convert each document into a compact numeric representation (a corpus). Then the LDA algorithm searches for topics by estimating which words tend to appear together across documents. Finally, the model produces topic scores for each document—meaning each tweet receives a mixture of topic probabilities that we can use as features in downstream predictive models.

Two ideas are especially important for understanding LDA output. First, each topic is a probability distribution over words, so a topic is defined by its highest-weight terms. Second, each document is a probability distribution over topics, so a single tweet can be mostly about one topic, evenly split across multiple topics, or unrelated to the main topics (depending on what the model learns).

#### Step 1: Generate the Dictionary and Corpus

In gensim, we start by building a dictionary and a corpus. A **dictionary** — A mapping from each unique token (word or n-gram) to an integer ID used by the topic model. is the model’s vocabulary. Each unique unigram/bigram/trigram/fourgram receives an ID so we can represent text numerically. We can create this dictionary from our cleaned documents like this.

```python
# Create Dictionary
from gensim import corpora

id2word = corpora.Dictionary(docs)

for row in id2word.iteritems():
  print(row)

# Output:
# (0, 'engine')
# (1, 'nice')
# (2, 'predictable')
# (3, 'profit')
# (4, 'Region')
# ... [through 2234]
```

The dictionary stores the vocabulary and the ID for each term. The loop is included only so you can see the structure and confirm that the dictionary was created correctly. In practice, the dictionary is mainly used to translate between (1) human-readable tokens and (2) the integer IDs expected by the modeling code.

Next, we convert each document into a bag-of-words representation called a topic modeling corpus. A **topic modeling corpus** — A list where each document is represented by (token_id, count) pairs derived from the dictionary. is a list of documents, and each document is represented as a list of (wordID, quantity) tuples. This is compact, fast to compute, and is the standard input format for LDA in gensim.

```python
# Create Corpus: Term Document Frequency
corpus = [id2word.doc2bow(text) for text in docs]
corpus

# Output: (wordID, quantity)
# [[(0, 1), (1, 1), (2, 1), (3, 1)], # This is the first Twitter/X post
#  [(4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)],  # Second post
#  ...
```

Each inner list corresponds to one tweet. Each tuple is (token_id, count). The token_id is the integer from the dictionary, and the count is how many times that term appears in that tweet. Tweets are short, so many counts will be 1, but longer documents often have larger counts. This corpus is the numeric “bridge” between language (words) and the probability model (LDA).

#### Step 2: Build the LDA

LDA requires you to choose the number of topics before training, similar to how k-means requires you to choose the number of clusters. Because you do not know the correct number of topics in advance, topic modeling is usually iterative: you fit several models with different topic counts and then evaluate which result is most interpretable and useful for your goal.

_random_state_ controls the random seed so that results are reproducible. _chunksize_ controls how many documents are processed at a time; larger values can speed training when you have more memory.

_passes_ is the number of full training iterations over the corpus (also called epochs). More passes often improves stability and topic quality, but increases runtime.

_per_word_topics_ tells gensim to compute extra per-word topic information. This is useful for deeper analysis, but it also makes the model a bit slower and more memory intensive.

```python
# Change the number of topics in the LDA here
topics = 4

# Build LDA model
lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                            id2word=id2word,
                                            num_topics=topics,
                                            random_state=1,
                                            chunksize=20,
                                            passes=10,
                                            per_word_topics=True)

ldatopics = lda_model.show_topics(formatted=False)
for topic in lda_model.print_topics():
  print(topic)

# Output:
# (0, '0.062*"support" + 0.040*"CloudComputing" + 0.024*"cloud" + ...')
```

Each printed line is one topic. Within a topic, each term is shown with a weight. These weights are not regression coefficients, but you can think of them as “importance scores” within that topic: higher-weight terms are more representative of the topic’s meaning.

A helpful way to read the topics is to ignore the decimals at first and focus on the top terms. Then give the topic a human label based on what those terms collectively suggest (for example, “Support & Help,” “Storage & S3,” or “New Product Launches”). Those human labels are not produced by the model—you create them after interpreting the results.

**Perplexity** — A statistical measure of how well a probability model predicts a sample. measures how well an LDA model statistically explains the observed words in the corpus. Lower perplexity indicates that the model is better at predicting the distribution of words in unseen documents, similar to how a lower error metric indicates better fit in other machine learning models. However, perplexity is optimized purely for mathematical likelihood and does not consider whether the resulting topics are meaningful or interpretable to humans.

**Coherence** — The degree to which a set of words or phrases are semantically related and interpretable as a unified concept. measures how well the high-probability words within a topic make sense together from a human perspective. High coherence occurs when topic words reinforce a clear underlying theme, while low coherence occurs when unrelated or noisy terms appear together due to statistical coincidence. In practice, coherence is often prioritized over perplexity when selecting the number of topics because it better reflects whether the topics are useful for interpretation and decision-making.

In summary, the goal is not to find a single value of _n_ that simultaneously optimizes all metrics, but rather to balance statistical fit and interpretability. Perplexity and coherence are often correlated in opposite directions: models with very low perplexity may produce topics that are difficult to interpret, while highly coherent topics may come at the cost of higher perplexity. As a result, topic modeling typically favors topic counts that maximize coherence while keeping perplexity within a reasonable range.

```python
from gensim.models import CoherenceModel

df_fit = pd.DataFrame(columns=['index', 'perplexity', 'coherence'])
df_fit.set_index('index', inplace=True)

for n in range(3,10):
  # Fit LDA model
  lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                              id2word=id2word,
                                              num_topics=n,
                                              random_state=1,
                                              chunksize=100,
                                              passes=5,
                                              per_word_topics=True)

  # Generate fit metrics
  coherence = CoherenceModel(model=lda_model, texts=docs, dictionary=id2word, coherence='c_v').get_coherence()
  perplexity = lda_model.log_perplexity(corpus)

  # Add metrics to df_fit
  df_fit.loc[n] = [perplexity, coherence]

df_fit
```

To make the tradeoffs easier to see, we can plot perplexity and coherence across topic counts. Because they are on different scales, we normalize both metrics before plotting.

```python
# Visualize results
import seaborn as sns, matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

# Normalize these scores to the same scale
scaler = MinMaxScaler()
df_fit[['perplexity', 'coherence']] = scaler.fit_transform(df_fit[['perplexity', 'coherence']])

plt.plot(df_fit.index, df_fit.perplexity, marker='o');
plt.plot(df_fit.index, df_fit.coherence, marker='o');
plt.legend(['Perplexity', 'Coherence'], loc='best')
plt.xlabel('Number of Topics')
plt.ylabel('Score')
plt.show()
```

![LDA Model Fit by Number of Topics. Line graph showing perplexity and coherence scores across different topic counts.](../Images/Chapter22_images/coherence_perplexity.png)

Answer the following questions to check your understanding of how perplexity and coherence are used when evaluating topic models.

1. Which metric primarily evaluates how well the model predicts word distributions mathematically, and which metric evaluates whether the topics are meaningful to humans?

2. Why might an LDA model with very low perplexity still produce topics that are difficult to interpret?

3. If two models have similar coherence scores but different perplexity scores, which model would you typically prefer for exploratory text analysis, and why?

You will not always find a topic count where coherence is maximized and perplexity is minimized at the same time. When they disagree, prioritize the metric that aligns with your goal: if you care about interpretability and explanation, coherence often matters more; if you care about predictive likelihood in a strict probabilistic sense, perplexity may matter more. In this example, coherence peaks and perplexity is also favorable around 8 topics, so we will proceed with an 8-topic solution.

#### Step 3: Score Topics

Once the LDA model is trained, we can convert its output into structured features. Specifically, we generate a topic score for each topic for each document. These topic scores behave like new numeric columns—one per topic—that can be added to a DataFrame and used in regression or classification models. Conceptually, each score is the model’s estimated probability that the document belongs to that topic.

```python
pd.options.display.max_colwidth = 50

lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                      id2word=id2word,
                                      num_topics=8,
                                      random_state=1,
                                      chunksize=100,
                                      passes=5,
                                      per_word_topics=True)

df_topics = df.copy()

num_topics = len(lda_model.get_topics()) # store the number of topics from the last model
for col in range(num_topics): # generate a new column for each topic
  df_topics[f'topic_{col + 1}'] = 0.0

# Store the topic score for each document
for i, words in enumerate(docs):
  doc = lda_model[id2word.doc2bow(words)] # generate a bow for this document

  for j, score in enumerate(doc[0]): # topic distribution for this document
    df_topics.iat[i, (len(df_topics.columns) - ((num_topics) - score[0]))] = score[1]

df_topics.head()
```

![Tweet Data with Topic Scores. A DataFrame showing original tweet fields plus topic_1 through topic_8 score columns.](../Images/Chapter22_images/tweets_with_topic_scores.png)

At this point, each tweet has been transformed into a vector of topic features (topic_1 through topic_8). These features can now be used like any other engineered variables—for example, to improve predictions of RetweetCount or to explain which themes tend to attract more engagement. In applied settings, this can support “draft-and-test” workflows where you predict performance before posting and revise the text to improve the predicted outcome.

---

## 22.6 Explore the LDA

Now that we have trained the LDA model, the next step is to interpret the results by understanding what each topic represents. Although topics are learned mathematically, they only become meaningful once we connect them back to the original text. One effective way to do this is to examine the documents that score highest on each topic, as these documents tend to contain the most representative language for that topic.

```python
# Display setting to show more characters in column
pd.options.display.max_colwidth = 200

# Create the output DataFrame to store representative documents
df_representative_tweets = pd.DataFrame(columns=['text'])

# Identify the most representative document for each topic
for n in range(1, num_topics + 1):
  topic_col = f'topic_{n}'
  top_index = df_topics[topic_col].idxmax()
  df_representative_tweets.loc[topic_col] = df_topics.loc[top_index]

df_representative_tweets
```

The table above shows, for each topic, the original tweet with the highest topic score. In theory, these documents should clearly reflect the underlying theme of each topic. In practice, however, interpretation can still be difficult when documents contain long URLs, hashtags, or other noisy text. For this reason, representative documents are often most useful when combined with topic-word inspection and visualization techniques, which we explore next.

---

## 22.7 Visualize the LDA

Besides the tabular reports we generated to help us interpret the topics, visualizations can reveal patterns that are hard to see in tables—especially document length issues, topic overlap, and “noisy” keywords that weaken topic separation.

#### Frequency Distribution

This histogram is a quick diagnostic for document length. Topic modeling can work with skewed document lengths, but extreme skews (many very short or very long posts) can reduce topic quality because short posts do not provide enough context and long posts can dominate the word distributions.

```python
# Frequency distribution of document lengths (number of words per post)
import matplotlib.pyplot as plt
import seaborn as sns

# Count words in each original post (before preprocessing) to understand raw-length variation
doc_lengths = df['text'].astype(str).apply(lambda s: len(s.split())).tolist()

sns.displot(doc_lengths, bins=17)
plt.gca().set(ylabel='Number of Posts', xlabel='Post Word Count')
plt.title('Distribution of Post Word Counts')
plt.show()
```

![Distribution of Tweet Word Counts. A bell-shaped curve with word count on the x-axis and number of tweets on the y-axis.](../Images/Chapter22_images/text_word_counts.png)

#### Clouds of Top N Keywords

This visualization helps you quickly interpret what each topic is “about” by showing the highest-weight keywords in that topic. Word clouds are intuitive, but treat them as an interpretive aid (not model validation): large words are higher-weight within a topic, but that does not necessarily mean the word is unique to that topic or important across the entire dataset.

```python
# 1. Wordcloud of Top N words in each topic (interpretive, not a fit metric)
import math
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Topic-word weights: list of (topic_id, [(word, weight), ...])
topics = lda_model.show_topics(formatted=False)

cols = [color for _, color in mcolors.TABLEAU_COLORS.items()]
k = len(topics)

matrix_size = math.ceil(k ** 0.5)
fig, axes = plt.subplots(matrix_size, matrix_size, figsize=(10, 10), sharex=True, sharey=True)
axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

# If you've already removed stop words upstream, you can keep this empty for speed.
# If you want a defensive layer, uncomment the next line.
stopwords_wc = set()  # or: stopwords_wc = set(stop_words_spacy)

# Reuse a single WordCloud object for speed; change color via a mutable topic index.
current_topic = {"i": 0}
def color_by_topic(*args, **kwargs):
  return cols[current_topic["i"] % len(cols)]

cloud = WordCloud(
  stopwords=stopwords_wc,
  background_color='white',
  width=2500,
  height=1800,
  max_words=20,
  random_state=1,
  color_func=color_by_topic
)

for i, ax in enumerate(axes):
  ax.axis('off')
  if i >= k:
    continue

  current_topic["i"] = i

  # topics[i] is (topic_id, [(word, weight), ...])
  topic_terms = topics[i][1]
  topic_words = dict(topic_terms)

  cloud.generate_from_frequencies(topic_words)
  ax.imshow(cloud, interpolation='bilinear')
  ax.set_title('Topic ' + str(i + 1), fontdict=dict(size=16))

plt.subplots_adjust(wspace=0, hspace=0)
plt.margins(x=0, y=0)
plt.tight_layout()
plt.show()
```

#### Topic Keywords Counts

This diagnostic compares two different ideas at once: (1) a keyword’s _topic weight_ (how strongly it defines that topic) and (2) its _frequency_ across the corpus (how often it appears). Use this chart to guide iterative refinement of your stop word list: keywords that appear in multiple topics reduce topic distinctiveness, and keywords that are extremely frequent but low-weight often behave like domain stop words (they add noise without improving interpretation).

```python
# Bar chart of word counts and word weights for each topic (diagnostic for stopword refinement)
import math
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from collections import Counter

topics = lda_model.show_topics(formatted=False)

# Flatten the cleaned corpus to compute overall word counts
data_flat = [w for w_list in docs for w in w_list]
counter = Counter(data_flat)

# Build a tidy DataFrame with one row per (topic, keyword)
out = []
for topic_id, topic_terms in topics:
  for word, weight in topic_terms:
    out.append([word, topic_id + 1, weight, counter.get(word, 0)])

df_temp = pd.DataFrame(out, columns=['word', 'topic_id', 'importance', 'word_count'])

# Plot Word Count and Weights of Topic Keywords
matrix_size = math.ceil(num_topics**(1/2))
fig, axes = plt.subplots(matrix_size, matrix_size, figsize=(20, 20), sharey=True, dpi=160)
cols = [color for _, color in mcolors.TABLEAU_COLORS.items()]

for i, ax in enumerate(axes.flatten()):
  if i >= len(topics):
    ax.axis('off')
    ax.title.set_visible(False)
    continue

  color = cols[i % len(cols)]
  topic_df = df_temp.loc[df_temp.topic_id == (i + 1), :]

  ax.bar(x='word', height='word_count', data=topic_df, width=0.5, alpha=0.3, label='Word Count')
  ax_twin = ax.twinx()
  ax_twin.bar(x='word', height='importance', data=topic_df, width=0.2, label='Weights')

  ax.set_ylabel('Word Count')
  ax.set_title('Topic: ' + str(i + 1), fontsize=16)
  ax.tick_params(axis='y', left=False)

  ax.set_xticks(ax.get_xticks())
  ax.set_xticklabels(topic_df['word'], rotation=30, horizontalalignment='right')

  ax.legend(loc='upper center')
  ax_twin.legend(loc='upper right')

fig.tight_layout(w_pad=2)
fig.suptitle('Word Count and Importance of Topic Keywords', fontsize=20, y=1.03)
plt.show()
```

Use the chart above to flag “problem terms” that weaken topic separation. Here are two practical heuristics: (1) remove terms that appear as top keywords in multiple topics because they do not distinguish topics well, and (2) consider removing terms that are extremely frequent but receive relatively low topic weights because they often behave like domain-specific stop words.

These are the terms I identified as problems based on those rules. But you may find some that I missed:

- **Topic 1:** learn (Rule 1)
- **Topic 2:** amp (Rule 1 & 2), support (Rule 1 & 2), cloud (Rule 1 & 2)
- **Topic 3:** cloud (Rule 1), Cloud (Rule 2) <- BTW, maybe we should have convered everything to lowercase so we don't get two versions of "Cloud"; it depends; does the proper noun version of a word make a difference in meaning? You have to decide that.
- **Topic 4:** region & Region (Rule 1) <- another instance where we may want to standardize case; another option if some capitalization differences matter and others don't is to search and replace in a targeted way word-by-word
- **Topic 5:** Region (Rule 1), join (Rule 2), learn (Rule 1)
- **Topic 6:** amp (Rule 1 & 2), support (Rule 1)
- **Topic 7:** amp (Rule 1 & 2, Cloud (Rule 1))
- **Topic 8:** amp (Rule 1 & 2), learn (Rule 1), use (Rule 1)

After you identify problem terms, return to your custom stop word list (see Section Not Found) and add those terms once each, then rerun the preprocessing and rebuild the LDA. Topic modeling is often iterative: you refine vocabulary, rerun the model, and re-check interpretability and overlap until the topics are both meaningful and reasonably distinct.

#### t-SNE Clustering Chart

This chart visualizes how documents relate to one another based on their topic mixtures. **t-SNE clustering** — A non-linear dimensionality reduction technique that projects high-dimensional data into a low-dimensional space while preserving local neighborhood similarity. In this context, each document is represented by a vector of topic scores (one score per topic), which exists in a high-dimensional space. t-SNE compresses those vectors into two dimensions so we can visually inspect whether documents with similar topic mixtures appear close together.

It is important to interpret t-SNE plots carefully. t-SNE excels at preserving _local_ structure, meaning that points that are close together in the plot tend to have similar topic mixtures. However, distances between far-apart points and the overall shape of clusters are not reliable indicators of global similarity or topic quality. As a result, t-SNE should be treated as an exploratory diagnostic tool rather than formal evidence that topics are correct or distinct.

Several parameters in the t-SNE model control how the visualization behaves. The _n_components_ parameter specifies that we want a two-dimensional projection for visualization. The _perplexity_ parameter controls how many neighboring points each document considers when forming local clusters; lower values emphasize very local structure, while higher values smooth clusters over larger neighborhoods. The _max_iter_ parameter determines how long the optimization runs, with larger values allowing the layout to stabilize more fully. The _random_state_ parameter ensures reproducibility so the same visualization is produced each time the code is run. Initializing with _init='pca'_ provides a stable starting configuration that often leads to more consistent results.

```python
# t-SNE visualization of document-topic mixtures (exploratory diagnostic)
import warnings
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn.manifold import TSNE

# Suppress warnings for cleaner output (optional)
warnings.filterwarnings('ignore')

# Extract topic weights for each document; lda_model[corpus] yields per-document topic mixtures
topic_weights = [[weight for topic_id, weight in doc_topics[0]] for doc_topics in lda_model[corpus]]

# Convert to a dense matrix (fill missing topics with 0)
arr = pd.DataFrame(topic_weights).fillna(0).values

# Optional: keep documents with a clear dominant topic to reduce visual clutter
mask_keep = np.max(arr, axis=1) > 0.35
arr = arr[mask_keep]

# Dominant topic index for each retained document
topic_num = np.argmax(arr, axis=1)

# Apply t-SNE dimensionality reduction
tsne_model = TSNE(
  n_components=2,
  verbose=1,
  random_state=1,
  perplexity=30,
  max_iter=1000,
  init='pca'
)
tsne_lda = tsne_model.fit_transform(arr)

# Visualize topic clusters using matplotlib
plt.figure(figsize=(12, 8))
colors = list(mcolors.TABLEAU_COLORS.values())

for topic_id in range(num_topics):
  topic_mask = topic_num == topic_id
  plt.scatter(
    tsne_lda[topic_mask, 0],
    tsne_lda[topic_mask, 1],
    c=colors[topic_id % len(colors)],
    label=f'Topic {topic_id + 1}',
    alpha=0.6,
    s=50
  )

plt.title(f't-SNE Clustering of {num_topics} LDA Topics', fontsize=16)
plt.xlabel('t-SNE Component 1')
plt.ylabel('t-SNE Component 2')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

![t-SNE Clustering Chart showing documents colored by dominant topic and positioned based on similarity of topic mixtures.](../Images/Chapter22_images/text_t_sne.png)

In summary, this t-SNE plot allows you to visually inspect whether documents with similar topic mixtures cluster together. Substantial overlap between multiple topic colors suggests that topics may be too similar, that additional stop word refinement is needed, or that the number of topics should be adjusted and re-evaluated. Clear separation, on the other hand, provides supporting evidence that the topics capture distinct patterns in the data.

---

## 22.8 Interactive Visualization: pyLDAvis

Finally, the pyLDAvis package provides an interactive visualization that brings together many of the concepts and diagnostics we have explored throughout this chapter. Rather than viewing topic keywords, topic distances, and topic prevalence separately, pyLDAvis allows you to explore all of them simultaneously in a single interactive interface. For this reason, pyLDAvis is one of the most widely used tools for interpreting and refining LDA models.

First, we need to install the package. Because pyLDAvis adds new dependencies to the Python environment, you will need to restart the runtime after installation.

```python
# Make sure to restart the runtime after installing pyLDAvis.
# You will need to rerun the prior steps to recreate docs, the dictionary, the corpus, and the LDA model.
!pip install pyLDAvis
```

After restarting the session and recreating the final LDA model, the visualization itself is generated with just a few lines of code. The output is an interactive object that renders directly inside the notebook.

```python
import warnings
import pyLDAvis
import pyLDAvis.gensim_models as gensimvis

# Suppress deprecation warnings for cleaner output
warnings.filterwarnings('ignore', category=DeprecationWarning)

pyLDAvis.enable_notebook()

vis = gensimvis.prepare(
  lda_model,
  corpus,
  dictionary=id2word
)
vis
```

Hover over each circle in the Intertopic Distance Map to inspect a topic. When you do, the panel on the right updates to show the most important terms for that topic along with their relative weights. Focus primarily on the top-ranked terms, as lower-ranked terms contribute far less to topic meaning.

Ideally, topics should appear as relatively small circles that are clearly separated from one another. Larger circles indicate that documents within a topic are more diverse, while overlapping circles suggest that two topics may be capturing similar patterns in the text. When substantial overlap occurs, it is often a sign that the number of topics should be adjusted or that additional stop words should be removed before retraining the model.

When assigning human-readable labels to topics, begin by examining the top 5–10 keywords for each topic in pyLDAvis. Look for a shared theme or concept that ties those terms together, and avoid relying on a single keyword in isolation. If a topic contains several generic or overlapping terms, consider refining the stop word list and rerunning the model before finalizing the label.

Why is topic overlap in the pyLDAvis Intertopic Distance Map often a signal to revisit model assumptions?

A. Because overlapping topics indicate an error in the visualization

B. Because overlapping topics suggest the corpus is too small

C. Because overlapping topics may indicate that topics are not sufficiently distinct and the model needs refinement

D. Because overlapping topics mean that LDA cannot be used on text data

**Correct answer:** C

---

## 22.9 Improving Model Fit

What was the purpose of all of this text analytics work? Ultimately, our goal is to generate new features from unstructured text that can improve the performance of predictive models. To evaluate whether the topic scores are actually useful, we will first build a baseline regression model to predict RetweetCount _without_ including any topic features. We explicitly drop the topic score variables so we can later compare model fit after adding them back in.

```python
# Generate a baseline model that doesn't include topic scores

import statsmodels.api as sm
import pandas as pd

df = df_topics.copy()

y = df['RetweetCount']
X = df.drop(columns=[
    'RetweetCount',
    'Reach',
    'text',
    'topic_1', 'topic_2', 'topic_3', 'topic_4',
    'topic_5', 'topic_6', 'topic_7', 'topic_8'
])

# Convert categorical variables to dummy variables and add intercept
X = pd.get_dummies(X, drop_first=True)
X = sm.add_constant(X)

results = sm.OLS(y, X.astype('float64')).fit()
print(results.summary())

# Output:
#                             OLS Regression Results
# ==============================================================================
# Dep. Variable:           RetweetCount   R-squared:                       0.215
# Model:                            OLS   Adj. R-squared:                  0.205
# Method:                 Least Squares   F-statistic:                     20.37
# Date:                Tue, 26 Mar 2024   Prob (F-statistic):           7.22e-43
# Time:                        06:09:50   Log-Likelihood:                -4255.7
# No. Observations:                 979   AIC:                             8539.
# Df Residuals:                     965   BIC:                             8608.
# Df Model:                          13
# Covariance Type:            nonrobust
# =====================================================================================
#                      coef    std err          t      P>|t|      [0.025      0.975]
# -------------------------------------------------------------------------------------
# Hour                  0.3644      0.116      3.153      0.002       0.138       0.591
# Day                  -0.1085      0.076     -1.421      0.156      -0.258       0.041
# Klout                 0.5355      0.051     10.522      0.000       0.436       0.635
# Sentiment             1.4602      0.680      2.149      0.032       0.127       2.794
# Gender_Male           1.2055      4.491      0.268      0.788      -7.608      10.019
# Gender_Unisex        -1.8570      5.630     -0.330      0.742     -12.905       9.191
# Gender_Unknown        6.1547      4.421      1.392      0.164      -2.521      14.830
# Weekday_Monday        1.2932      2.351      0.550      0.582      -3.321       5.907
# Weekday_Saturday      1.9597      3.156      0.621      0.535      -4.234       8.153
# Weekday_Sunday        1.5432      3.520      0.438      0.661      -5.364       8.451
# Weekday_Thursday      3.8403      2.106      1.824      0.068      -0.292       7.972
# Weekday_Tuesday       2.2534      2.162      1.042      0.297      -1.989       6.495
# Weekday_Wednesday     3.2981      2.201      1.498      0.134      -1.022       7.618
# const               -28.9080      5.473     -5.282      0.000     -39.648     -18.168
# ==============================================================================
# Omnibus:                      958.912   Durbin-Watson:                   2.020
# Prob(Omnibus):                  0.000   Jarque-Bera (JB):            56871.665
# Skew:                           4.438   Prob(JB):                         0.00
# Kurtosis:                      39.268   Cond. No.                     1.01e+03
# ==============================================================================
```

As you can see, the baseline model produces an R2 of 21.5 percent. This model relies only on structured predictors such as timing, user characteristics, and sentiment, and it serves as a reference point for evaluating whether the topic-based features add meaningful explanatory power.

```python
# Improved model that includes topic scores

X = df.drop(columns=['RetweetCount', 'Reach', 'text'])

# Convert categorical variables to dummy variables and add intercept
X = pd.get_dummies(X, drop_first=True)
X = sm.add_constant(X)

results = sm.OLS(y, X.astype('float64')).fit()
print(results.summary())

# Output:
#                         OLS Regression Results
# ==============================================================================
# Dep. Variable:           RetweetCount   R-squared:                       0.237
# Model:                            OLS   Adj. R-squared:                  0.221
# Method:                 Least Squares   F-statistic:                     14.17
# Date:                Wed, 24 Dec 2025   Prob (F-statistic):           1.83e-43
# Time:                        18:10:36   Log-Likelihood:                -4241.9
# No. Observations:                 979   AIC:                             8528.
# Df Residuals:                     957   BIC:                             8635.
# Df Model:                          21
# Covariance Type:            nonrobust
# =====================================================================================
#                         coef    std err          t      P>|t|      [0.025      0.975]
# -------------------------------------------------------------------------------------
# const               -60.7265     66.795     -0.909      0.363    -191.807      70.354
# Hour                  0.3506      0.115      3.036      0.002       0.124       0.577
# Day                  -0.0982      0.076     -1.291      0.197      -0.247       0.051
# Klout                 0.5183      0.053      9.780      0.000       0.414       0.622
# Sentiment             1.1686      0.681      1.716      0.086      -0.168       2.505
# topic_1              29.4179     67.472      0.436      0.663    -102.992     161.828
# topic_2              31.5802     67.464      0.468      0.640    -100.815     163.975
# topic_3              30.4059     67.572      0.450      0.653    -102.200     163.011
# topic_4              29.6185     67.711      0.437      0.662    -103.261     162.498
# topic_5              35.8723     67.630      0.530      0.596     -96.848     168.592
# topic_6              44.4798     67.537      0.659      0.510     -88.059     177.018
# topic_7              31.5553     68.047      0.464      0.643    -101.983     165.094
# topic_8              33.6880     67.654      0.498      0.619     -99.079     166.455
# Gender_Male           0.4924      4.454      0.111      0.912      -8.249       9.234
# Gender_Unisex        -1.8706      5.600     -0.334      0.738     -12.860       9.119
# Gender_Unknown        6.0678      4.389      1.382      0.167      -2.546      14.681
# Weekday_Monday        0.7376      2.339      0.315      0.753      -3.852       5.327
# Weekday_Saturday      2.1830      3.145      0.694      0.488      -3.989       8.355
# Weekday_Sunday        1.5335      3.516      0.436      0.663      -5.367       8.434
# Weekday_Thursday      3.3989      2.101      1.618      0.106      -0.723       7.521
# Weekday_Tuesday       2.1685      2.150      1.009      0.313      -2.050       6.387
# Weekday_Wednesday     2.9177      2.185      1.335      0.182      -1.370       7.205
# ==============================================================================
# Omnibus:                      955.830   Durbin-Watson:                   2.022
# Prob(Omnibus):                  0.000   Jarque-Bera (JB):            57605.143
# Skew:                           4.407   Prob(JB):                         0.00
# Kurtosis:                      39.531   Cond. No.                     2.30e+04
# ==============================================================================
```

With the topic scores included, the R2 increases to 23.9 percent—an improvement of approximately 2.4 percentage points. Topic scores behave as _soft, probabilistic features_, meaning that a single tweet can partially belong to multiple topics at the same time rather than being forced into a single category. This flexibility allows the model to capture nuanced semantic information that traditional categorical variables cannot represent.

You may notice that the individual topic coefficients are not statistically significant, even though overall model fit improves. This is common in topic models because topic scores tend to be correlated with one another and with other predictors, which inflates standard errors. In this context, our goal is prediction rather than causal interpretation of individual topics. The improvement in overall model performance demonstrates that topic-based features add useful information, even when their individual coefficients should not be interpreted in isolation.

More broadly, this example illustrates the central promise of text analytics. By transforming unstructured language into numeric representations, we can enrich traditional models with semantic information that would otherwise be discarded. Modern language models extend this same idea by replacing topic distributions with dense embeddings, but the underlying principle remains the same: better representations of text lead to better predictions.

---

## 22.10 Concepts Quiz

Complete these practice problems:

### 22.10 Text Analytics: Concepts Quiz

---

## 22.11 Practice

Consider working through the practice problems below. However, please note that these practice problems assume that you have also completed the practice in the prior chapter. You should consider working on those first if you haven't already completed them.

Next, we are going to try to improve the prediction by incorporating topic modeling. Since we have already removed stop words and punctuation and lemmatized the quotes in the practice problems from the prior chapter, let's proceed by calculating bigrams and trigrams for the tokenized lists in the token column. Follow the example in the chapter to calculate bigrams and trigrams. Do not worry about creating fourgrams.

You could simply modify the function in the chapter and remove the fourgrams. The resulting output should be a list of lists called "docs" that includes each cleaned token list along with the identified bigrams and trigrams. Print out the first five lists to preview the data.

The resulting output should look something like this:

```python
['true', 'wisdom', 'know', 'know']
['Knowledge', 'speak', 'wisdom', 'listen']
['investment', 'knowledge', 'pay', 'good', 'interest']
['wisdom', 'product', 'schooling', 'lifelong', 'attempt', 'acquire']
['seek', 'wisdom', 'step', 'silence', 'second', 'listening', 'remembering', 'fourth', 'practicing', 'fifth', 'teach']
```

Next, let's filter out unwanted parts of speech from the "docs" list we just created. Create a function or use the one from the chapter to remove anything that is not a: NOUN, ADJ, VERB, or ADV. Update the "docs" list of lists after removing these unwanted parts of speech. Print out the first five records to preview the data.

Does it appear that anything was removed? Answer: only the word "Knowledge" from the second list

The resulting output should look something like this:

```python
['true', 'wisdom', 'know', 'know']
['speak', 'wisdom', 'listen']
['investment', 'knowledge', 'pay', 'good', 'interest']
['wisdom', 'product', 'school', 'lifelong', 'attempt', 'acquire']
['seek', 'wisdom', 'step', 'silence', 'second', 'listening', 'remember', 'fourth', 'practice', 'fifth', 'teach']
```

Next, create the dictionary and corpus needed to build an LDA topic model. Preview the first five records of each to preview the data.

The resulting output should look something like this:

```python
[(0, 'know'), (1, 'true'), (2, 'wisdom'), (3, 'listen')]
[[(0, 2), (1, 1), (2, 1)],
 [(2, 1), (3, 1), (4, 1)],
 [(5, 1), (6, 1), (7, 1), (8, 1), (9, 1)],
 [(2, 1), (10, 1), (11, 1), (12, 1), (13, 1), (14, 1)],
 [(2, 1),
  (15, 1),
  (16, 1),
  (17, 1),
  (18, 1),
  (19, 1),
  (20, 1),
  (21, 1),
  (22, 1),
  (23, 1),
  (24, 1)]]
```

Next, let's determine how many topics should be in the LDA model. Follow the example from the book to generate a chart that visualizes the coherence and perplexity score of each model of topic size 2 to 10.

HINT: you may get a warning if you keep all of the LDA parameters we used in the book. I increased the number of passes to 10 to eliminate the warning. But you should try out a few values and see how it changes the results.

Based on these results, how many topics should we choose?

The resulting output should look something like this:

Based on the evidence above, let's go with the topic model with 7 topics because the coherence is above perplexity and it provides a broader range of potential topics than 3-4.

Build and save a model based on 7 topics. Then, add the topics scores, 1-7, to the DataFrame for each quote just like we did in the chapter. Print out the first five records to preview the results.

Based on these results, how many topics should we choose?

The resulting output should look something like this:

![practice text 6](../Images/Chapter22_images/practice_text_6.png)

Next, build another linear regression model like we did in the practice problems earlier where you predict the number of likes each quote received on goodreads.com using each of the numeric features from the DataFrame you've developed including the topic scores, sentiment, and the parts of speech from the prior chapter.

Recall that our last model got an R squared of 0.098. How does this expanded model compare? Answer: better, 0.232

Does the topic model or parts of speech do a better job of predicting likes? Answer: the topics because the coefficients are much larger

The resulting output should look something like this:

For our final practice problem, let's examine the details of the 7-topic LDA model. Use the pyLDAvis package to create a gensimvis interactive object.

Based on this interactive visualization, what is the primary topic that is most represented across all quotes about? Answer: Topic 1 appears to be about love and truth

The resulting output should look something like this:

![practice text 8](../Images/Chapter22_images/practice_text_8.png)

---

## 22.12 Assignment

Complete the assignment(s) below (if any).

---
