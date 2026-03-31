# Chapter 21: NLP: Linguistic Features

## Learning Objectives

- Students will be able to transform unstructured text into structured numeric features suitable for machine learning models
- Students will be able to apply tokenization, lemmatization, and part-of-speech tagging using spaCy
- Students will be able to extract named entities from text and classify them into semantic categories (persons, organizations, locations)
- Students will be able to create document-level features from token counts and linguistic properties for downstream modeling

---

## 21.1 Introduction

![Three word clouds created from movie scripts, where individual words form the shapes of iconic characters, illustrating how unstructured text can be visually summarized.](../Images/Chapter21_images/wordcloud.png)

The images above were created using the wordcloud package, movie scripts, and stencil images. Click the Google Colab link to explore the Python code used to generate these visual summaries.

Modern analytics problems increasingly involve large volumes of text data, such as customer reviews, survey responses, emails, social media posts, chat transcripts, and policy documents. Unlike numeric or categorical data, text does not arrive in a structured, model-ready format. As a result, traditional statistical and machine learning models cannot use raw text directly. Text analytics provides the tools needed to convert unstructured language into structured representations that can be analyzed, modeled, and interpreted.

The field of **text analytics** — The process of deriving structured, high-quality information from raw text. has evolved rapidly alongside advances in **natural language processing (NLP)** — A collection of algorithms and techniques used to analyze, interpret, and generate human language.. While modern NLP includes powerful large language models capable of generating long-form text, this chapter focuses on a more foundational and practical goal: preparing text data for use in predictive and descriptive models.

It is important to distinguish **unstructured text** from **categorical features**. Categorical features, such as gender or region, take on values from a fixed and known set of categories. Unstructured text, by contrast, has no predefined vocabulary, length, or format. Each document may contain entirely new words, phrases, or meanings. Because of this openness and variability, text must be transformed through specialized preprocessing and feature extraction steps before it can be treated similarly to categorical or numeric data.

In this book, we do not build new sentiment models or train large language models from scratch. Instead, we use pre-existing text analytics techniques to transform raw text into numeric features that can be incorporated into forecasting, classification, and clustering models. This process is commonly referred to as **text hashing** — The process of transforming raw text into standardized numeric features suitable for modeling.. Text hashing in this context is not related to cryptographic hashing. Rather, it is a feature engineering workflow that typically involves two high-level steps: cleaning and preparing the text, and converting the processed text into numeric features.

The remainder of this chapter follows a structured workflow that mirrors the modeling chapters you have already studied. We begin by breaking text into tokens, then examine grammatical structure through parts of speech and named entities, explore sentiment as a signal, and finally demonstrate how text-derived features can be integrated into forecasting, classification, and clustering models.

One key term used throughout this chapter is _document_. A **document** — Any single unit of unstructured text to be analyzed. refers to one piece of text associated with a single observation or case. A document may be very short, such as a tweet, or very long, such as an interview transcript. A dataset in text analytics typically consists of many documents, with one document per row.

Ask an AI assistant to explain how raw text from sources like customer reviews or social media posts can be transformed into numeric features for modeling. You might also ask how text features differ from traditional numeric or categorical variables, or how text analytics complements clustering and classification techniques.

**Concept Check:** Why must unstructured text be transformed before it can be used in predictive or clustering models, and how does this transformation differ from encoding categorical features?

---

## 21.2 Install Packages

As you work through this chapter and the next, you will be introduced to several Python packages commonly used for text analytics and natural language processing. If you are running code in Google Colab, most required packages are already installed. However, if you are using a local Python environment or a different IDE, you may need to install one or more of the packages listed below.

The code cell that follows includes installation commands for the libraries used throughout these chapters. Some lines are commented out because they are preinstalled in Colab or only needed for specific sections. You can uncomment individual lines as needed depending on your environment.

```python
# Some of these packages are already installed in Google Colab.
# If you are using a different environment, you may need to install them manually.

!pip install pyLDAvis
# !pip install bokeh
# !pip install gensim
# !pip install spacy
# !pip install nltk
# !pip install wordcloud
# !python -m spacy download en_core_web_sm
# !pip install translators --upgrade

# If you see a "Restart Runtime" prompt after running this cell,
# restart the runtime before continuing with the rest of the notebook.
```

After running the installation cell, you may see a message indicating that the runtime needs to be restarted. In Google Colab, this typically appears as a button labeled “Restart Runtime,” or you can manually restart by selecting Runtime → Restart runtime from the top menu. Restarting is especially important after installing or upgrading packages, as changes may not take effect until the environment is refreshed.

![Google Colab interface showing a Restart Runtime button that appears after installing or upgrading Python packages.](../Images/Chapter21_images/restart_runtime.png)

---

## 21.3 Tokenization

The first step in natural language processing is **tokenization**. Tokenization is the process of breaking unstructured text into smaller, linguistically meaningful units called _tokens_. These tokens form the foundational building blocks used to derive all subsequent text features for modeling.

**Tokenization** — The process of splitting unstructured text documents into individual tokens, such as words, punctuation, numbers, symbols, or emojis, according to language- and model-specific rules.
Unlike categorical features in structured datasets, raw text cannot be used directly in predictive or clustering models. Tokenization is therefore a required first step in transforming text into structured representations that can later be quantified, compared, and modeled.

Tokenization plays a role in text analytics similar to feature engineering in regression or clustering. Just as numeric features must be scaled or encoded before modeling, text must be segmented into tokens before higher-level linguistic features—such as parts of speech, named entities, or sentiment—can be extracted. Because token boundaries depend on modeling choices, tokenization reinforces the exploratory nature of text analytics.

Modern NLP libraries make tokenization straightforward. In this chapter, we use the _spaCy_ library, which provides a pre-trained language pipeline that performs tokenization as its first step. Importantly, this pipeline does more than split text—it also predicts multiple linguistic attributes for each token, which we will explore in later sections.

```python
import spacy

nlp = spacy.load("en_core_web_sm")
doc = nlp("This is a great social media post!")
for token in doc:
  print(token.text)

# Output:
# This
# is
# a
# great
# social
# media
# post
# !
```

Each item printed above is a token. Notice that punctuation is treated as its own token and that tokens are not limited to words alone. The _nlp_ object represents a full language-processing pipeline. When text is passed into this pipeline, it is tokenized and enriched with linguistic information such as part-of-speech tags, syntactic roles, named entities, and sentiment cues.

In practice, text analytics is often performed on datasets containing many documents. A common workflow is to apply the NLP pipeline to each document in a DataFrame and store the resulting processed document for reuse. This mirrors how derived features are stored in structured modeling workflows.

```python
import pandas as pd
import spacy

df_quotes = pd.DataFrame({
  'Person': ['Marge', 'Grandpa', 'Homer', 'Kent Brockman', 'Homer', 'Bart', 'Comic Book Guy', 'Homer', 'Ralph', 'Lisa'],
  'Quote': [
    "Better go upstairs and make sure the neds are still made.",
    "I used to be with it, but then they changed what 'it' was, and now what I'm with isn't it. And what's 'it' seems weird and scary to me.",
    "You don't win friends with salad.",
    "I, for one, welcome our new insect overlords.",
    "It takes two to lie: one to lie and one to listen.",
    "I can't promise I'll try, but I'll try to try.",
    "Loneliness and cheeseburgers are a dangerous mix.",
    "You tried your best and you failed miserably. The lesson is: Never try.",
    "My cat's breath smells like cat food.",
    "You mean those leagues where parents push their kids into vicious competition to compensate for their own failed dreams of glory?"
  ]
})

nlp = spacy.load("en_core_web_sm")
df_quotes['Tokenized'] = df_quotes['Quote'].apply(lambda x: nlp(x))
df_quotes
```

![Tokenization](../Images/Chapter21_images/df_tokenized.png)

The _.apply()_ function applies the NLP pipeline to each document in the dataset. Each row now contains a fully processed document object that can be queried repeatedly for different linguistic features. This avoids reprocessing the raw text and supports efficient feature extraction in later steps.

With tokenized documents in place, we can now move beyond surface-level text and begin extracting richer linguistic structure. In the next sections, we will use these tokens to identify parts of speech, recognize named entities, and estimate sentiment.

Ask an AI assistant to tokenize the same sentence using different rules (for example, treating emojis or hashtags as separate tokens). How do the resulting tokens differ, and how might those differences affect sentiment analysis or classification models?

**Concept Check:** Why must tokenization occur before extracting features such as parts of speech, named entities, or sentiment, and why can raw text not be treated as a simple categorical feature?

---

## 21.4 Parts of Speech

Once text has been tokenized, the next step in natural language processing is to understand **how** words function within a sentence. **Parts of speech (POS)** provide this structural information by categorizing each token based on its grammatical role (for example, noun, verb, or adjective). From a modeling perspective, POS features help capture writing style, sentence structure, and linguistic patterns that go beyond the presence or absence of specific words.

Unlike bag-of-words or TF–IDF features, which focus primarily on content, parts-of-speech features describe **how language is used**. These features are commonly used to enhance sentiment analysis, authorship detection, document classification, and clustering by providing signals about emphasis, action, descriptiveness, and complexity in text.

We will continue using the same spaCy language pipeline introduced in the tokenization section. In addition to tokenizing text, spaCy predicts multiple linguistic properties for each token. Run the code below and then examine the resulting table.

```python
import pandas as pd
import spacy

nlp = spacy.load("en_core_web_sm")
doc = nlp("This is a great post!")

df_ling = pd.DataFrame(columns=['text', 'lemma_', 'pos_', 'tag_', 'dep_', 'shape_', 'is_alpha', 'is_stop'])
df_ling.set_index('text', inplace=True)

for token in doc:
  df_ling.loc[token.text] = [
    token.lemma_,
    token.pos_,
    token.tag_,
    token.dep_,
    token.shape_,
    token.is_alpha,
    token.is_stop
  ]

df_ling
```

Each row in the table above corresponds to a single token from the document. The **lemma** — the base or dictionary form of a word standardizes different word forms (for example, converting “running,” “ran,” and “runs” into “run”). The **pos** — a coarse-grained grammatical category such as noun, verb, or adjective indicates how the word functions grammatically, while \_tag\_\_ provides a more detailed classification. The \_dep\_\_ value describes the syntactic dependency between tokens, capturing how words relate to one another within the sentence.

In practice, you will rarely use all of these linguistic properties in a predictive model. Most modeling applications focus on a small subset—such as lemma normalization, POS categories, and stop-word filtering—while treating others (like dependency structure) as optional or advanced. The goal is not to preserve full sentence structure, but to engineer informative, consistent features that models can learn from.

Because parts of speech are assigned based on context, spaCy can also visualize syntactic dependencies within a sentence. These visualizations illustrate why the same word can receive different POS tags depending on how it is used.

```python
from spacy import displacy
doc = nlp("This is a great post!")
displacy.render(doc, style="dep")
```

A key challenge in text analytics is that a single document produces many tokens, while predictive models require one row per document. To resolve this mismatch, analysts typically aggregate linguistic information—such as counting the number of nouns, verbs, or adjectives—into document-level features. This aggregation is a modeling choice that simplifies structure while retaining useful signals.

Parts-of-speech features are therefore best understood as **engineered summaries** of language. They are most effective when combined with other text features, such as sentiment scores, named entities, or TF–IDF representations, and when normalized to account for document length.

Ask an AI system to compare the parts-of-speech distributions of two documents (for example, a product review and a news article). How do differences in nouns, verbs, and adjectives reflect differences in purpose, tone, or writing style? How might those differences affect a classification or clustering model?

**Concept Check:** Why are parts-of-speech counts considered engineered features rather than intrinsic properties of a document, and what information is lost when sentence structure is reduced to POS counts?

---

## 21.5 Named Entity Recognition

In the Parts of Speech section, you saw that one useful category is **proper nouns**. Proper nouns often refer to meaningful real-world objects such as people, organizations, products, places, dates, and times. In many modeling problems, those references can be predictive. For example, posts that mention a brand, a city, or a specific event may behave differently than posts that do not.

To extract these references more precisely, we can apply **named entity recognition (NER)** — a natural language processing technique that identifies spans of text that refer to real-world entities and classifies them into categories such as PERSON, ORG, GPE, DATE, and TIME.. NER goes beyond identifying proper nouns because it finds multi-word spans (for example, _Santa Clara_) and assigns a specific entity type.

In this book, we use NER for the same reason we used token counts and parts-of-speech counts: to convert unstructured text into **structured features** that can be used in regression, classification, and clustering models. Common NER-based features include counts by entity type (for example, ORG_count), indicator flags (for example, contains_location), or the presence of particular high-value entities.

NER predictions are not perfect. Informal text (such as tweets) contains abbreviations, hashtags, and creative spelling that can reduce accuracy. Entities may be missed, mislabeled, or split into multiple spans. For modeling, this is normal: the goal is not perfection, but consistent features that capture meaningful patterns.

Let’s begin with a simple example using one short document.

```python
import pandas as pd
import spacy

nlp = spacy.load("en_core_web_sm")
doc = nlp("Jordan waited too long to buy stock in a company. Jordan visited the Santa Clara headquarters last June at 9am.")

df_ent = pd.DataFrame(columns=["text", "start_char", "end_char", "label_"])
df_ent.set_index("text", inplace=True)

for entity in doc.ents:
  df_ent.loc[entity.text] = [entity.start_char, entity.end_char, entity.label_]

df_ent
```

The **ents** property of a spaCy document returns the extracted entities. Each entity includes the span text, its character offsets (start*char and end_char), and a predicted category label (label*). This creates a compact, structured summary of key references embedded in the original unstructured sentence.

Below is a list of common named entity categories available in spaCy models. You do not need to memorize this list. Instead, focus on selecting entity types that make sense for your modeling goal (for example, organizations and locations might matter for market analysis, while dates and times might matter for operational logs).

spaCy includes a visualizer that highlights entity spans directly in the original text. This is a useful quality check before you engineer features, because you can quickly spot missing entities or obvious misclassifications.

```python
from spacy import displacy
displacy.render(doc, style="ent")
```

To use NER in modeling, we typically summarize entities into numeric features. One straightforward approach is to count how many entities of each type appear in a document. These counts can then be used as features alongside other text-based features (for example, word counts or sentiment) and non-text features (for example, weekday and hour).

```python
# Count entities of a given named entity type in a spaCy document
def count_entities(spacy_doc, entity_type):
  count = 0
  for ent in spacy_doc.ents:
    if ent.label_ == entity_type:
      count += 1
  return count

# Example counts (works best on realistic documents that mention people, places, or organizations)
df_quotes["People"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "PERSON"))
df_quotes["Organizations"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "ORG"))
df_quotes["Locations"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "GPE"))
df_quotes["Products"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "PRODUCT"))
df_quotes["Events"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "EVENT"))
df_quotes["Dates"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "DATE"))
df_quotes["Times"] = df_quotes["Quote"].apply(lambda x: count_entities(nlp(x), "TIME"))

df_quotes.head()
```

Toy datasets may contain few named entities, so do not be surprised if many counts are zero. In real-world text, entities are often more frequent. Next, we will validate these functions using a dataset where named entities are more likely to appear.

Download the Twitter/X dataset below, which contains posts that include the term _AWS_. Because social media includes abbreviations, hashtags, and user handles, you should expect some entity predictions to be noisy. The goal is to extract useful signals, not perfect linguistic labeling.

```python
# Mount Google Drive if needed
from google.colab import drive
drive.mount("/content/drive")

import pandas as pd

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/tweets_aws.csv")
df.head()
```

![A preview table of a tweets dataset showing multiple columns including metadata (such as weekday and hour), numeric measures (such as reach and retweet count), a text column, and a sentiment column.](../Images/Chapter21_images/df_X.png)

This dataset combines non-text variables (such as posting time) with text variables (the post content). It also includes outcome variables such as reach and retweet count that can serve as labels. This makes it a useful example for testing whether text-derived features (including named entity counts) improve model performance.

Now we can apply our text-cleaning, POS, and NER feature functions to generate additional columns. For larger datasets, this may take time. In practice, performance matters, so it is helpful to minimize repeated work (for example, by loading the spaCy model once and avoiding unnecessary recomputation).

```python
# Assumes you already defined clean_text() and count_pos() earlier in the chapter

nlp = spacy.load("en_core_web_sm")

df["Tokenized"] = df["text"].apply(lambda x: clean_text(nlp(x)))
df["Nouns"] = df["text"].apply(lambda x: count_pos(nlp(x), "NOUN"))
df["Verbs"] = df["text"].apply(lambda x: count_pos(nlp(x), "VERB"))
df["Adjectives"] = df["text"].apply(lambda x: count_pos(nlp(x), "ADJ"))
df["Numbers"] = df["text"].apply(lambda x: count_pos(nlp(x), "NUM"))
df["Pronouns"] = df["text"].apply(lambda x: count_pos(nlp(x), "PRON"))

df["People"] = df["text"].apply(lambda x: count_entities(nlp(x), "PERSON"))
df["Organizations"] = df["text"].apply(lambda x: count_entities(nlp(x), "ORG"))
df["Locations"] = df["text"].apply(lambda x: count_entities(nlp(x), "GPE"))
df["Products"] = df["text"].apply(lambda x: count_entities(nlp(x), "PRODUCT"))
df["Events"] = df["text"].apply(lambda x: count_entities(nlp(x), "EVENT"))
df["Dates"] = df["text"].apply(lambda x: count_entities(nlp(x), "DATE"))
df["Times"] = df["text"].apply(lambda x: count_entities(nlp(x), "TIME"))

df.head()
```

Notice that more named entities appear in this dataset than in toy examples. Also notice that this step can be computationally expensive. In later sections, you will learn additional techniques and data structures that make text feature engineering faster and more scalable.

At this point, we have engineered several structured features from text. In the next section, we will learn about sentiment, which provides a different kind of signal: the emotional or evaluative tone of a document. Together, POS, NER, and sentiment features often improve predictive models beyond what we can achieve with word counts alone.

Using the Twitter/X dataset, ask an AI assistant to propose three NER-based features that might help predict _Reach_ or _RetweetCount_. Then ask the assistant to explain how each feature could accidentally introduce label leakage, and how you could test for leakage in your modeling workflow.

**Concept Check:** Why might the same span of text be labeled as ORG in one document and not labeled at all in another, and how would you design NER-based features that remain useful even when entity predictions are noisy?

---

## 21.6 Sentiment

Text analytics has produced many pretrained NLP models that convert unstructured language into structured, model-ready features. One of the most widely used feature types is **sentiment** — The emotional tone expressed in text, often summarized as negative, neutral, and positive intensity.. In practice, sentiment does not measure whether a statement is true or correct; it estimates whether the language used sounds more negative, neutral, or positive. This makes sentiment useful for exploratory analysis and predictive modeling when emotion is related to an outcome (for example, whether a post gets shared or whether a review predicts future sales).

Sentiment is commonly reported in two ways. Some tools produce a single overall score (for example, a value on a -1 to 1 scale), while others produce separate scores for negative, neutral, and positive tone. Keeping negative and positive separate is often more informative for modeling because real text can express mixed emotions (for example, praising one feature while criticizing another). In those cases, combining everything into one score can hide important patterns.

Many sentiment systems are trained using supervised learning. Human raters label documents (such as reviews or tweets) as more negative, neutral, or positive. The text is then converted into features such as **n-grams** — A sequence of words in a particular order (for example, single words, pairs, or triples)., and a model learns which words and phrases tend to be associated with different types of sentiment. The resulting model can then score new documents by estimating how strongly the text expresses each sentiment category.

Training high-quality sentiment models can be expensive because it requires large datasets and consistent human judgments. Fortunately, many pretrained sentiment models are available. In this chapter, we will use VADER from the nltk package, which is designed for short, informal text (including punctuation and emphasis that often appear in social media).

```python
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download the VADER lexicon (Colab often already has it; this is safe to run)
nltk.download('vader_lexicon')

# Create the analyzer once and reuse it
sia = SentimentIntensityAnalyzer()

# Example
sia.polarity_scores("This is a really great tweet!")

# Output (example):
# {'neg': 0.0, 'neu': 0.461, 'pos': 0.539, 'compound': 0.6893}
```

The output includes four scores. The **neg**, **neu**, and **pos** values estimate the proportion of negative, neutral, and positive tone in the text. The **compound** value is a single overall summary score on a -1 to 1 scale. Try changing the text and punctuation to see how the scores shift. For example, exclamation points and intensifiers (such as “really”) often increase the strength of the score.

Next, we will calculate sentiment for the Twitter/X dataset about AWS introduced in the prior section. This dataset already includes a single sentiment score, but a single score can blend different emotional signals together. To keep the results interpretable and consistent with later modeling steps, we will create two separate numeric features: **sentiment_pos** and **sentiment_neg**. These can be used like any other numeric feature in regression, classification, or clustering models.

```python
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download once (safe in Colab even if already present)
nltk.download('vader_lexicon')

# Create one analyzer and reuse it for every document (more efficient)
sia = SentimentIntensityAnalyzer()

def sentiment_scores(text):
  """
  Return VADER sentiment scores for a single document as a dict:
  {'neg': ..., 'neu': ..., 'pos': ..., 'compound': ...}
  """
  return sia.polarity_scores(text)

# If you are reading from Google Drive in Colab, mount it first:
# from google.colab import drive
# drive.mount('/content/drive')

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/tweets_aws.csv')

# Create the same two output columns as before so your images do not need to change
df['sentiment_pos'] = df['text'].apply(lambda x: sentiment_scores(x)['pos'])
df['sentiment_neg'] = df['text'].apply(lambda x: sentiment_scores(x)['neg'])

df.head()
```

When interpreting these scores, remember that they represent estimated tone rather than a definitive label. A tweet can have both nonzero positive and negative values if it contains mixed language. Also, sentiment tools are not equally accurate in every domain: slang, sarcasm, technical jargon, and context outside the sentence can all reduce reliability. Treat sentiment as a helpful feature that may improve a model, not as ground truth.

Ask an AI assistant to (1) explain why VADER is often used for social media text, (2) give examples of sarcastic sentences that VADER might score incorrectly and explain why, and (3) recommend an alternative sentiment approach (lexicon-based or transformer-based) and describe the tradeoffs in speed, interpretability, and accuracy.

**Concept Check:** Why might it be more useful to include both sentiment_pos and sentiment_neg as separate features rather than using only the compound sentiment score, and what is one limitation of sentiment scores that could reduce predictive performance?

Now that we have generated sentiment features from unstructured text, we can test whether they improve prediction. In the next section, we will continue expanding our text-based feature set before incorporating these features into a modeling workflow.

---

## 21.7 Text Features in Modeling

In this section, we walk through a compact CRISP-DM workflow to build a regression model using Twitter/X data. Our objective is to predict **RetweetCount** and evaluate whether features derived from unstructured text meaningfully improve model performance.

We begin with a baseline model using only structured features, then incrementally add sentiment, parts-of-speech counts, and named-entity counts. This mirrors how text analytics typically supports modeling: not by replacing traditional features, but by augmenting them.

#### Import Data

First, import the Twitter/X dataset so that we can run through the entire process end-to-end.

```python
from google.colab import drive
drive.mount('/content/drive')

import pandas as pd

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/tweets_aws.csv')
df.head()
```

![Five records from the Twitter/X dataset showing structured features and the raw text column.](../Images/Chapter21_images/df_X.png)

#### Data Understanding

Before modeling, we examine category balance and check for missing data.

```python
print(df.Gender.value_counts())
df.isna().sum()
```

There are no missing values in this dataset. However, the _Female_ and _Unisex_ categories are small, which can destabilize dummy-coded regression coefficients. We therefore combine them into a single category.

#### Data Preparation

```python
df.loc[df.Gender == 'Female', 'Gender'] = 'Female_Unisex'
df.loc[df.Gender == 'Unisex', 'Gender'] = 'Female_Unisex'
```

Text analytics contributes during data preparation by transforming raw text into numeric features. Here, we generate sentiment scores, parts-of-speech counts, and named-entity counts. These features capture tone, writing style, and specificity while remaining compatible with tabular models.

The code below refactors earlier examples so each document is processed once with spaCy and reused across all feature calculations. Column names are preserved to match the figures shown later in this section.

```python
import nltk
import spacy
from nltk.sentiment import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

nlp = spacy.load("en_core_web_sm")
sia = SentimentIntensityAnalyzer()

def clean_text(doc):
  kept = []
  for token in doc:
    if not token.is_stop and token.pos_ != 'PUNCT':
      kept.append(token.lemma_)
  return tuple(kept)

def count_pos(doc, pos):
  return sum(1 for token in doc if token.pos_ == pos)

def count_entities(doc, label):
  return sum(1 for ent in doc.ents if ent.label_ == label)

# Sentiment features
df['sentiment_pos'] = df['text'].apply(lambda t: sia.polarity_scores(t)['pos'])
df['sentiment_neg'] = df['text'].apply(lambda t: sia.polarity_scores(t)['neg'])

# Parse all documents once
docs = list(nlp.pipe(df['text'].astype(str)))

# POS counts
df['Nouns'] = [count_pos(d, 'NOUN') for d in docs]
df['Verbs'] = [count_pos(d, 'VERB') for d in docs]
df['Adjectives'] = [count_pos(d, 'ADJ') for d in docs]
df['Numbers'] = [count_pos(d, 'NUM') for d in docs]
df['Pronouns'] = [count_pos(d, 'PRON') for d in docs]

# Named entities
df['People'] = [count_entities(d, 'PERSON') for d in docs]
df['Organizations'] = [count_entities(d, 'ORG') for d in docs]
df['Locations'] = [count_entities(d, 'GPE') for d in docs]
df['Products'] = [count_entities(d, 'PRODUCT') for d in docs]
df['Events'] = [count_entities(d, 'EVENT') for d in docs]
df['Dates'] = [count_entities(d, 'DATE') for d in docs]
df['Times'] = [count_entities(d, 'TIME') for d in docs]

df['Tokenized'] = [clean_text(d) for d in docs]

df.drop(columns=['Reach'], inplace=True)
df.head()
```

#### Modeling and Evaluation

We now predict _RetweetCount_ using linear regression. We report **R2** for comparability with earlier chapters and **MAE** for interpretability in the original units.

```python
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error

df_model = df.drop(columns=['text'])
y = df_model['RetweetCount']

def prep_X(df_in, cols):
  X = pd.get_dummies(df_in[cols], drop_first=True)
  X = pd.DataFrame(
    MinMaxScaler().fit_transform(X),
    columns=X.columns,
    index=X.index
  )
  return X

# Baseline model
X = prep_X(df_model, ['Gender', 'Weekday', 'Hour', 'Day', 'Klout'])
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1)

m0 = LinearRegression().fit(X_train, y_train)
pred0 = m0.predict(X_test)

print('R2 w/o sentiment:', round(m0.score(X_test, y_test), 3))
print('MAE w/o sentiment:', round(mean_absolute_error(y_test, pred0), 3))
```

Across successive models, performance improves modestly as text-derived features are added. While gains may appear small in this dataset, the broader takeaway is that unstructured text can provide measurable predictive signal when transformed thoughtfully.

Suggest three additional text-derived features that could plausibly improve prediction of _RetweetCount_. For each, explain why it might matter and where it would be engineered in the CRISP-DM process.

**Concept Check:** Why is it appropriate to drop the raw _text_ column after feature engineering, and why might different sentiment representations perform differently on the same dataset?

---

## 21.8 Summary: Creating Structure from Unstructured Data

So what should you take away from this chapter overall? The high level purpose of text analytics is to take unstructured text which was previously unusable in our predictive models and turn it into standardized features that may help to explain the variance in some label of interest in our dataset.

![Summary: Creating Structure from Unstructured Data](../Images/Chapter21_images/structure_unstructure.png)

In the past, we simply dropped the social media post text because the only thing we knew how to do with categorical data was to generate dummy codes. However, dummy codes are useless with unstructured text because it would result in n (rows) - 1 new features that each have only a single 1 value in the entire column to identify that unique text.

Now we are able to extract parts of speech, named entities, and sentiment scores from the unstructed text. We should carefully choose which parts of speech and named entities that we want to track based on the label(s) we are interested in predicting. For example, if we want to predict the reach of social media posts, then we would only create measures for parts of speech that have a theoretical reason for raising interest among social media consumers. Similarly, we would only care about named entities that people would be interested in talking about or reposting. Other contexts may favor different elements than those we tracked. So what should we do when there are so many text-based features that could possibly be generated?

Typically, we adopt one of two approaches. First, if you (or someone you can work with) has extensive domain knowledge about the text you are working with, then you take a theory-based approach. This is what I described in the prior paragraph about carefully considering what it is you need to predict or explain and which parts of speech and named entities are likely to influence those labels. Then you generate only those features you need. The other approach, which we sometimes call the "kitchen sink" approach, is to generate every possible named entity and part of speech and then use the techniques taught in this book about feature selection to select only those features that have an effect on the label(s) of interest. For example, you could generate features for all 20+ named entities and 20+ parts of speech and then calculate a permutation feature importance scoreNot Found to indicate which features contribute the most to the overall model fit and remove/delete the rest.

#### Top-Down vs Bottom-Up Approaches

These two options illustrate the two general approaches to model building and feature selection that we have discussed in this book before. The first approach uses "top-down" logic where theory informs what we collect, create, and use in our predictive models. This is tyipcally how academic, or peer-reviewed, research is performed. Researchers who perform what we call "positivistic" investigations first examine the work of prior researchers and summarize what others have learned to ensure that their new investigations are building on the works of others. They use the prior work of others to generate a theoretical lens and set of expectations for what they are about to investigate. This process is essential to make sure that they avoid "recreating the wheel" move along the domain of knowledge as far as possible.

![Summary: Creating Structure from Unstructured Data](../Images/Chapter21_images/top_down_bottom_up.png)

The second approach uses "bottom up" logic where you allow the data to tell you how to build the model based on the statistics you generate. This is how the medical and criminology disciplines operate because investigators do not want to be biased by any pre-conceived theories and, instead, let the data "do the talking" so to speak. The advantage here is that there is less temptation to allow your expectations to blind you to the realities of the data.

However, both approaches described above have their own biases. As mentioned, the "top-down" approach can be biased if the investigator has certain theory(ies) that he or she wants to prove. They might only consider data that tells the story that they want to tell. This is often realted to the phenomenon of **confirmation bias** — occurs when people only examine or consider data that fits their own expectations, desires, or beliefs and ignores data that is contrary to their hypotheses where people tend to ignore any evidence that is contrary to stories, logic, or principles they _want_ to believe. It may also lead to **faulty interpretations** — occurs when people make interpretations from data analyses that are better explained by either a simpler interpretation or one that has fewer logical leaps which occur when people explain results from analyses that are better explained by either a simpler interpretation or one that has fewer logical leaps.

The "bottom-up" approach can also be biased. What if there is no data available (or not enough data) to adequately tell the truth of a phenomenon? What if the investigators miss some data that is available because of simple human error? In that case, they will only consider a limited set of data and may come to a conclusion that fits the limited dataset but would ultimately be disproven if they had access to all data available. This bias is associated with the phenomena of **under-represented populations** — occurs when certain sub-populations are under-sampled in the dataset used to form analyses, conclusions, and/or predictions. and **analytics bias** — occurs when a data set is incomplete because it is lacking features that would logically explain important variance in the label of interest which occur when there are not enough rows representing the true distribution of sub-populations or enough features to adequately explain the variance in the label of interest. These can also lead to faulty interpretations as described above; except that the faulty interpretation is the result of incomplete data rather than confirmation bias.

#### Combating Biases of All Types

Hopefully, you can see how several types of biases are very relevant and tempting in data analytics and machine learning. As investigators, we must be aware of, and fight against, all types of biases to avoid coming to incorrect conclusions or it could cost us dearly with incorrect predictions in the future. How do we do that? Well, for the confirmation bias, we need to be prepared to admit when we are wrong. This is _very_ difficult for the average human being. People stake their careers on certain ideas and beliefs. Think of the religious and political belief systems around the world. We adhere to these beliefs dearly and may do anything to hold them up. However, if we don't find a way to allow our beliefs to coexist with data that may be contrary to them (assuming the data represent ground truth), we will ultimately fail either in business or other areas. This would be a good discussion topic in your class guided by your professor.

![Summary: Creating Structure from Unstructured Data](../Images/Chapter21_images/combating_bias.png)

To combat analytical bias and under-represented populations, we must also take careful steps to avoid misinterpretation. First, we should always examine the univariate statistics and visualizations that we generate to describe our distributions. Do they truly represent our population of interest or are they biased? For example, does our data represent the variation in age, gender, ethnicity, education, work experience, purchase history, etc. of the entire population we intend to make predictions around? If we ignore or speed past that examination, then we set ourselves up for faulty interpretations.

Okay, that is a decent start to this discussion for now. But we should talk again later about other related issues like whether there are features that should not be included in a model at all (even if we have the data and it is relevant) or whether there are labels that should not be predicted for ethical reasons. We will come back to those topics and they will also make great classroom discussions.

This chapter is only a small start to field of text analytics and the capabilities for turning unstructured text into structured, useful features. In the next chapter, we will take NLP modeling to another level and generate topic scores from the unstructured text which will add significantly more value to our predictive model.

---

## 21.9 Practice

Complete the practice problems below, which are based on the concepts and techniques introduced in this chapter. Note that the graded assignment for this chapter is combined with the next chapter, so you should complete these practice problems after finishing the chapter exercises.

Begin by importing the spaCy package. Then tokenize the following string and print each token on a separate line:

"I am going to practice this NLP stuff until I completely understand it"

The resulting output should look like this:

```python
I
am
going
to
practice
this
NLP
stuff
until
I
completely
understand
it
```

Next, copy the dictionary below and load it into a pandas DataFrame. You can do this by pasting the dictionary into a code cell and running _df = pd.DataFrame(quotes_dict)_. Then use the pandas _.apply()_ function to tokenize the quotes column. Store the tokenized results in a new DataFrame column named _tokens_, following the same approach used in the chapter.

```python
quotes_dict = {
  "authors": [
    "Socrates", "Jimi Hendrix", "Benjamin Franklin", "Albert Einstein", "Solomon Ibn Gabriol",
    "Socrates", "Nelson Mandela", "Plutarch", "Albert Einstein", "Herbert Spencer",
    "William Shakespeare", "David Viscott", "Eden Ahbez", "Aristotle", "The Beatles",
    "Anonymous", "Osho", "Johann Wolfgang von Goethe", "George Sand", "Audrey Hepburn",
    "Anonymous", "Charlie Chaplin", "Oscar Wilde", "Jim Carrey", "Mark Twain",
    "Anonymous", "Oscar Wilde", "Mitch Hedberg", "Steven Wright", "Oscar Wilde",
    "John Muir", "Albert Einstein", "Ralph Waldo Emerson", "Chief Seattle", "Ralph Waldo Emerson",
    "Jane Austen", "John Keats", "John Muir", "Henry David Thoreau", "Mahatma Gandhi",
    "Mark Twain", "Ralph Waldo Emerson", "Eleanor Roosevelt", "Ambrose Redmoon", "Muhammad Ali",
    "Japanese Proverb", "Mahatma Gandhi", "Susan Jeffers", "Ralph Waldo Emerson", "Eleanor Roosevelt",
    "Carl Sagan", "Neil deGrasse Tyson", "Carl Sagan", "Louis Pasteur", "Albert Einstein",
    "Rosalind Franklin", "Wernher von Braun", "Isaac Asimov", "Harold Urey", "William Penn",
    "Albert Einstein", "Lao Tzu", "Andy Warhol", "Nathaniel Hawthorne", "Benjamin Franklin",
    "Bertrand Russell", "Jack Kornfield", "Leonardo da Vinci", "Menander", "Aeschylus",
    "Buddha", "George Orwell", "George Washington", "Oscar Wilde", "Sojourner Truth",
    "Aldous Huxley", "Thomas Jefferson", "Mark Twain", "Mark Twain"
  ],
  "quotes": [
    "The only true wisdom is in knowing you know nothing.",
    "Knowledge speaks, but wisdom listens.",
    "An investment in knowledge pays the best interest.",
    "Wisdom is not a product of schooling but of the lifelong attempt to acquire it.",
    "In seeking wisdom, the first step is silence, the second listening, the third remembering, the fourth practicing, the fifth teaching others.",
    "The unexamined life is not worth living.",
    "Education is the most powerful weapon which you can use to change the world.",
    "The mind is not a vessel to be filled, but a fire to be kindled.",
    "The measure of intelligence is the ability to change.",
    "Science is organized knowledge. Wisdom is organized life.",
    "Love all, trust a few, do wrong to none.",
    "To love and be loved is to feel the sun from both sides.",
    "The greatest thing you’ll ever learn is just to love and be loved in return.",
    "Love is composed of a single soul inhabiting two bodies.",
    "In the end, the love you take is equal to the love you make.",
    "You don’t love someone for their looks, their clothes, or their fancy car, but because they sing a song only you can hear.",
    "Love is not about possession. Love is about appreciation.",
    "We are shaped and fashioned by what we love.",
    "There is only one happiness in this life: to love and be loved.",
    "The best thing to hold onto in life is each other.",
    "I'm not arguing; I'm just explaining why I'm right.",
    "A day without laughter is a day wasted.",
    "I can resist everything except temptation.",
    "Behind every great man is a woman rolling her eyes.",
    "Age is an issue of mind over matter. If you don’t mind, it doesn’t matter.",
    "Common sense is like deodorant. The people who need it most never use it.",
    "I am so clever that sometimes I don’t understand a single word of what I am saying.",
    "My fake plants died because I did not pretend to water them.",
    "A clear conscience is usually the sign of a bad memory.",
    "The best way to appreciate your job is to imagine yourself without one.",
    "In every walk with nature, one receives far more than he seeks.",
    "Look deep into nature, and then you will understand everything better.",
    "Adopt the pace of nature: her secret is patience.",
    "The Earth does not belong to us: we belong to the Earth.",
    "Nature always wears the colors of the spirit.",
    "To sit in the shade on a fine day and look upon verdure is the most perfect refreshment.",
    "The poetry of the earth is never dead.",
    "The clearest way into the Universe is through a forest wilderness.",
    "Heaven is under our feet as well as over our heads.",
    "Earth provides enough to satisfy every man’s needs, but not every man’s greed.",
    "Courage is resistance to fear, mastery of fear—not absence of fear.",
    "Do the thing you fear, and the death of fear is certain.",
    "You gain strength, courage, and confidence by every experience in which you really stop to look fear in the face.",
    "Courage is not the absence of fear, but the ability to act despite it.",
    "He who is not courageous enough to take risks will accomplish nothing in life.",
    "Fear is only as deep as the mind allows.",
    "It is not the strength of the body that counts, but the strength of the spirit.",
    "Feel the fear and do it anyway.",
    "A hero is no braver than an ordinary man, but he is brave five minutes longer.",
    "Do one thing every day that scares you.",
    "Science is a way of thinking much more than it is a body of knowledge.",
    "The good thing about science is it's true whether or not you believe in it.",
    "Somewhere, something incredible is waiting to be known.",
    "Science knows no country, because knowledge belongs to humanity, and is the torch which illuminates the world.",
    "The important thing is to never stop questioning.",
    "Science and everyday life cannot and should not be separated.",
    "Research is what I'm doing when I don't know what I'm doing.",
    "The saddest aspect of life right now is that science gathers knowledge faster than society gathers wisdom.",
    "In science, there are no shortcuts to truth.",
    "Time is what we want most, but what we use worst.",
    "The only reason for time is so that everything doesn't happen at once.",
    "Time is a created thing. To say 'I don't have time' is to say 'I don't want to'.",
    "They always say time changes things, but you actually have to change them yourself.",
    "Time flies over us, but leaves its shadow behind.",
    "Lost time is never found again.",
    "Time you enjoy wasting is not wasted time.",
    "The trouble is, you think you have time.",
    "Time is the wisest counselor of all.",
    "Time brings all things to pass.",
    "Three things cannot be long hidden: the sun, the moon, and the truth.",
    "In a time of deceit telling the truth is a revolutionary act.",
    "Truth will ultimately prevail where there is pains to bring it to light.",
    "A thing is not necessarily true because a man dies for it.",
    "Truth is powerful and it prevails.",
    "Facts do not cease to exist because they are ignored.",
    "The truth will set you free, but first it will make you miserable.",
    "Honesty is the first chapter in the book of wisdom.",
    "Truth is stranger than fiction, but it is because Fiction is obliged to stick to possibilities; Truth isn’t.",
    "If you tell the truth, you don’t have to remember anything.",
  ],
  "likes": [15340, 3616, 851, 1239, 0, 7203, 6274, 2304, 4315, 775, 37110, 451, 262, 83, 6017, 7749, 2253, 491, 3253, 1799, 6, 14964, 5, 79, 247, 1, 71279, 26, 27, 0, 587, 916, 2033, 45, 42, 197, 1065, 3655, 795, 2301, 3335, 401, 6692, 0, 222, 1, 1122, 132, 928, 0, 2743, 8007, 30, 6798, 42, 37, 13807, 0, 6, 1235, 565, 2181, 2907, 13, 19213, 20949, 10350, 43, 567, 135, 8307, 0, 8087, 1284, 10465, 5, 1555, 6003, 91206]
}
```

The resulting DataFrame should look similar to the output shown below:

![Practice](../Images/Chapter21_images/practice_text_1.png)

Next, perform additional text cleaning and feature engineering. Create (or reuse from the chapter) two functions. The first function should remove stop words, lemmatize tokens, and exclude punctuation from a tokenized document. The second function should count the number of occurrences of a specified part of speech (e.g., NOUN, VERB, ADJ) in a tokenized document and return that count.

Apply the first function to overwrite the existing _tokens_ column with its cleaned version. Then use the second function to create new columns for the following parts of speech: NOUN, VERB, ADJ, PRON, and PROPN.

Print the first five rows of the modified DataFrame. Your output should resemble the example below:

![Practice](../Images/Chapter21_images/practice_text_2.png)

Next, add sentiment scores to the DataFrame. Using the nltk _SentimentIntensityAnalyzer_, calculate positive and negative sentiment scores for each quote and store them in new columns. You may reuse the sentiment function from the chapter or write your own. Print the first five rows to review the results.

![Practice](../Images/Chapter21_images/practice_text_3.png)

This dataset includes a numeric feature called _likes_, which represents the number of likes each quote received on the Goodreads website. Build a linear regression model to predict _likes_ using the numeric text-based features you created. Apply a MinMaxScaler to the predictors so the coefficients are comparable. Because the dataset is small, you do not need to split it into training and testing sets.

What is the R2 value of the model? Answer: 0.098

Which feature appears to have the largest impact on likes? Answer: pronouns

Does positive or negative sentiment have a larger effect on likes? Answer: positive sentiment

Based on this result, does sentiment make quotes more or less likeable? Answer: more

If you completed all of these exercises, well done. The practice problems in the next chapter build directly on the skills you developed here.

---
