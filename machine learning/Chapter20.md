# Chapter 20: Cluster Modeling

## Learning Objectives

- Students will be able to distinguish between supervised and unsupervised machine learning and explain when clustering is appropriate
- Students will be able to implement k-means clustering in Python and interpret cluster centroids and assignments
- Students will be able to evaluate cluster quality using the elbow method, silhouette scores, and the Calinski-Harabasz criterion
- Students will be able to normalize and standardize features appropriately before applying distance-based clustering algorithms

---

## 20.1 Introduction

![Introduction](../Images/Chapter20_images/clustering_banner.png)

Who are our customers? Although this will sound very impersonal, the answer to that question is not a set of names; it’s a set of data that describes why they would or would not want to purchase our products. _If_ (really big _if_) all of our customers varied along a single dimension (i.e., income) and _if_ that single dimension explained all variance in purchasing (which it never does) and _if_ we could only offer one model of bicycle, then our problem is simple: we need to target customers with more income.

But it is never that simple. There are many variables that explain why customers do or do not purchase our products. For example, customers with lower income also desire bicycles, but they may not be as discerning about owning a top brand name. The purpose of the bicycle is also different for each person. People with children may want leisure bicycles for family bike rides in addition to performance bicycles for exercise. Therefore, answering the question “Who are our customers?” requires us to gather all relevant information about customers that would explain why they may want to purchase any and all of our product offerings. If we can answer it successfully, then we can customize different product offerings, service commitments, and marketing campaigns to fit the needs of each type of customer.

Once that data is gathered, **cluster analysis** — A form of unsupervised machine learning that assigns cases to groups such that the distance between cases from the center of their assigned group is minimized while the distance between cluster groupings is maximized. is the solution. The algorithms learned for modeling and prediction in prior chapters are referred to as **supervised** — Machine learning algorithms that require a label. machine learning algorithms because they require a clear, single label among multiple features. Sometimes we may not have a label. In these cases, we would use **unsupervised** — Machine learning algorithms that do not require a label. machine learning algorithms, which means that we either do not know or do not care to differentiate between cause and effect variables (or “features” and “label”) such as cause="income, age, commute distance" and effect="purchased bike". Rather, clustering will assign customers to segments based on how they naturally group together along multiple dimensions. Formally stated, the objective of cluster analysis is to assign observations to groups (“clusters” or “segments”) so that observations within each group are similar to one another with respect to variables or attributes of interest and the groups themselves stand apart from one another.

![3D plot of commute distance, income, and children with three clusters of dots highlighted.](../Images/Chapter20_images/3d scatterplot.png)

In addition to the market segmentation example above, other examples of clustering might include grouping customers based on credit risk, employees based on performance, insurance policyholders based on claim history, houses based on type and value, elementary school students based on reading or math ability, and much more. Clustering includes five primary steps:

1. **Data Collection:** Gather the data points to be clustered.
1. **Feature Selection:** Identify the features or attributes based on which the clustering will be performed.
1. **Similarity Measure:** Choose a metric to measure the similarity or distance between data points (e.g., Euclidean distance, Manhattan distance).
1. **Clustering Algorithm:** Apply a clustering algorithm to group the data points. Common algorithms include:
1. **Cluster Evaluation:** Assess the quality of the clusters using internal metrics (e.g., silhouette score) or external metrics if true labels are available.

---

## 20.2 Select a Distance Measure

The first step in the cluster analysis is to determine a measure for the distance between two observations (i.e., cases or data points). When only a single variable or dimension is being considered, this task is easy. For example, consider the ages of these five customers: 21, 23, 24, 40, 41. It’s pretty easy to see two basic groups: the younger customers (21, 23, 24) and the older customers (40, 41). But what if there are several variables, like age, income, and number of children? What if there are several dozen variables? This task becomes a bit more complex and requires a special distance measure. There are many types of distance measures for many different scenarios (e.g., Chebychev, Minkowski, Mahalanobis, Manhattan, maximum distance, cosine similarity, simple correlation). We’re going to learn and use the simplest (and most common) measure: **Euclidean distance** — A distance measure calculated as the square root of the sum of squared differences across each dimension of two observations., which is calculated as the square root of the sum of squared differences across each dimension of two observations. The formula for Euclidean distance is as follows:

d
(

p

,

q

# )

d
(

q

,

p

# )

(

q
1

−

p
1

)
2

- (

q
2

−

p
2

)
2

- .
  .
  .
- (

q
n

−

p
n

)
2

=

∑

# i

1

n

(

q
i

−

p
i

)
2

Follow along with the video below to understand Euclidean distance:

---

## 20.3 Select a Clustering Algorithm

Now that we have defined a distance measure for cases (rows of data), we need to choose a clustering algorithm. There are many different algorithms, and each applies best in different scenarios (i.e., with different data types). Below are two of the most common algorithm categories, examples of each, and their use cases:

- Centroid-based clustering:

Details: works best with uniform, standardized, continuous data; simple and effective; easy to understand and implement; very fast

Example: K-means is the most well-known algorithm

Common distance measures: Euclidean distance, Manhattan distance, Minkowski distance

- Connectivity-based clustering (Hierarchical Clustering)

Details: used for categorical data

Example: Divisive analysis (DIANA) or Agglomerative Nesting (AGNES)

Common distance measures: Ward’s Distance, Centroid Distance, average linkage, complete linkage, single linkage.

### K-Means

Since k-means is the most common as well as the most easily understood, let’s begin there. Follow along with the video below for a high-level visual overview of how k-means works.

In summary, there are four basic steps to the k-means algorithm:

1. Determine the number of cluster groups you want and randomly initialize their respective center points. See the section below for help determining the appropriate number of clusters.
1. Classify each data point by computing the distance between that point and each group center, then classify the point in the group whose center is closest to it.
1. Based on these classified points, recompute the group center by taking the mean of all the vectors in the group.
1. Repeat these steps for a set number of iterations or until the group centers don’t change much between iterations.

Again, there are several advantages and disadvantages to k-means:

- Advantages

Can be used with any numeric data type (but should be normalized)

Faster than other algorithms

Easy to understand and interpret

- Disadvantages

Does not work well for non-linear data

Must determine the number of clusters beforehand

Does not work well for categorical or binary (0/1) data

Does not handle outliers well

### How Many Clusters?

Typically, the k-means algorithm requires the analyst to specify exactly how many clusters to segment the data into a priori. In cases where there is reliable theory to explain why there should be _n_ number of clusters, it is appropriate to simply self-select the number of desired clusters. However, what if you don’t know the right number of clusters? There are a variety of metrics that can help you choose, such as the Calinski-Harabasz criterion:

S

S
B

S

S
W

×

(
N
−
k
)

(
k
−
1
)

_SSB_ is the overall between-cluster variance and _SSW_ is the overall within-cluster variance. Lowercase _k_ is the number of clusters and _N_ is the number of observations. The greater the value of this ratio, the more cohesive the clusters (i.e., low within-cluster variance) are and the more distinct or separate the individual clusters (i.e., high between-cluster variance) are.

This score is calculated for many different numbers of clusters (2 through n) until the criterion has found the maximum possible value of this score. Then the criterion will use that number of clusters as its default suggestion. Python will make it easy to implement and use criteria like this.

---

## 20.4 In Python: K-means

Now that we have reviewed the theoretical concept of clustering, let’s use Python to perform clustering on a sample dataset.

This dataset is a set of survey questions that measure the Big Five personality traits (agreeableness, conscientiousness, extraversion, intellect, neuroticism) and Hofstede’s six cultural dimensions (individualism/collectivism, indulgence/restraint, long-term orientation, femininity/masculinity, power distance, uncertainty avoidance). The surveys were taken by a cohort of 216 students entering an information systems program. The results were clustered into two groups and used to determine the team composition for the program. In the first semester, students were placed into four-person teams with others in the same cluster (homogenous condition). In the second semester, the students were placed into teams consisting of two members of each cluster (heterogeneous condition). This practice was proven to maximize student learning and confidence.1

K-means is one of the most widely used clustering algorithms because it is simple, fast, and often produces useful groupings on numeric data. The core idea is to represent each cluster using a **centroid** (a point that acts like the “center” of the cluster) and then assign each data point to the nearest centroid.

To decide what “nearest” means, k-means typically uses **Euclidean distance**, which measures straight-line distance between points in a multi-dimensional space. For two points in two dimensions, the Euclidean distance is:

For example, suppose we have two points A = (2, 1) and B = (5, 5). The Euclidean distance is:

K-means repeats a simple two-step process until it stabilizes:

1. **Assignment step:** Assign each point to the closest centroid (based on Euclidean distance).
1. **Update step:** Recompute each centroid as the average (mean) of the points assigned to that cluster.

A quick centroid example: suppose Cluster 1 contains three points (2, 1), (4, 1), and (3, 2). The centroid is simply the mean of each coordinate: centroid = ( (2+4+3)/3 , (1+1+2)/3 ) = (3, 4/3 ). This “average point” becomes the new center used in the next assignment step.

Because k-means depends on Euclidean distance, feature scaling is critical. If one variable has a much larger numeric range than others, it can dominate distance calculations and effectively control the clustering outcome. Later in this section, you will see why clustering is usually performed on standardized or normalized features.

Finally, k-means is not guaranteed to find the single best clustering solution. It can converge to different solutions depending on its starting centroids. This is why practical implementations often run k-means multiple times with different initializations and keep the best result.

### Determine Number of Clusters

With the k-means algorithm, we must determine how many clusters we want beforehand. There are several ways to do this. You may have domain knowledge (i.e., theory) that tells you how many clusters there _should_ be in the dataset, and you can simply use that number. But if you do not know how many clusters there should be, you can use one of the many metrics available to make that determination. We will use three:

- Within-Cluster Sum of Squares (WCSS; the Elbow Method)

Identify the “elbow” in the curve where the slope transitions from < -1 to > -1

- Calinski-Harabasz (CH) Criterion

Highest score is best

- Silhouette Analysis

Highest score is best

To keep this discussion brief, we will not go into the mathematical details behind each method. But rather, we will simply learn how to interpret the criterion. Begin by importing the data along with some packages we will need for later:

```python
import pandas as pd
import plotly.express as px
import seaborn as sns
import sklearn.cluster as cluster
from sklearn.cluster import KMeans
from scipy.spatial import distance as sdist

df = pd.read_csv('https://www.ishelp.info/data/personality.csv')
df.head()

# Output:
# [See the results in your own notebook]
```

Next, examine the data. Each latent (i.e., unobservable or unmeasurable) factor such as _agreeableness_ or _individualism_ is measured using three to five survey questions. There are 11 averaged scores at the end of the DataFrame, and these scores are a simple average of the three to five questions for every concept. While there are more scientific ways of reducing dimensions, we will not address them here. Therefore, let’s reduce the DataFrame down to just those 11 averaged scores:

```python
df = df[['AGR', 'CON', 'EXT', 'INT', 'NEU', 'IDV', 'IVR', 'LTO', 'MAS', 'PD', 'UAI']]
df.head()
```

![Determine Number of Clusters](../Images/Chapter20_images/personality_data.png)

Let’s begin with the Calinski and Harabasz (CH) criterion. The process we will follow is a loop from 2 to _n_ that trains a cluster model using _n_ number of clusters. The CH criterion is calculated based on the scored labels of each k-means model. The model with the highest CH score represents the optimal number of clusters to use.

```python
# Calinski and Harabasz Criterion
from sklearn.metrics import calinski_harabasz_score
from matplotlib import pyplot as plt

ch_score = []
for n in range(2, 21):
  kmeans = KMeans(n, random_state=12345).fit(df)
  ch_score.append(calinski_harabasz_score(df, labels=kmeans.labels_))

plt.plot(range(2, 21), ch_score, 'bx-')
plt.xlabel('number of clusters')
plt.ylabel('Calinski_Harabasz Criterion')
plt.title('Optimal Number of Clusters')
plt.text(12, 40, 'Higher is better', bbox=dict(facecolor='red', alpha=0.5))
plt.show()
```

The curve above is highest when x = 2; or, in other words, for a k-means cluster model based on only two clusters.

Now we will use another criterion referred to as the _silhouette_ score using the same process as the CH criterion:

```python
# Silhouette Analysis
from sklearn.metrics import silhouette_score

si_score = []
for n in range(2, 21):
kmeans = KMeans(n, random_state=12345).fit(df)
si_score.append(silhouette_score(df, kmeans.labels_))

plt.plot(range(2, 21), si_score, 'bx-')
plt.xlabel('number of clusters')
plt.ylabel('Silhouette score')
plt.title('Optimal Number of Clusters')
plt.text(11, .14, 'Higher is better', bbox=dict(facecolor='red', alpha=0.5))
plt.show()
```

Once again, the curve is highest when x = 2. So far, it appears that a two-cluster model is best. But let’s try one more method that works a bit differently.

The _within-cluster sum of squares_ (i.e., “elbow method”) is based on the sum of squared residuals measuring the distance between each case and the center of the cluster they are assigned to. These scores are generated by the k-means model itself and do not require an additional package. Rather than looking for the highest point on the curve, we are looking for the elbow of the curve; or, in other words, the point where the slope shifts from < -1 to > -1

```python
# WCSS: Elbow Method

ss_score = []
for n in range(2,21):
    kmeans = KMeans(n, random_state=12345).fit(df)
    ss_score.append(kmeans.inertia_)

# Where does the slope bend? Find the highest (least negative) slope.
changes = []
for n in range(2, 20):
  changes.append(float(ss_score[n - 1] - ss_score[n - 2]))

optimal_n = changes.index(max(changes))

plt.plot(range(2,21), ss_score, 'bx-', markevery=[optimal_n])
plt.xlabel('number of clusters')
plt.ylabel('SS distance')
plt.title('Optimal Number of Clusters')
plt.text(8, 900, 'The point where slope "bends" from a \ndecreasing to increasing rate of change', bbox=dict(facecolor='red', alpha=0.5))
plt.show()
```

This curve is somewhat deceiving because the scale of the y and x axes are so different. The actual elbow is found at n = 19. To see this for yourself, try printing out the list ss_score. You will see that the rate of slope change shifts most dramatically at n = 19.

Which criterion should we use? Two of them say that two clusters is best, and one says that 19 is best. This is why we calculate multiple criteria—they do not always agree, and it helps to get the big picture. Personally, I find there are often times when I need larger or smaller numbers of clusters. In other words, it depends on what your use case, or context of use, is. If you need a larger number of clusters, then go with 19 (as opposed to 18 or 20). If you need a smaller number, then choose two (as opposed to three).

### Final K-Means Model

Let’s assume that we need a smaller number of clusters and, therefore, choose two. Now we should build our final model—much the same way that we did to calculate the metrics above. This time, we substitute '2' for 'n' and keep the random state (while testing). In addition, we will create a new DataFrame and add the assigned cluster labels.

```python
# Train the model
kmeans = KMeans(2, random_state=12345).fit(df)

# Add assigned clusters to a new DataFrame
df_wcluster = df.copy()
df_wcluster['cluster'] = kmeans.labels_
df_wcluster.head()
```

Notice that you have a new column with the assigned cluster (0 or 1) of each row. Now that we have cluster assignments, we might want to know which factors played the largest role in separating the data into clusters. We can do this by calculating the mean scores of all features separately for both clusters and then sorting them by the difference (greatest to least):

```python
# Which features played the largest role in determining clusters?

output_df = pd.DataFrame({'C0_means': df_wcluster[df_wcluster.cluster == 0].mean(), 'C1_means': df_wcluster[df_wcluster.cluster == 1].mean()})
output_df['diff'] = abs(output_df['C0_means'] - output_df['C1_means'])
output_df.drop(['cluster']).sort_values(by=['diff'], ascending=False)
```

![The 11 sums as rows with CO_means and C1_means and diff as columns in a table.](../Images/Chapter20_images/cluster_means.png)

Based on these results, extraversion, neuroticism, agreeableness, and intellect (in that order) play the largest role in determining which cluster each student is assigned to. Next, let’s use the trained k-means model to predict and assign the placement of a new student who registered late to the appropriate cluster:

```python
import pandas as pd

# Make cluster predictions for new data
# Define the column names based on the training DataFrame (df)
feature_names = ['AGR', 'CON', 'EXT', 'INT', 'NEU', 'IDV', 'IVR', 'LTO', 'MAS', 'PD', 'UAI']

# Create a DataFrame for the new data with the correct column names
new_data = pd.DataFrame([[5.2, 3.4, 3, 1, 6, 5.9, 4.6, 2.7, 3.3, 5, 4]], columns=feature_names)

prediction = kmeans.predict(new_data)[0]
print(f'Case with values AGR=5.2, CON=3.4, EXT=3, INT=1, NEU=6, IND=5.9, IVR=4.6, LTO=2.7, MAS=3.3, PD=5, UAI=4 predicted to be in cluster: {prediction}')

# Output:
# Case with values AGR=5.2, CON=3.4, EXT=3, INT=1, NEU=6, IND=5.9, IVR=4.6, LTO=2.7, MAS=3.3, PD=5, UAI=4 predicted to be in cluster: 0
```

Finally, let’s visualize the cluster assignment as well as possible. We can use the plotly package to build a 3D scatterplot based on the largest three features determining the clusters:

```python
# Create 3D Scatterplot to visualize cluster

fig = px.scatter_3d(df_wcluster, x='EXT', y='NEU', z='INT', color='cluster', size_max=20, opacity=1.0)
fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
fig.show()
```

![3D scatterplot with two main clusters, one purple and one yellow, at the top right of the graph.](../Images/Chapter20_images/cluster_scatter.png)

---

## 20.5 Hierarchical Clustering

![Side-by-side conceptual comparison of two clustering approaches: (1) K-means shown as colored points grouped around centroids using straight-line (Euclidean) distance, and (2) hierarchical clustering shown as points connected through progressive merges resembling a dendrogram, emphasizing nested cluster structure rather than fixed centroids.](../Images/Chapter20_images/clustering_types.png)

The personality data in the previous section was well suited for k-means clustering because it consisted entirely of numeric variables measured on comparable scales. However, k-means relies on strong assumptions: numeric features, meaningful Euclidean distance, and roughly spherical cluster shapes. When these assumptions are violated—such as when data contain mixed variable types or very different measurement scales—k-means often performs poorly. In such cases, **hierarchical clustering** provides a more flexible alternative.

Hierarchical clustering is an **unsupervised learning** technique that builds clusters by progressively merging or splitting observations based on their similarity. Rather than assigning observations directly to a fixed number of clusters, hierarchical clustering constructs a hierarchy (or tree) of nested clusters that reveals structure at multiple levels of granularity.

This approach differs fundamentally from k-means. K-means assigns observations to cluster centroids and optimizes a global objective function, requiring the number of clusters to be specified in advance. Hierarchical clustering, by contrast, compares observations directly to one another, does not rely on centroids, and does not require choosing the number of clusters upfront.

There are two primary forms of hierarchical clustering. **Agglomerative** clustering follows a bottom-up approach, starting with each observation in its own cluster and repeatedly merging the closest clusters. **Divisive** clustering follows a top-down approach, starting with all observations in a single cluster and recursively splitting them. In practice, agglomerative clustering is far more common and is the approach used throughout this section.

To illustrate hierarchical clustering, we will use the insurance dataset. Unlike the personality data, this dataset contains a mixture of numeric and categorical variables with very different scales. This makes it an ideal example for demonstrating distance measures and clustering algorithms that are robust to mixed data types.

```python
df = pd.read_csv('https://www.ishelp.info/data/insurance.csv')
df.head()
```

Because hierarchical clustering compares observations directly, the choice of distance metric is critical. When datasets include mixed variable types, **Gower distance** is often preferred over Euclidean distance. Gower distance computes a single similarity score between two observations by combining scaled numeric differences and categorical mismatches into one unified measure.

Conceptually, Gower distance plays the same role as Euclidean distance: it quantifies how similar or dissimilar two observations are. The key difference is that Gower distance automatically handles dummy coding, normalization, and mixed data types, allowing heterogeneous variables to contribute fairly to the overall distance.

```python
!pip install gower

import gower
distance_matrix = gower.gower_matrix(df)
pd.DataFrame(distance_matrix).head()
```

![A square, symmetric matrix of pairwise Gower distances for the insurance dataset, with values between 0 and 1 and zeros along the diagonal.](../Images/Chapter20_images/gower_matrix.png)

The resulting distance matrix contains the pairwise distances between every pair of observations. Each row and column correspond to a single case, and each cell represents how dissimilar two cases are. The diagonal is zero because every observation is identical to itself. In hierarchical clustering, this distance matrix replaces the original feature matrix as the primary input to the algorithm.

Agglomerative clustering proceeds through a simple but powerful sequence of steps. First, each observation begins as its own cluster. Next, the two closest clusters—according to the chosen distance metric—are merged. Distances between the newly formed cluster and all remaining clusters are then recomputed. This process repeats until all observations have been merged into a single hierarchy.

```python
from sklearn.cluster import AgglomerativeClustering

agg = AgglomerativeClustering(metric="precomputed", linkage="average").fit(distance_matrix)
df['agg_cluster'] = agg.labels_
df.head()
```

Notice that the initial cluster assignments align closely with the _smoker_ variable. As discussed earlier, smoking status is strongly associated with insurance charges, making it a dominant driver of similarity. Hierarchical clustering faithfully reflects this structure by grouping smokers and non-smokers together.

This example illustrates an important principle of clustering: dominant features can overwhelm other patterns. To explore alternative structures in the data, analysts often remove or transform highly influential variables before clustering.

```python
distance_matrix = gower.gower_matrix(df[['age', 'sex', 'bmi', 'children', 'region', 'charges']])
agg = AgglomerativeClustering(metric="precomputed", linkage="average").fit(distance_matrix)
df['agg_cluster_no_smoker'] = agg.labels_
df.head()
```

![Insurance dataset preview showing updated agglomerative cluster labels after removing the smoker variable, revealing an alternative clustering structure beyond smoker status.](../Images/Chapter20_images/data_with_agg_clusters.png)

Conceptually, hierarchical clustering produces a tree-like structure known as a **dendrogram**. Each merge represents a node in the tree, and cutting the tree at different heights yields different numbers of clusters. Although scikit-learn returns cluster labels directly, it is helpful to remember that these labels correspond to a particular cut of an underlying hierarchy. The code below gives an example of the dendrogram created from this cluster model.

```python
import matplotlib.pyplot as plt

from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage, dendrogram, fcluster

# Option A: with smoker included (will often dominate)
# distance_matrix = gower.gower_matrix(df)

# Option B: remove smoker to see different structure (often better for teaching)
cols = ['age', 'sex', 'bmi', 'children', 'region', 'charges']
distance_matrix = gower.gower_matrix(df[cols])

# Convert NxN distance matrix -> condensed distance vector required by scipy
condensed = squareform(distance_matrix, checks=False)

# Hierarchical linkage (match your "average" linkage choice)
Z = linkage(condensed, method='average')

# Plot dendrogram
plt.figure(figsize=(14, 6))
dendrogram(
  Z,
  truncate_mode='lastp',   # keeps it readable; shows the last p merges
  p=30,                    # increase/decrease for more/less detail
  leaf_rotation=45,
  leaf_font_size=10
)
plt.title('Hierarchical Clustering Dendrogram (Average Linkage, Gower Distance)')
plt.xlabel('Cluster Merge (truncated)')
plt.ylabel('Distance (Gower)')
plt.tight_layout()
plt.show()

# OPTIONAL: choose a number of clusters and create labels from the dendrogram cut
k = 2
labels_from_cut = fcluster(Z, t=k, criterion='maxclust')
df['agg_cluster_cut'] = labels_from_cut
df[['agg_cluster_cut']].head()
```

To read a dendrogram, focus on the vertical axis: higher merges indicate that two clusters were more dissimilar when they were combined. Large vertical “jumps” suggest natural separation points, where cutting the tree just below the jump often yields clusters that are more distinct.

This dendrogram is truncated to keep it readable, so each leaf label may represent a small group of observations rather than a single record. Even with truncation, the key interpretation is the same: merges that occur at relatively low Gower distances represent very similar groups, while merges at higher distances represent broader, more heterogeneous combinations.

Another way to view hierarchical clustering results is to project the pairwise Gower distances down to a two-dimensional map using **multidimensional scaling (MDS)**, as shown in the code below. This allows you to see whether the chosen number of clusters produces a clear separation in a 2D view.

```python
from sklearn.manifold import MDS

X = df[cols]

# Gower distance matrix
distance_matrix = gower.gower_matrix(X)

# Fit hierarchical clustering on the precomputed distances
agg = AgglomerativeClustering(
  n_clusters=2,            # pick a number for visualization
  metric='precomputed',
  linkage='average'
)
labels = agg.fit_predict(distance_matrix)

# 2D embedding from distances
mds = MDS(
  n_components=2,
  dissimilarity='precomputed',
  random_state=1,
  normalized_stress='auto'
)
coords = mds.fit_transform(distance_matrix)

# Plot
plt.figure(figsize=(9, 6))
plt.scatter(coords[:, 0], coords[:, 1], c=labels, s=20)
plt.title('MDS Map of Insurance Data (Gower Distance) Colored by Hierarchical Clusters')
plt.xlabel('MDS Dimension 1')
plt.ylabel('MDS Dimension 2')
plt.tight_layout()
plt.show()
```

![Two-dimensional MDS scatterplot of the insurance dataset based on Gower distances, with points colored by a two-cluster hierarchical clustering solution; clusters appear as separated groups primarily along the first MDS dimension, illustrating how distance-based structure can be visualized in 2D.](../Images/Chapter20_images/mds_map.png)

In the MDS map, points that are closer together represent customers who are more similar across the mixed insurance features (according to Gower distance). Clear separation between color groups suggests that the clustering solution captures meaningful structure in the distance matrix, while heavy overlap suggests the chosen clustering cut may be forcing groups that are not strongly separated.

Because MDS is a projection, the axis values themselves do not have a direct business meaning, and some distortion is expected when compressing many dimensions into two. Use this plot as a qualitative diagnostic for separation, not as proof that the clusters are “correct.”

Finally, similar to what we did with the k-means cluster model, we can summarize feature averages and distributions by cluster to create an inferential description of each group. This step turns cluster labels into an interpretable story (for example, “higher charges and higher BMI,” or “more common in certain regions”), which is essential when communicating clustering results to stakeholders.

```python
# If you already have df and labels from either method above:
df_viz = df.copy()
df_viz['cluster'] = labels  # or df['agg_cluster_no_smoker'], etc.

# Numeric summary by cluster
display(df_viz.groupby('cluster')[['age', 'bmi', 'children', 'charges']].mean().round(2))

# Categorical distribution by cluster (example: region)
display(pd.crosstab(df_viz['region'], df_viz['cluster'], normalize='columns').round(3))

# Output:
#         age     bmi  children   charges
# cluster
# 0      39.36   30.13     1.09  12527.43
# 1      38.96   31.51     1.10  14457.77

# cluster       0       1
# region
# northeast  0.196   0.317
# northwest  0.395   0.000
# southeast  0.213   0.367
# southwest  0.197   0.317
```

According to these results, we might interpret that region has the largest impact on the cluster definition with those who are NOT in the northwest make up Cluster 1 while Cluster 0 includes everyone in the northwest as well as people in other regions.

Hierarchical clustering requires storing and processing all pairwise distances, which scales quadratically with the number of observations. As a result, hierarchical methods are best suited for small to medium-sized datasets where interpretability and flexibility outweigh computational cost.

In practice, hierarchical clustering is most appropriate when data include mixed types, when the number of clusters is unknown, when interpretability matters, or when analysts want to explore structure at multiple levels of granularity.

### Linkage

Hierarchical algorithms differ primarily in how they define **linkage**, or the distance between clusters. Single linkage considers the closest pair of points, complete linkage considers the farthest pair, and average linkage computes the mean distance between all points in two clusters. Average linkage, used here, often provides a balanced compromise between sensitivity to noise and overly compact clusters.

**Concept Check:** Why does hierarchical clustering not require specifying the number of clusters in advance, and how does the choice of linkage influence the final cluster assignments?

---

## 20.6 Cluster Algorithm Summary

Because there are many clustering algorithms and many different data geometries, scikit-learn provides a helpful visualization that compares common clustering methods side-by-side on the same set of example datasets. The purpose of this figure is not to identify a single “best” clustering algorithm, but to illustrate how different algorithmic assumptions shape the clusters that emerge.

To read the figure below, focus on two dimensions. Each **column** corresponds to a different clustering algorithm, while each **row** corresponds to a different underlying data geometry (such as rings, spirals, blobs, or anisotropic clusters). Each point represents a data record, and its **color** indicates the cluster assignment. Some algorithms also explicitly identify **noise or outliers**, often shown in black.

**Analyst mindset:** Unlike supervised learning, clustering is not evaluated by accuracy against a known answer. Instead, the goal is to select an algorithm whose assumptions about distance, shape, density, and hierarchy align with the structure of the data and the decision context.

![A comparison grid showing multiple clustering algorithms applied to the same example datasets. Columns represent algorithms such as MiniBatch K-Means, Affinity Propagation, MeanShift, Spectral Clustering, Ward, Agglomerative Clustering, DBSCAN, OPTICS, BIRCH, and Gaussian Mixture. Rows represent different data shapes such as rings, spirals, blobs, and anisotropic clusters. Colors indicate cluster assignments, and some methods label noise or outliers.](../Images/Chapter20_images/sklearn_clustering.png)

Several important insights are visible in this comparison. Some algorithms tend to **force every observation into a cluster**, even when the data has weak or ambiguous structure. Other algorithms are capable of identifying **non-convex shapes**, such as rings or spirals, while centroid-based methods often struggle in these settings. Density-based methods can explicitly model **noise**, and hierarchical approaches reveal **multi-scale structure** that can be explored by cutting the hierarchy at different levels.

A useful way to apply this summary is to ask not only which algorithm might work, but also which assumptions are likely to fail. For example, centroid-based methods struggle with rings, density-based methods struggle when densities vary, and hierarchical methods can become expensive for large datasets.

Concept check: if your data contains irregular cluster shapes and also includes meaningful noise points that should remain unclustered, which clustering family would you try first, and why?

This visualization is provided by the scikit-learn documentation: plot_cluster_comparison.

---

## 20.7 Practice

Consider working through these practice problems:

### 20.7 Clustering: Concepts Quiz

---

## 20.8 Modeling Midterm

Complete the modeling midterm below:

---
