# Chapter 13: Classification Modeling

## 13.1 Introduction

Modeling categorical data differs from modeling numeric data because categorical values do not have an inherent numeric ordering. This does not mean that categories never have a meaningful sequence in the real world, but rather that classification models deliberately ignore any assumed order. Instead of fitting a regression line to ordered values, classification modeling estimates the likelihood that each observation belongs to each possible category. We refer to this process as **classification modeling** — Assigning items in a collection to predefined target categories or classes with the goal of accurately predicting the class membership for each case in the data..

![A cloud of customer records with known outcomes (such as churned and retained customers). An arrow points from the cloud to a box labeled Classification Modeling, which then points to two groups representing predicted classes: likely to churn and likely to stay.](../Images/Chapter13_images/Algorithms - Classification.png)

For example, classifying customer behavior, product categories, or loan approval outcomes involves learning how strongly different attributes contribute to category membership. One category is not greater than or less than another, so numeric regression techniques that rely on ordered values are inappropriate. Instead, classification models evaluate how combinations of attributes influence the probability that an observation belongs to categories such as “high risk” or “low risk,” “churn” or “retain,” or “approve” or “deny.”

Classification models can technically be applied to numeric data, but doing so ignores any natural ordering in the values. For instance, predicting age using classification treats 18 and 19 as two unrelated categories rather than recognizing that 19 represents a larger quantity than 18. As a result, while classification models _can_ be used to predict numeric outcomes, they are often a poor choice when the numeric ordering carries important meaning.

Many algorithms support classification modeling, including logistic regression, decision trees, neural networks, and support vector machines. In this chapter, we focus on two foundational and widely used approaches: _logistic regression_ and _classification decision trees_. Logistic regression extends linear modeling to probabilistic class prediction, while decision trees provide a flexible, non-linear alternative that does not rely on distributional assumptions such as normality. Together, these models form the conceptual and practical foundation for more advanced techniques introduced later, including ensemble methods such as random forests and gradient boosting.

---

## 13.2 Problem Setup

In a classification problem, the label is categorical rather than numeric. Instead of predicting a single numeric value (such as price), we predict which category a case belongs to (such as whether a loan ends in a good outcome or a default outcome).

Throughout this chapter, we will use a LendingClub-style dataset to predict _loan_status_. Because _loan_status_ contains multiple categories, we will first recode it into a two-class label for binary classification.

#### Dataset overview

The dataset contains _35_ columns. The table below serves as a data dictionary for every column in the file.

#### Step 1: Recode the label into a two-class outcome

To make the chapter’s modeling tasks clear and consistent, we will convert _loan_status_ into a binary label. We will treat _Default_ and _Charged Off_ as the negative outcome, and we will treat all other statuses as the positive outcome.

To match the direction you requested, we will code the “good” outcome as _1_ and the “bad” outcome (default or charged off) as _0_. This makes it natural to interpret predicted probabilities as “probability of a good outcome.”

#### Step 2: Define the feature matrix X and label vector y

When building _X_, we must remove both _loan_status_ and _loan_status_numeric_. These columns contain the label (or a direct encoding of the label), and including them as predictors would cause target leakage and invalidate evaluation results.

#### Step 3: Create train/validation/test splits (reused in later sections)

In this chapter, we will create our train/validation/test splits once and then reuse them throughout the remaining sections. We will also use _stratify_ so that the class balance is similar in each split.

```python
import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("lc_small.csv")

bad_statuses = {"Charged Off", "Default"}

df["loan_good"] = (~df["loan_status"].isin(bad_statuses)).astype(int)

y = df["loan_good"].copy()
X = df.drop(columns=["loan_status", "loan_status_numeric", "loan_good"]).copy()

X_train_full, X_test, y_train_full, y_test = train_test_split(
  X, y, test_size=0.20, random_state=42, stratify=y
)

X_train, X_val, y_train, y_val = train_test_split(
  X_train_full, y_train_full, test_size=0.25, random_state=42, stratify=y_train_full
)

print("Train:", X_train.shape, " Val:", X_val.shape, " Test:", X_test.shape)
print("Positive class rate (loan_good=1):")
print("Train:", y_train.mean(), " Val:", y_val.mean(), " Test:", y_test.mean())

# Output:
# Train: (6285, 33)  Val: (2095, 33)  Test: (2096, 33)
# Positive class rate (loan_good=1):
# Train: 0.9143993635640414  Val: 0.9140811455847255  Test: 0.9141221374045801
```

From this point forward, the rest of the chapter will treat _X_train_, _X_val_, _X_test_, _y_train_, _y_val_, and _y_test_ as “already created.” This keeps later sections shorter and emphasizes that good evaluation depends on a consistent split strategy.

#### Why stratified splitting matters

We use _stratified_ splitting to preserve the proportion of default and non-default loans in the training, validation, and test sets.

This is essential in credit-risk modeling because defaults are typically rare. Without stratification, one split might contain far fewer default cases than another, leading to unstable model training and misleading performance metrics.

In the next section, we will introduce classification performance metrics and use these same data objects to evaluate our first models.

---

## 13.3 Logistic Regression

The first core algorithm we study for classification is _logistic regression_. Despite its name, logistic regression is not a regression model in the usual sense—it is a probabilistic classification model designed specifically for predicting categorical outcomes.

Logistic regression is widely used in credit risk modeling, fraud detection, medical diagnosis, marketing response prediction, and many other business and scientific applications because it is fast, stable, and highly interpretable.

In this section, we use the same training, validation, and test splits created in Section 13.2 to build and evaluate a logistic regression classifier for predicting whether a loan ends in a good outcome (_loan_good = 1_) or a bad outcome (_loan_good = 0_).

#### Why Linear Regression Fails for Classification

It may be tempting to apply linear regression directly to classification problems by coding classes as 0 and 1. While this sometimes produces usable predictions, the approach is fundamentally flawed.

_Unbounded predictions:_ Linear regression can produce values below 0 or above 1, which cannot be interpreted as probabilities.

_Non-probabilistic output:_ Linear regression does not model class probabilities in a principled way, making it difficult to interpret prediction confidence.

_Poor decision boundaries:_ Linear regression minimizes squared error rather than classification error, often resulting in suboptimal class separation.

Classification models are explicitly designed to avoid these issues by modeling probabilities directly and constraining predictions to lie between 0 and 1.

#### The Logistic Function and Odds

Logistic regression solves these problems by passing a linear combination of the input features through the _logistic (sigmoid) function_.

The sigmoid function has an S-shaped curve that maps any real number into the interval (0, 1), making it ideal for modeling probabilities.

Conceptually, logistic regression models the _log-odds_ of the positive class as a linear function of the predictors, and then converts those log-odds into a probability.

A predicted probability of 0.80 means the model believes there is an 80% chance that the loan ends in a good outcome, given the observed features.

This probabilistic interpretation is one of the main reasons logistic regression is so widely used in business decision systems.

#### Logistic Regression in scikit-learn

We now train a logistic regression classifier using scikit-learn. As with previous chapters, we use a pipeline to combine preprocessing and modeling into a single reusable object.

This model will be reused later for evaluation and comparison with decision trees.

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

num_cols = X_train.select_dtypes(include=["int64", "float64"]).columns.tolist()
cat_cols = X_train.select_dtypes(include=["object"]).columns.tolist()

numeric_pipe = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="median")),
  ("scaler", StandardScaler())
])

categorical_pipe = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
  transformers=[
    ("num", numeric_pipe, num_cols),
    ("cat", categorical_pipe, cat_cols)
  ]
)

logit_model = Pipeline(steps=[
  ("prep", preprocessor),
  ("clf", LogisticRegression(max_iter=1000))
])

logit_model.fit(X_train, y_train)
```

Logistic regression provides two types of predictions: class labels and class probabilities.

_predict()_ returns the predicted class (0 or 1).

_predict_proba()_ returns the estimated probability for each class.

```python
y_val_pred = logit_model.predict(X_val)
y_val_proba = logit_model.predict_proba(X_val)

print("First 5 predicted classes:", y_val_pred[:5])
print("First 5 predicted probabilities (P(class=1)):", y_val_proba[:5, 1])

# Output:
# First 5 predicted classes: [1 1 1 1 1]
# First 5 predicted probabilities (P(class=1)): [0.99965759 0.99650636 0.99905931 0.99639334 0.99999159]
```

#### Interpreting Logistic Regression Coefficients

Each coefficient in a logistic regression model represents the change in the _log-odds_ of the positive class for a one-unit increase in the corresponding feature.

Exponentiating a coefficient converts it into an _odds ratio_, which is often easier to interpret.

An odds ratio greater than 1 indicates higher odds of a good outcome, while a value less than 1 indicates lower odds.

In predictive modeling, coefficients are interpreted as _associations useful for prediction_, not as causal effects.

This differs from causal regression, where coefficient interpretation requires strong assumptions about confounding, functional form, and omitted variables.

In classification modeling, the primary purpose of coefficients is to understand model behavior and feature influence, not to establish cause-and-effect relationships.

#### Strengths and Weaknesses of Logistic Regression

- Strengths
- Weaknesses

Despite these limitations, logistic regression remains one of the most important and widely deployed classification algorithms in practice. In the next section, we will study classification decision trees and compare their behavior to logistic regression.

#### Regression assumptions: linear vs logistic

Because logistic regression is derived from linear regression, it inherits some assumptions but modifies or removes others. Understanding which assumptions still apply is important for knowing when logistic regression is appropriate and how to diagnose problems in practice.

The most important conceptual difference is that logistic regression replaces the assumption of normally distributed errors with a probabilistic model for binary outcomes. This makes it well suited for classification while preserving many of the interpretability benefits of linear regression.

In practice, this means that many of the same diagnostic habits you developed for linear regression—checking for multicollinearity, inspecting influential predictors, and validating functional form—remain useful for logistic regression, even though the error structure and interpretation of coefficients change.

---

## 13.4 Decision Tree Classification

In the prior chapter, you learned how regression trees predict a numeric outcome by splitting the feature space into regions and predicting the average label value inside each leaf.

In this section, we apply the same core idea to a new goal: predicting a _class label_ (such as whether a loan will end in a negative outcome). A _classification tree_ learns a sequence of splits that separates classes as cleanly as possible.

Because decision trees use simple “if–then” rules, they are intuitive and easy to visualize. They also require fewer modeling assumptions than many other algorithms. However, like regression trees, they can overfit easily if we let them grow without constraints.

#### How classification trees differ from regression trees

Regression trees and classification trees are built using the same greedy splitting strategy: at each node, the algorithm searches for the split that most improves a chosen objective function. The key difference is the objective function itself.

A regression tree chooses splits that reduce numeric error (often measured with _mean squared error_). A classification tree chooses splits that reduce _class impurity_, which measures how mixed the classes are inside a node.

In a classification node, the data consists of a mix of class labels. A node is considered “pure” if almost all observations belong to the same class, and “impure” if the classes are heavily mixed.

Two common impurity measures are _Gini impurity_ and _entropy_. Both reach their minimum when a node is perfectly pure and their maximum when classes are evenly mixed.

In scikit-learn, you choose this criterion using the _criterion_ hyperparameter. For binary classification, the most common options are _gini_ (default) and _log_loss_ (or _entropy_, depending on the version).

In a regression tree, a leaf contains a numeric prediction such as the average sale price in that region. In a classification tree, a leaf contains a _class distribution_.

For example, suppose a leaf contains 100 loans, and 25 of them ended in a negative outcome. Then the leaf’s estimated probability for the negative class is 0.25, and the probability for the positive class is 0.75.

When you call _predict_, the tree assigns each observation to its leaf and returns the most common class in that leaf (majority vote). When you call _predict_proba_, the tree returns the class probabilities implied by leaf proportions.

Conceptually, classification trees work the same way as regression trees: repeated splits build a hierarchy of decision rules. The main change is that the tree is now optimizing purity (Gini/entropy/log loss) instead of numeric error.

Next, we will train a _DecisionTreeClassifier_ on our loan dataset using a predictive workflow and examine how hyperparameters like _max_depth_ and _min_samples_leaf_ control model complexity.

#### Training a Classification Tree in Python

We now train a classification tree using the same loan dataset introduced earlier in this chapter. The workflow mirrors what you have already seen for regression trees: train/test split, preprocessing pipeline, model fitting, and evaluation.

In scikit-learn, classification trees are implemented with the _DecisionTreeClassifier_ class.

- _max_depth_: Maximum number of splits from root to leaf. Controls overall complexity.
- _min_samples_leaf_: Minimum number of observations allowed in a leaf node.
- _criterion_: Impurity measure used to choose splits (_gini_, _entropy_, or _log_loss_).

As before, we begin with a simple model using mostly default settings and then study how performance changes as we adjust complexity.

```python
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, log_loss

tree_clf = DecisionTreeClassifier(
  max_depth=None,
  min_samples_leaf=1,
  criterion="gini",
  random_state=27
)

model = Pipeline(steps=[
  ("prep", preprocessor),
  ("tree", tree_clf)
])

model.fit(X_train, y_train)

y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, y_pred)
ll = log_loss(y_test, y_prob)

print("Test accuracy:", round(acc, 4))
print("Test log loss:", round(ll, 4))

# Output:
# Test accuracy: 0.9165
# Test log loss: 3.0094
```

Accuracy measures the fraction of correct class predictions. Log loss evaluates the quality of predicted probabilities and penalizes confident mistakes more strongly.

These two metrics often move in different directions as model complexity changes, which becomes important when diagnosing overfitting. We will explain them in more detail later.

Next, we will systematically vary the tree depth to visualize how classification trees overfit and how validation performance reveals the optimal level of complexity.

#### Overfitting in Classification Trees

Classification trees are highly flexible models. This flexibility allows them to capture complex patterns, but it also makes them especially prone to _overfitting_.

An overfit classification tree memorizes the training data, achieving very high training accuracy while performing substantially worse on new, unseen observations.

Each additional level of depth allows the tree to create more specialized decision rules. Eventually, the model begins splitting on noise rather than signal.

This produces leaves that contain only a few observations and predictions that are unstable across samples.

The figure below illustrates this progression using a simple two-class example. On the left, the decision boundary is nearly flat—too simple to separate the classes effectively. This is **underfitting**: the model ignores meaningful structure in the data. In the center, the boundary captures the general pattern without chasing every individual point. This is the **optimal fit**: a few training observations are misclassified, but the model will generalize well to new data. On the right, the boundary twists around every point, achieving zero training errors. This is **overfitting**: the model has memorized the training data, including its noise, and will perform poorly on observations it has not seen before.

![Three scatter plots comparing classification decision boundaries. The left plot shows underfitting with a nearly horizontal line that misclassifies 6 points. The center plot shows an optimal fit with a diagonal boundary that misclassifies 4 points but generalizes well. The right plot shows overfitting with a highly curved boundary that misclassifies 0 training points but will not generalize to new data.](../Images/Chapter13_images/classifiction_fitting.png)

As you work through the depth-sweep experiment below, keep this visual in mind. Increasing tree depth moves the model from left to right in the figure: shallow trees underfit, moderate-depth trees approximate the optimal boundary, and excessively deep trees overfit.

To diagnose overfitting, we train multiple trees with increasing values of _max_depth_ and compare performance on both the training and validation sets.

We track two metrics:

- _Accuracy_, which measures classification correctness.
- _Log loss_, which measures probability quality and penalizes confident errors.

```python
from sklearn.metrics import accuracy_score, log_loss
import pandas as pd

results = []

for depth in [1, 2, 3, 4, 6, 8, 10, 12, 15]:
  tree_clf = DecisionTreeClassifier(
    max_depth=depth,
    min_samples_leaf=1,
    criterion="gini",
    random_state=27
  )

  model = Pipeline(steps=[
    ("prep", preprocessor),
    ("tree", tree_clf)
  ])

  model.fit(X_train, y_train)

  y_train_pred = model.predict(X_train)
  y_train_prob = model.predict_proba(X_train)[:, 1]

  y_val_pred = model.predict(X_test)
  y_val_prob = model.predict_proba(X_test)[:, 1]

  results.append({
    "max_depth": depth,
    "train_accuracy": accuracy_score(y_train, y_train_pred),
    "val_accuracy": accuracy_score(y_test, y_val_pred),
    "train_log_loss": log_loss(y_train, y_train_prob),
    "val_log_loss": log_loss(y_test, y_val_prob)
  })

depth_results = pd.DataFrame(results)
depth_results
```

![A DataFrame summarizing the regularization results of decision tree classification models of various max_depth values on accuracy and log_loss for training vs validation data](../Images/Chapter13_images/dtc_regularization.png)

The resulting table summarizes how classification performance evolves as the tree becomes deeper. But let's make this a bit easier to process by visualizing it below:

```python
import matplotlib.pyplot as plt

best_ll_depth = int(depth_results.loc[depth_results["val_log_loss"].idxmin(), "max_depth"])
best_acc_depth = int(depth_results.loc[depth_results["val_accuracy"].idxmax(), "max_depth"])

left = min(best_ll_depth, best_acc_depth)
right = max(best_ll_depth, best_acc_depth)

x = depth_results["max_depth"].to_numpy()

plt.figure(figsize=(10.5, 4.8))
plt.plot(x, depth_results["train_log_loss"], marker="o", linewidth=1.5, label="Train log loss")
plt.plot(x, depth_results["val_log_loss"], marker="o", linewidth=1.5, label="Validation log loss")

plt.axvline(best_ll_depth, linewidth=1.5, label=f"Best val log loss (depth={best_ll_depth})")
plt.axvline(best_acc_depth, linewidth=1.5, linestyle="--", label=f"Best val accuracy (depth={best_acc_depth})")
plt.axvspan(left, right, alpha=0.18, label="Selection region")

plt.xlabel("max_depth")
plt.ylabel("Log loss")
plt.title("Log loss vs tree depth (train vs validation)")
plt.legend(frameon=False)
plt.grid(True, linewidth=0.3)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10.5, 4.8))
plt.plot(x, depth_results["train_accuracy"], marker="o", linewidth=1.5, label="Train accuracy")
plt.plot(x, depth_results["val_accuracy"], marker="o", linewidth=1.5, label="Validation accuracy")

plt.axvline(best_ll_depth, linewidth=1.5, label=f"Best val log loss (depth={best_ll_depth})")
plt.axvline(best_acc_depth, linewidth=1.5, linestyle="--", label=f"Best val accuracy (depth={best_acc_depth})")
plt.axvspan(left, right, alpha=0.18, label="Selection region")

plt.xlabel("max_depth")
plt.ylabel("Accuracy")
plt.title("Accuracy vs tree depth (train vs validation)")
plt.legend(frameon=False)
plt.grid(True, linewidth=0.3)
plt.tight_layout()
plt.show()
```

![Two line charts showing model overfitting checks for a classification tree. The top chart plots train and validation log loss versus max_depth; the bottom chart plots train and validation accuracy versus max_depth. A solid vertical line marks the depth with the lowest validation log loss, and a dashed vertical line marks the depth with the highest validation accuracy. A lightly shaded vertical band spans between these two depths, indicating a model-selection region where either depth is reasonable depending on whether probability quality (log loss) or hard-label accuracy is prioritized.](../Images/Chapter13_images/tree_depth_marked.png)

To choose a reasonable value for _max_depth_, we evaluate candidate depths using _validation-set_ metrics rather than training-set metrics. The training curves almost always improve as depth increases, because deeper trees can memorize the training data.

In the plots, the solid vertical line marks the depth with the lowest _validation log loss_. Log loss uses the full predicted probabilities and penalizes overconfident mistakes, so it is usually the most reliable criterion when you care about probability quality (for example, risk scoring and decision-making under uncertainty).

The dashed vertical line marks the depth with the highest _validation accuracy_. Accuracy evaluates only the final class label after applying a probability threshold (typically 0.50), so it can be easier to explain but it ignores confidence and can be sensitive to class imbalance.

The shaded region spans the depths between the best validation log loss and the best validation accuracy. Any depth inside this interval is a defensible choice, and selecting within it becomes a modeling judgment: choose closer to the log-loss optimum when calibrated probabilities matter, or closer to the accuracy optimum when the primary goal is correct hard-label decisions.

A practical rule is to pick the _simplest_ depth inside the shaded region, because simpler trees are usually more stable and easier to explain while still achieving near-optimal validation performance.

If your application uses predicted probabilities to make decisions (for example, pricing, risk scores, or ranking), prioritize _validation log loss_ and select a depth near the solid line. If your application only needs a yes/no decision and the threshold is fixed, accuracy may be a meaningful secondary consideration. When the two best depths differ, treat the shaded region as the set of “reasonable” options, then choose the simplest tree in that region unless you have a clear business reason to do otherwise.

The validation curves above illustrate a classic concept you first encountered in linear regression: the _bias–variance tradeoff_. Tree depth directly controls where a model falls on this spectrum.

The shaded selection region in the validation plots corresponds to the middle row of this table: depths where the model is complex enough to capture structure but not so complex that it memorizes noise.

In practice, modelers identify this region using validation curves or cross-validation, then choose the simplest model within it that satisfies performance requirements.

In the next section, we will visualize the learned tree structure to better understand how classification decisions are formed inside the model.

---

## 13.5 Visualizing Classification Trees

Decision trees are often described as “interpretable” models because their structure can be visualized directly. In this section, we examine how to plot a trained classification tree and how to interpret the information shown inside each node.

Unlike linear and logistic regression models, where predictions are produced through equations, a tree model makes decisions by following a sequence of splits from the root to a leaf. Visualizing this process helps clarify how the model converts input features into class predictions.

#### Plotting a classification tree in Python

We will visualize the trained classification tree from the previous section using _sklearn.tree.plot_tree_. Because large trees quickly become unreadable, we typically visualize only shallow trees or pruned versions of deeper models.

```python
from sklearn.tree import plot_tree

tree_model = model.named_steps["tree"]

plt.figure(figsize=(20, 10), dpi=350)

plot_tree(
  tree_model,
  feature_names=preprocessor.get_feature_names_out(),
  class_names=["good", "default"],
  filled=True,
  rounded=True,
  max_depth=3,
  fontsize=9,
  precision=2,
  proportion=True,
  impurity=False
)

plt.title("Classification tree (first 3 levels)", fontsize=14)
plt.tight_layout()
plt.show()
```

![A visualization of the decision tree fitted classification model that goes 3 levels deep.](../Images/Chapter13_images/dt_classification_viz.png)

The visualization is controlled by several important parameters in _plot_tree_. _feature_names_ supplies readable variable names for each split, and _class_names_ labels the predicted categories shown in leaf nodes. Setting _filled=True_ colors nodes by the predicted class and class purity, while _rounded=True_ improves visual clarity by softening box corners. The _max_depth_ parameter limits how many levels of the tree are drawn, which is useful for focusing on high-level structure without overwhelming detail. _fontsize_ controls text size, _precision_ limits the number of decimal places displayed in probabilities, and _proportion=True_ shows class probabilities instead of raw sample counts. Setting _impurity=False_ removes Gini or entropy values to reduce clutter. Additional optional parameters include _label_ (to control how much text is shown in nodes), _node_ids_ (to display internal node numbers for debugging or pruning), and _rotate_ (to draw the tree horizontally for very wide models).

#### What each node represents

Each rectangle in the tree visualization is called a _node_. Internal nodes represent decision points, while leaf nodes represent final predictions.

- _Predicted class_: The class label shown at the bottom of the node is the majority class among training samples that reached that node.
- _Class probabilities_: The value vector shows the proportion of samples from each class at that node (for example, 0.85 good and 0.15 default).
- _Impurity_: The Gini or entropy value measures how mixed the classes are at that node. Lower values indicate purer nodes.
- _Sample count_: The number of training observations that reached the node.
- _Split rule_: For internal nodes, the condition (for example, income ≤ 42,000) determines which branch each observation follows.

#### How predictions are formed

To classify a new observation, the tree starts at the root node and evaluates the split rule. The observation follows the corresponding branch until it reaches a leaf node, where the predicted class is produced.

Although the final prediction is a single class label, the model internally computes class probabilities based on the proportion of training samples in the leaf.

#### Interpretation limits of tree visualizations

Tree visualizations become difficult to interpret as depth increases. Even moderately sized trees can contain dozens or hundreds of nodes, making global reasoning about the model impractical.

In addition, splits are chosen greedily. A feature that appears near the top of the tree is not necessarily globally more important than all features below it; it simply provided the best local impurity reduction at that step.

#### Why shallow trees are often preferable

Shallow trees are often preferred when interpretability is important. A tree with depth 3 or 4 can usually be inspected and explained by a human, while deeper trees behave more like opaque predictive machines.

From a predictive perspective, shallow trees also tend to generalize better because they avoid memorizing idiosyncratic patterns in the training data.

For these reasons, tree depth is commonly restricted using hyperparameters such as _max_depth_ and _min_samples_leaf_, or by pruning after training.

In practice, tree visualizations are best used as diagnostic and explanatory tools rather than complete descriptions of a model’s behavior.

Next, we extend classification trees to problems involving more than two outcome categories.

#### Saving high-resolution tree diagrams for large models

When trees become larger, the default screen resolution is often insufficient to read split rules, class probabilities, and impurity values. In these cases, it is best to save the tree visualization as a high-resolution image file using the _dpi_ parameter in Matplotlib.

Below, we first export the selected model from the previous section (max depth = 4), and then export a deeper diagnostic tree (max depth = 8) using a much higher DPI suitable for zooming or printing.

```python
tree_model = model.named_steps["tree"]

plt.figure(figsize=(20, 10), dpi=250)

plot_tree(
  tree_model,
  feature_names=preprocessor.get_feature_names_out(),
  class_names=["good", "default"],
  filled=True,
  rounded=True,
  fontsize=9,
  precision=2,
  proportion=True,
  impurity=False
)

plt.title("Classification tree (max_depth = 4)", fontsize=14)
plt.tight_layout()
plt.savefig("classification_tree_depth4.png", bbox_inches="tight")
plt.close()
```

The file _classification_tree_depth4.png_ preserves the full structure of the selected model while remaining readable at standard zoom levels.

```python
deep_tree = DecisionTreeClassifier(
  max_depth=8,
  min_samples_leaf=1,
  criterion="gini",
  random_state=27
)

deep_model = Pipeline(steps=[
  ("prep", preprocessor),
  ("tree", deep_tree)
])

deep_model.fit(X_train, y_train)

plt.figure(figsize=(28, 16), dpi=450)

plot_tree(
  deep_model.named_steps["tree"],
  feature_names=preprocessor.get_feature_names_out(),
  class_names=["good", "default"],
  filled=True,
  rounded=True,
  fontsize=8,
  precision=2,
  proportion=True,
  impurity=False
)

plt.title("Classification tree (max_depth = 8)", fontsize=16)
plt.tight_layout()
plt.savefig("classification_tree_depth8_highdpi.png", bbox_inches="tight")
plt.close()
```

The file _classification_tree_depth8_highdpi.png_ is saved at very high resolution so that individual nodes remain legible even when the tree contains many dozens of splits.

For trees deeper than 5–6 levels, visualization is primarily useful for debugging and teaching rather than interpretation. In applied modeling, feature importance measures and validation metrics usually provide more reliable insight than attempting to reason about hundreds of individual decision paths.

---

## 13.6 Evaluation Metrics for Classification

After training a classification model, the next question is not “How accurate is it?” but “How costly are its mistakes?” In a business setting, classification errors often have unequal consequences, so selecting evaluation metrics is fundamentally a decision about tradeoffs.

In the Lending Club example, our goal is to predict whether a loan ends in _default_ (the bad outcome) versus _good standing_ (all other outcomes). This section shows how to evaluate a model using both (1) _threshold-based_ metrics (based on predicted classes) and (2) _probability-based_ metrics (based on predicted probabilities).

**Metric choice is a cost choice:** Use _accuracy_ when false positives and false negatives have similar costs, _precision_ when false positives are costly (flagging too many good loans as risky), _recall_ when false negatives are costly (missing risky loans), and _F1_ when you want a balance of precision and recall and true negatives add little value.

In the code examples that follow, we will reuse the Lending Club train/test objects and preprocessing pipeline created most recently in the prior section, and we will evaluate a _DecisionTreeClassifier_ with _max_depth = 8_, which also happens to be in the range of valid potential options.

We begin with the confusion matrix, because it makes every other metric easier to understand.

### Baseline Accuracy and Class Imbalance

Before interpreting any accuracy value, always compute the _baseline accuracy_ you would get from a “no-skill” model that predicts only the most common class. In a two-class problem, the baseline is simply the larger class proportion: if 75% of loans are _good_ and 25% are _default_, then a useless classifier that predicts _good_ for every case achieves 75% accuracy without learning anything.

This is why accuracy can be misleading in imbalanced datasets: a model can appear “high accuracy” while still failing to detect the minority class that matters most (such as defaults). A quick case example: imagine a lender screens 10,000 loans, and only 500 (5%) end in default. A model that predicts _no default_ for everyone achieves 95% accuracy, but its recall for defaults is 0.00 because it never catches any risky borrowers.

Whenever your model’s accuracy is near the baseline accuracy, it is not providing meaningful predictive value. Your goal is not merely to exceed baseline accuracy, but to improve the metrics that reflect your business costs (often recall, precision, and probability quality). The baseline concept also connects directly to the confusion matrix: a majority-class classifier produces a confusion matrix with a large true-negative count, but _zero true positives_ for the minority class.

For that reason, always evaluate imbalanced classification models with metrics that focus on the positive class (precision, recall, F1) and with probability-based metrics (log loss), not accuracy alone. In multiclass problems, the same idea applies: the baseline accuracy is the proportion of the most frequent class. If one class dominates, a model can earn deceptively high accuracy by predicting only that class.

Next, we return to the confusion matrix because it makes the baseline and all other metrics easy to interpret.

### Confusion Matrix

The **confusion matrix** — A table that compares predicted classes to actual classes to summarize all correct and incorrect classification outcomes. is the foundation for nearly all classification metrics. For a two-class problem, it contains four values: true negatives (TN), false positives (FP), false negatives (FN), and true positives (TP).

Because we defined _default_ as the positive class, the confusion matrix answers four business-relevant questions: How many safe loans did we correctly approve? How many safe loans did we incorrectly flag as risky? How many risky loans did we miss? And how many risky loans did we correctly detect?

The code below uses the trained decision tree classifier (_max_depth = 3_) and the Lending Club test set created earlier in the chapter.

```python
from sklearn import metrics
import matplotlib.pyplot as plt

# Class predictions on test set
y_test_pred = deep_model.predict(X_test)

# Confusion matrix (explicit label order)
cm = metrics.confusion_matrix(
  y_test,
  y_test_pred,
  labels=[0, 1] # Use numeric labels as they are in y_test
)

cm_display = metrics.ConfusionMatrixDisplay(cm, display_labels=["bad", "good"])

cm_display.plot(values_format="d", cmap="Blues")
plt.title("Confusion Matrix – Lending Club (Decision Tree, depth = 8)")
plt.tight_layout()
plt.show()
```

Cells on the diagonal represent correct predictions (true negatives and true positives). Off-diagonal cells represent errors: false positives (predicting _default_ for a good loan) and false negatives (predicting _good_ for a loan that actually defaults).

Every metric introduced next is computed directly from these four numbers.

### Accuracy

**Accuracy** — The proportion of all predictions that are correct: (TP + TN) / (TP + TN + FP + FN). is the simplest summary metric derived from the confusion matrix.

Accuracy answers the question: _“What fraction of loans did the model classify correctly overall?”_

In scikit-learn, accuracy can be computed either with the _accuracy_score_ function or with the model’s built-in _.score()_ method. Both produce the same value when applied to classification models.

```python
from sklearn.metrics import accuracy_score

acc1 = accuracy_score(y_test, y_test_pred)
acc2 = model.score(X_test, y_test)

print("Accuracy (accuracy_score):", round(acc1, 4))
print("Accuracy (model.score):   ", round(acc2, 4))

# Output
# Accuracy (accuracy_score): 0.9256
# Accuracy (model.score):    0.9222
```

Because accuracy treats all errors equally, it is most appropriate when false positives and false negatives have similar business costs and when class sizes are reasonably balanced.

In credit risk modeling, this assumption is rarely true: approving a loan that later defaults (false negative) is usually far more costly than rejecting a good borrower (false positive). For this reason, accuracy alone is not sufficient for evaluating lending models.

The next metrics—precision, recall, and F1—separate different types of mistakes and allow us to measure these risks explicitly.

### Precision and Recall

**Precision** — The proportion of predicted positive cases that are truly positive: TP / (TP + FP). answers the question: _“When the model predicts default, how often is it correct?”_

**Recall** — The proportion of actual positive cases that are correctly identified: TP / (TP + FN). answers the question: _“Of all loans that truly default, how many did the model catch?”_

Precision focuses on avoiding false alarms (false positives), while recall focuses on avoiding missed detections (false negatives). These two goals often conflict.

In the Lending Club context, we defined the positive class as _default = 1_. This means:

- **False positive:** A good borrower incorrectly predicted to default.
- **False negative:** A borrower predicted to be safe who later defaults.

In most lending applications, false negatives are more expensive because they lead directly to financial losses. This makes recall especially important.

```python
from sklearn.metrics import precision_score, recall_score

precision = precision_score(y_test, y_test_pred)
recall = recall_score(y_test, y_test_pred)

print("Precision:", round(precision, 4))
print("Recall:   ", round(recall, 4))

# Output:
# Precision: 0.9305
# Recall:    0.9927
```

A model can achieve high precision by being conservative and predicting default only in extreme cases, but this often lowers recall because many true defaults are missed.

Conversely, a model can achieve high recall by aggressively labeling loans as risky, but this usually lowers precision by rejecting many safe borrowers.

This tension motivates the F1-score, which combines precision and recall into a single metric.

### F1 Score and the Classification Report

**F1-score** — The harmonic mean of precision and recall: 2 × (precision × recall) / (precision + recall). provides a single number that balances false positives and false negatives.

The harmonic mean penalizes extreme values. If either precision or recall is low, the F1-score will also be low. This makes F1 useful when both types of errors matter.

In credit risk modeling, F1 is often preferred over accuracy because defaults are relatively rare and false negatives are costly.

```python
from sklearn.metrics import f1_score

f1 = f1_score(y_test, y_test_pred)
print("F1 score:", round(f1, 4))

# Output:
# F1 score: 0.9606
```

Rather than computing each metric separately, scikit-learn can generate a full summary table called the _classification report_.

The classification report shows precision, recall, F1-score, and support (number of observations) for each class.

```python
from sklearn.metrics import classification_report
import pandas as pd

report = classification_report(y_test, y_test_pred, output_dict=True)
pd.DataFrame(report).transpose()
```

Key columns in the classification report:

- **precision** – reliability of positive predictions
- **recall** – ability to capture actual positives
- **f1-score** – balance between precision and recall
- **support** – number of true samples in each class

For imbalanced datasets like loan defaults, macro averages treat each class equally, while weighted averages account for class frequency.

Always examine per-class metrics before trusting a single summary number. A high overall score can still hide poor performance on the default class.

### Log Loss and Probability Quality

**log loss** — A probabilistic classification metric that penalizes confident wrong predictions more than uncertain predictions; lower values indicate better probability estimates. evaluates how good your _predicted probabilities_ are, not just whether your predicted class label is correct.

This matters because many business decisions depend on probability confidence. For example, a loan team may treat a predicted default probability of 0.55 very differently than 0.95, even though both would be classified as “default” at a 0.50 threshold.

Log loss is especially useful when you care about calibrated risk scores. It heavily penalizes predictions like “99% chance of default” when the loan actually does not default.

In this chapter’s workflow, we chose the classification tree depth using _validation log loss_. That means our selected model is optimized for probability quality rather than raw accuracy at a single threshold.

```python
from sklearn.metrics import log_loss

# Probability of the positive class (default = 1)
y_test_prob = model.predict_proba(X_test)[:, 1]

ll = log_loss(y_test, y_test_prob)
print("Test log loss:", round(ll, 4))

# Output:
# Test log loss: 1.1187
```

Interpretation guide:

- **Lower is better.** Unlike accuracy, higher values are worse.
- Log loss is minimized when predicted probabilities match the true outcomes.
- A few extremely confident wrong predictions can raise log loss sharply.

When two models have similar accuracy, the model with better log loss is usually the better choice if your downstream decision process uses probability scores (risk tiers, pricing, prioritization, or manual review queues).

Accuracy only checks whether the predicted class label matches the true label at one fixed threshold (often 0.50). Log loss uses the full probability value, so it can prefer a model that makes slightly fewer confident mistakes—even if its 0.50-threshold accuracy is similar.

### ROC Curves and AUC

Another way to evaluate a classifier across _all possible thresholds_ is with the **ROC curve** — A plot showing the tradeoff between true positive rate (recall) and false positive rate as the classification threshold varies..

The ROC curve answers the question: _How well can the model separate good loans from defaulting loans, regardless of where we place the cutoff threshold?_

Instead of using predicted class labels, ROC curves use the model’s predicted probabilities. This makes them complementary to log loss, which also evaluates probability quality.

```python
from sklearn.metrics import roc_curve, roc_auc_score, RocCurveDisplay
import matplotlib.pyplot as plt

y_test_prob = model.predict_proba(X_test)[:, 1]

fpr, tpr, thresholds = roc_curve(y_test, y_test_prob)
auc_value = roc_auc_score(y_test, y_test_prob)

disp = RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=auc_value)
disp.plot()
plt.title("ROC curve – Lending Club default prediction")
plt.show()

print("AUC:", round(auc_value, 4))

# Output:
# AUC: 0.7073
```

Each point on the ROC curve corresponds to a different probability threshold. Moving along the curve trades off:

- **True Positive Rate (TPR / Recall)**: proportion of actual defaults correctly flagged.
- **False Positive Rate (FPR)**: proportion of good loans incorrectly flagged as defaults.

The diagonal line represents random guessing. Curves that bow strongly toward the top-left corner indicate better discrimination ability.

The **AUC** — Area Under the ROC Curve; the probability that the model assigns a higher risk score to a randomly chosen positive case than to a randomly chosen negative case. summarizes the ROC curve into a single number between 0 and 1.

- 0.50 = random guessing
- 0.70–0.80 = acceptable
- 0.80–0.90 = strong
- > 0.90 = excellent discrimination

Unlike accuracy, AUC is insensitive to class imbalance and does not depend on a single threshold choice, making it useful for comparing competing classification models.

When the positive class is rare (such as loan defaults), precision–recall curves are often more informative about operational performance. ROC curves remain valuable for measuring overall ranking quality, but PR curves better reflect the cost of false positives in imbalanced datasets.

### Threshold Tradeoffs and Business Costs

So far, we have evaluated models assuming a fixed classification threshold (usually 0.50). In practice, this threshold is a _business decision_, not a statistical one.

A probability threshold determines when the model predicts _default_ versus _good loan_. Changing this threshold shifts the balance between:

- **False positives**: rejecting good borrowers.
- **False negatives**: approving borrowers who later default.

In lending, false negatives are usually more expensive because defaults cause direct financial loss, while false positives represent missed revenue opportunities. This asymmetry motivates using recall-focused or probability-based metrics.

We can directly visualize how precision and recall change as the threshold moves.

```python
from sklearn.metrics import precision_recall_curve
import matplotlib.pyplot as plt

y_test_prob = model.predict_proba(X_test)[:, 1]

precision, recall, thresholds = precision_recall_curve(y_test, y_test_prob)

plt.figure(figsize=(8, 5))
plt.plot(thresholds, precision[:-1], label="Precision")
plt.plot(thresholds, recall[:-1], label="Recall")
plt.xlabel("Probability threshold")
plt.ylabel("Score")
plt.title("Precision and recall vs. classification threshold")
plt.legend()
plt.grid(alpha=0.3)
plt.show()
```

![A sample precision-recall curve generated from python](../Images/Chapter13_images/precision_recall_curve.png)

This plot shows that:

- Lower thresholds increase recall but reduce precision.
- Higher thresholds increase precision but reduce recall.

There is no universally optimal threshold. The correct choice depends on organizational priorities, regulatory constraints, and financial risk tolerance.

Threshold selection is often formalized using a cost matrix:

Because false negatives are far more costly, lenders often choose thresholds that intentionally sacrifice some precision in exchange for higher recall.

Metrics guide model selection, but threshold choice reflects organizational strategy. Data scientists should present tradeoff curves and cost scenarios to stakeholders rather than selecting thresholds unilaterally.

### Putting the Metrics Together

Each evaluation metric highlights a different aspect of classification performance. No single number fully describes model quality, especially when business costs are asymmetric.

The table below summarizes when each metric is most useful.

For applied classification problems, a practical workflow is:

1. Train candidate models using training data.
1. Select hyperparameters using validation log loss or ROC AUC.
1. Evaluate confusion matrix, precision, recall, and F1 on the test set.
1. Inspect probability calibration using log loss.
1. Choose a classification threshold using business cost tradeoffs.

This separation between model selection, performance evaluation, and threshold choice prevents accidental overfitting and aligns modeling decisions with business objectives.

- Classification quality cannot be captured by accuracy alone.
- Probability-based metrics are essential for risk-sensitive applications.
- Thresholds encode business policy, not mathematical truth.
- Model evaluation must reflect real operational costs.

With evaluation principles established, we now turn to understanding how classification trees represent decisions internally and how to visualize their structure.

---

## 13.7 Multiclass Classification

So far, we have treated classification as a two-class problem. Many real business problems, however, involve more than two meaningful outcomes. This is called **multiclass classification**: predicting which one of three or more categories a case belongs to.

In Lending Club data, the raw **loan_status** variable contains many distinct statuses. Some are common (such as _Fully Paid_), while others are rare (such as “late” variants). When categories are extremely rare, models can struggle to learn stable patterns, and evaluation becomes noisy. A common modeling approach is to _bin rare categories into broader groups_ that still preserve business meaning.

#### Step 1: Examine loan_status frequencies

We begin by counting how frequently each raw status appears. This helps us decide which statuses are common enough to keep separate and which should be grouped.

```python
# Assumes from earlier sections:
# df (DataFrame) that includes loan_status
# We will create a multiclass label based on loan_status

status_counts = df["loan_status"].value_counts(dropna=False)
print(status_counts)

# Output:
# loan_status
# Current               6612
# Fully Paid            2722
# Charged Off            898
# Late (31-120 days)     152
# In Grace Period         56
# Late (16-30 days)       36
# Name: count, dtype: int64
```

In many Lending Club extracts, the “late” statuses appear relatively rarely compared to the dominant categories. If we keep every rare late category as its own class, the model may fit unstable decision rules, and metrics can be misleading because the model has too few examples to learn from.

#### Step 2: Create a 3-class label by binning statuses

To build a practical multiclass example, we will create three classes:

- **good**: combine _Current_ and _Fully Paid_.
- **late**: bin all late-related statuses together (rare categories become one stable class).
- **bad**: combine _Default_ and _Charged Off_.

This preserves the business meaning: loans are either doing fine, showing warning signs, or have clearly failed.

```python
# Create a 3-class label from loan_status
# good = Current or Fully Paid
# bad = Default or Charged Off
# late = all remaining statuses (late-related and rare variants)

def recode_status_multiclass(s):
  s = str(s).strip()
  if s in ["Current", "Fully Paid"]:
    return "good"
  if s in ["Default", "Charged Off"]:
    return "bad"
  return "late"

y3 = df["loan_status"].apply(recode_status_multiclass)

y3.value_counts()

# Output:
# loan_status
# good    9334
# bad      898
# late     244
# Name: count, dtype: int64
```

Always check the new class counts after recoding. If one class is still extremely small, you may need a different binning scheme, a larger dataset, or a different modeling approach.

#### One-vs-rest and native multiclass support

Some algorithms are naturally multiclass. For example, decision trees can directly split data to separate multiple classes in the leaves. Other algorithms, such as logistic regression, are fundamentally binary and must be extended to handle multiple classes.

A common extension method is **one-vs-rest (OvR)**. In OvR, the model trains one binary classifier per class. Each classifier learns to separate its class from “all other classes.” At prediction time, the class with the strongest predicted probability is chosen.

#### Step 3: Multiclass Decision Tree in Python

Decision trees in scikit-learn support multiclass classification directly. The model learns splits that reduce impurity across all classes at once.

```python
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier

# Assumes from earlier sections:
# X (features DataFrame), preprocessor (ColumnTransformer)

X_train3, X_test3, y_train3, y_test3 = train_test_split(
  X, y3, test_size=0.20, random_state=27, stratify=y3
)

tree3 = DecisionTreeClassifier(
  max_depth=3,
  min_samples_leaf=1,
  criterion="gini",
  random_state=27
)

model_tree3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("tree", tree3)
])

model_tree3.fit(X_train3, y_train3)
```

We use _stratify=y3_ to preserve the class proportions in both the training and test sets. Without stratification, rare classes such as _late_ may disappear entirely from one split, making evaluation unreliable.

#### Step 4: Multiclass logistic regression in Python

Scikit-learn logistic regression supports multiclass classification by combining multiple binary models. Under the hood, this is commonly done using a _one-vs-rest_ strategy, where one classifier is trained for each class against all others. For learning purposes, we implement this strategy explicitly using _OneVsRestClassifier_ so the extension from binary classification is transparent.

```python
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier

lr_base = LogisticRegression(
  solver="liblinear",
  max_iter=2000,
  random_state=27
)

lr_ovr = OneVsRestClassifier(lr_base)

model_lr3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("lr", lr_ovr)
])

model_lr3.fit(X_train3, y_train3)
```

#### Step 5: Metrics for multiclass classification

Metrics extend naturally to multiclass settings, but there is an important choice: do we average performance across classes equally, or do we weight classes by how common they are?

**Macro averaging** treats each class equally, which is useful when minority classes matter. **Weighted averaging** weights each class by its frequency, which is useful when overall performance is the priority.

### Baseline Accuracy in Multiclass Problems

In multiclass classification, the same “majority-class baseline” idea still applies: the baseline accuracy is the proportion of the _largest_ class, because a no-skill model can predict that single class for every case.

For example, if the three classes are distributed as 90% _good_, 8% _bad_, and 2% _late_, then predicting _good_ for every loan yields 90% accuracy without learning anything about risk.

This is why multiclass evaluation must go beyond accuracy: a model can post a “high” accuracy while still failing to detect the minority classes that often matter most operationally (such as _late_ warning cases).

In our Lending Club test split, the _good_ class is by far the largest class, so the multiclass baseline accuracy is essentially “always predict good.” The code below calculates that baseline directly from _y_test3_.

```python
# Multiclass baseline accuracy = largest class proportion in the test set
base_rate = y_test3.value_counts(normalize=True).max()
majority_class = y_test3.value_counts().idxmax()

print("Majority class:", majority_class)
print("Multiclass baseline accuracy:", round(base_rate, 4))

# Output:
# Majority class: good
# Multiclass baseline accuracy: 0.8907
```

Interpretation: if a model’s accuracy is not better than 89.07% accuracy, it is not providing meaningful predictive value because it is effectively behaving like a majority-class predictor.

After checking the baseline, we evaluate models using per-class precision/recall/F1 and log loss to see whether they are actually learning the minority classes rather than defaulting to “good” predictions.

```python
from sklearn.metrics import classification_report, accuracy_score, log_loss

# Predictions
y_tree3_pred = model_tree3.predict(X_test3)
y_lr3_pred = model_lr3.predict(X_test3)

print("Decision tree (3-class) accuracy:", round(accuracy_score(y_test3, y_tree3_pred), 4))
print("Logistic regression (3-class) accuracy:", round(accuracy_score(y_test3, y_lr3_pred), 4))

print("\nDecision tree (macro/weighted report):")
print(classification_report(y_test3, y_tree3_pred, digits=3, zero_division=0))

print("\nLogistic regression (macro/weighted report):")
print(classification_report(y_test3, y_lr3_pred, digits=3, zero_division=0))

# Probabilistic metric: multiclass log loss
y_tree3_prob = model_tree3.predict_proba(X_test3)
y_lr3_prob = model_lr3.predict_proba(X_test3)

print("Decision tree log loss:", round(log_loss(y_test3, y_tree3_prob), 4))
print("Logistic regression log loss:", round(log_loss(y_test3, y_lr3_prob), 4))

# Output
# Decision tree (3-class) accuracy: 0.8927
# Logistic regression (3-class) accuracy: 0.9375
#
# Decision tree (macro/weighted report):
#               precision    recall  f1-score   support
#
#          bad      0.463     0.106     0.172       180
#         good      0.901     0.992     0.944      1867
#         late      0.000     0.000     0.000        49
#
#     accuracy                          0.893      2096
#    macro avg      0.455     0.366     0.372      2096
# weighted avg      0.843     0.893     0.856      2096
#
#
# Logistic regression (macro/weighted report):
#               precision    recall  f1-score   support
#
#          bad      0.912     0.572     0.703       180
#         good      0.940     0.996     0.967      1867
#         late      0.500     0.061     0.109        49
#
#     accuracy                          0.938      2096
#    macro avg      0.784     0.543     0.593      2096
# weighted avg      0.928     0.938     0.924      2096
#
# Decision tree log loss: 0.4033
# Logistic regression log loss: 0.2246
```

At first glance, both models appear to perform well: the decision tree achieves about _89.3%_ accuracy and logistic regression achieves about _93.8%_ accuracy. This difference suggests that logistic regression is the stronger model overall, but accuracy alone does not tell the full story.

Because the _good_ class dominates the dataset, a model can achieve high accuracy simply by predicting “good” most of the time. This is why multiclass evaluation must always go beyond accuracy to examine per-class behavior.

Notice that the decision tree reports zero precision and recall for the _late_ class. This means the model never predicted this class at all. This is a common outcome when a class is rare and decision boundaries are optimized for overall accuracy rather than minority detection.

Logistic regression performs substantially better on minority classes. It achieves meaningful precision and recall for the _bad_ class and nonzero recall for the _late_ class.

Although recall for _late_ remains low, the key improvement is that the model is at least learning a decision boundary that sometimes detects warning cases instead of ignoring them entirely.

The difference in log loss is also large: approximately _0.403_ for the decision tree versus _0.225_ for logistic regression.

This indicates that logistic regression produces much better calibrated probability estimates across all three classes. Even when predictions are incorrect, its probability assignments tend to be less overconfident.

In risk modeling applications such as lending, probability quality is often more valuable than raw accuracy because decisions are based on risk thresholds, not just class labels.

```python
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

labels_order = ["good", "late", "bad"]

cm_tree3 = confusion_matrix(y_test3, y_tree3_pred, labels=labels_order)
cm_lr3 = confusion_matrix(y_test3, y_lr3_pred, labels=labels_order)

plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
ConfusionMatrixDisplay(cm_tree3, display_labels=labels_order).plot(cmap="Blues", values_format="d", ax=plt.gca())
plt.title("Decision Tree (3-class)")

plt.subplot(1, 2, 2)
ConfusionMatrixDisplay(cm_lr3, display_labels=labels_order).plot(cmap="Blues", values_format="d", ax=plt.gca())
plt.title("Logistic Regression (3-class)")

plt.tight_layout()
plt.show()
```

The confusion matrices visualize these patterns directly. For the decision tree, most _late_ loans are misclassified as _good_, confirming that the model does not separate warning cases into their own region of feature space.

For logistic regression, the confusion matrix shows fewer extreme failures. While some _late_ cases are still confused with _good_, a portion are correctly identified, creating a usable early-risk signal.

In multiclass problems, the confusion matrix shows not only how often predictions are correct, but _which classes are being confused with each other_. Each row represents the true class, and each column represents the predicted class.

This is especially important when classes have different business meanings. In this example, confusing _late_ loans with _good_ loans is much more costly than confusing them with _bad_ loans, because it hides early warning signs of default risk.

While accuracy and log loss summarize overall performance, the confusion matrix reveals _systematic error patterns_, such as models that completely ignore rare classes or consistently misclassify borderline cases. For operational decision-making, this class-by-class error structure is often more informative than any single summary metric.

#### Practical takeaway

Multiclass classification usually begins with a key modeling decision: whether categories should be kept separate or binned into larger groups. In this Lending Club example, we created three business-relevant outcome classes (good, late, bad) to stabilize learning and evaluation while preserving interpretability.

#### Summary

---

## 13.8 Log Reg vs Dec Trees

![Square conceptual diagram comparing logistic regression and decision tree regression models. On the left, logistic regression is illustrated with two classes of data points scattered in a two-dimensional space, separated by a straight diagonal decision boundary. Regions on each side are labeled with predicted probabilities near 0 and near 1, and a sigmoid curve shows how linear scores are transformed into probabilities between 0 and 1. Arrows indicate feature inputs flowing into the model and probability outputs flowing out. On the right, decision tree regression is illustrated with a tree structure that recursively splits the input space using feature thresholds. Branches lead to terminal leaf nodes that display numeric prediction values. The input space is partitioned into rectangular regions, each associated with a constant predicted outcome, highlighting how tree-based models make piecewise predictions.](../Images/Chapter13_images/class_lr_and_dt.png)

So far in this chapter, we have studied two core classification models: logistic regression and decision trees. Both can solve the same prediction problems, but they do so in fundamentally different ways and exhibit very different strengths and weaknesses.

Understanding these differences is more important than memorizing syntax. In practice, model selection is a design decision that balances interpretability, flexibility, stability, and risk.

Logistic regression produces smooth probability estimates and stable decision boundaries. It tends to generalize well and behaves predictably when data changes slightly.

Decision trees are more flexible and can automatically capture nonlinear interactions, but this flexibility comes at a cost: they are sensitive to noise and often overfit without careful depth control.

These differences explain the multiclass results seen earlier in this chapter: the tree achieved reasonable accuracy but failed to identify rare classes, while logistic regression produced better probability estimates and more balanced class detection.

#### When should you use each model?

- **Choose logistic regression when:** interpretability matters, probability quality matters, the dataset is small or noisy, or regulatory transparency is required.
- **Choose decision trees when:** relationships are highly nonlinear, feature interactions are complex, or explainable rule-based decisions are useful.

In modern practice, neither model is usually the final answer. Instead, they serve as the foundation for more powerful techniques that combine many models together.

In the next chapter, we will study ensemble methods, which are specifically designed to reduce the weaknesses of individual trees while preserving their flexibility.

---

## 13.9 Other Classification Algorithms

Logistic regression and decision trees are two of the most widely used classification models in practice. However, many other algorithms are available, each with different assumptions, strengths, and limitations.

In this section, we train several additional classifiers on the _same multiclass Lending Club problem_ introduced earlier and compare their performance using the same train/test split and evaluation metrics.

This allows us to focus on how algorithm choice affects results when the data, preprocessing, and label definition remain fixed.

#### k-Nearest Neighbors (k-NN)

![A conceptual visual depiction of how KNN classification works. The new case, represented by the star in the middle, is measured by distance to the nearts k (in this case 5) other cases and then classified based on which class is most represented by the k nearest neighbors.](../Images/Chapter13_images/class_knn.png)

k-Nearest Neighbors classifies a new observation by finding the _k_ most similar training points and assigning the majority label among them. k-NN requires no explicit training phase but is sensitive to feature scaling and becomes slow for large datasets.

This illustration to the right represents how the k-nearest neighbors (KNN) algorithm classifies a new observation based on proximity to existing labeled data points. Each colored cluster represents a different class in the training data, and the unlabeled point represents a new case whose class is unknown.

A circle drawn around the new point highlights its k closest neighbors in feature space, where distance is typically measured using Euclidean or a similar metric. The arrows from the neighboring points indicate that only these nearby observations influence the prediction, while distant points are ignored.

The predicted class is determined by a majority vote among the selected neighbors, meaning the class that appears most frequently among the k closest points is chosen. Because KNN makes no assumptions about the underlying data distribution, it can model highly irregular and nonlinear class boundaries.

Conceptually, this image emphasizes that KNN is an example-based method that defers learning until prediction time, relying entirely on stored training data rather than an explicit mathematical model.

Next, let's train a k-NN model in python:

```python
from sklearn.neighbors import KNeighborsClassifier

knn = KNeighborsClassifier(n_neighbors=7)

model_knn3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("knn", knn)
])

model_knn3.fit(X_train3, y_train3)
```

#### Naive Bayes

![Conceptual diagram of Naive Bayes classification showing three colored groups of data points labeled as different classes. A new unlabeled point is shown in the center. Arrows connect the new point to probability indicators for each class, illustrating that the algorithm independently estimates the likelihood of the point belonging to each class based on feature probabilities and selects the class with the highest overall probability.](../Images/Chapter13_images/class_nb.png)

Naive Bayes models use probability theory and assume that features are conditionally independent given the class label. Despite this unrealistic assumption, Naive Bayes often performs well in high-dimensional problems such as text classification.

The diagram to the right represents how the Naive Bayes classification algorithm assigns a class label using probabilities rather than geometric boundaries or distances. Each colored cluster represents an existing class in the training data, while the unlabeled point in the center represents a new observation that must be classified.

Arrows connect the new observation to probability indicators for each class, showing that Naive Bayes computes the likelihood of the observation belonging to each class separately. These probabilities are calculated by combining the conditional probabilities of each feature value given the class, under the simplifying assumption that features are independent of one another.

The model then selects the class with the highest resulting posterior probability as the predicted label. Although the independence assumption is often unrealistic in real-world data, Naive Bayes frequently performs well in practice because it estimates probabilities efficiently and remains robust even when features are moderately correlated.

Conceptually, this image highlights that Naive Bayes classifies points by comparing probabilistic evidence across classes rather than by learning complex decision boundaries or tree structures.

```python
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import FunctionTransformer

to_dense = FunctionTransformer(lambda x: x.toarray(), accept_sparse=True)

nb = GaussianNB()

model_nb3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("to_dense", to_dense),
  ("nb", nb)
])

model_nb3.fit(X_train3, y_train3)
```

Naive Bayes models in scikit-learn require _dense feature matrices_. However, modern preprocessing pipelines often produce _sparse matrices_ when one-hot encoding categorical variables. To resolve this mismatch, we explicitly convert the sparse matrix to a dense array inside the pipeline before passing it to the Naive Bayes classifier.

This conversion increases memory usage and computation time, which is one reason Naive Bayes is less commonly used in high-dimensional tabular problems with many categorical variables. In contrast, logistic regression and linear SVMs are designed to operate efficiently on sparse inputs.

Many machine learning models accept sparse matrices directly, but some probabilistic models (including Gaussian Naive Bayes) do not. When building pipelines, always verify whether an estimator supports sparse input to avoid runtime errors and unnecessary memory usage.

#### Support Vector Machines (SVM)

Support Vector Machines attempt to find the decision boundary that maximizes the margin between classes. With kernel functions, SVMs can model nonlinear boundaries, but they require careful tuning and do not naturally produce well-calibrated probabilities.

This visualization to the right represents how a support vector machine (SVM) classifier separates classes by constructing an optimal decision boundary. Each colored group of points corresponds to a different class, and the straight line between them represents the learned separating hyperplane.

Two parallel margin lines are shown on either side of the decision boundary, forming a corridor that the algorithm attempts to maximize. Only a small number of points lying closest to the boundary, called support vectors, directly determine the position and orientation of this separating line.

The highlighted new observation is classified according to which side of the boundary it falls on, rather than by measuring distance to many training points. By focusing on maximizing the margin, SVM aims to produce a boundary that generalizes well to unseen data and is robust to small perturbations.

Conceptually, this image shows that SVM classification depends on geometric separation and a small set of critical training points, rather than overall class averages or probability estimates.

```python
from sklearn.svm import SVC

svm = SVC(kernel="rbf", probability=True, random_state=27)

model_svm3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("svm", svm)
])

model_svm3.fit(X_train3, y_train3)
```

#### Shallow Neural Network (MLP)

Neural networks learn nonlinear transformations of the input features using layers of weighted combinations. Here we use a small multilayer perceptron (MLP) to demonstrate the basic idea without entering deep learning territory.

The image to the right illustrates the structure and logic of a **shallow neural network** used for multiclass classification. On the left, colored input nodes represent standardized feature values from a single loan record, such as income, credit history, or loan amount. These inputs are passed forward through weighted connections into a single hidden layer of neurons shown in the center. BTW, this image is not perfect. It was the best ChatGPT could do given my description ;). It will get better eventually. But for now, it gets teh main idea across.

Each hidden neuron computes a weighted sum of its inputs and applies a nonlinear activation function, allowing the model to capture interactions and curved decision boundaries that linear models cannot represent. The arrows emphasize that information flows in one direction only, from inputs to hidden layer to outputs, which is why this architecture is called a _feedforward_ network.

On the right, three output nodes correspond to the multiclass targets used in this chapter: _good_, _late_, and _bad_. The values at these nodes represent predicted probabilities after a softmax transformation, and the class with the highest probability becomes the final prediction.

The diagram highlights why shallow neural networks are more flexible than logistic regression but still simpler than deep learning models. With only one hidden layer, they can model moderate nonlinearity while remaining relatively fast to train and easier to tune. In later chapters, deeper neural networks extend this same structure by stacking many hidden layers to learn more complex representations.

```python
from sklearn.neural_network import MLPClassifier

mlp = MLPClassifier(
  hidden_layer_sizes=(32, 16),
  max_iter=500,
  random_state=27
)

model_mlp3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("mlp", mlp)
])

model_mlp3.fit(X_train3, y_train3)
```

#### Performance comparison

We now evaluate all models on the same test set using accuracy and multiclass log loss.

```python
from sklearn.metrics import accuracy_score, log_loss

models = {
  "Decision Tree": model_tree3,
  "Logistic Regression": model_lr3,
  "k-NN": model_knn3,
  "Naive Bayes": model_nb3,
  "SVM": model_svm3,
  "Neural Network": model_mlp3
}

results = []

for name, model in models.items():
  y_pred = model.predict(X_test3)
  y_prob = model.predict_proba(X_test3)

  results.append({
    "model": name,
    "accuracy": accuracy_score(y_test3, y_pred),
    "log_loss": log_loss(y_test3, y_prob)
  })

results_df = pd.DataFrame(results).sort_values("accuracy", ascending=False)
results_df
```

![DataFrame results of the model comparison. Logistic regression has the highest accuracy while SVM has the lowest log_loss suggesting those are the two best algorithms for this particular dataset.](../Images/Chapter13_images/class_model_df.png)

In most business datasets, logistic regression and shallow neural networks often provide the best probability quality, while trees and k-NN tend to struggle with minority classes.

SVMs can perform well but are harder to tune and interpret. Naive Bayes is usually fastest but rarely the most accurate. Let's put together a visual comparison of the results.

#### Visual comparison of model performance

The chart below compares all models using both accuracy (higher is better) and log loss (lower is better). Viewing both metrics together highlights the difference between correct classification and probability quality.

```python
import matplotlib.pyplot as plt

plot_df = results_df.copy()

fig, ax1 = plt.subplots(figsize=(10, 5))

# Accuracy bars (left axis)
ax1.bar(plot_df["model"], plot_df["accuracy"], alpha=0.7, label="Accuracy")
ax1.set_ylabel("Accuracy")
ax1.set_ylim(0, 1)

# Log loss line (right axis)
ax2 = ax1.twinx()
ax2.plot(plot_df["model"], plot_df["log_loss"], marker="o", color="red", label="Log loss")
ax2.set_ylabel("Log loss")

# Title and x-axis formatting
ax1.set_title("Classification model comparison (multiclass Lending Club)")
plt.setp(ax1.get_xticklabels(), rotation=30, ha="right")

# Combined legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

fig.tight_layout()
plt.show()
```

![A visual depiction of the model comparison. Logistic regression has the highest accuracy while SVM has the lowest log_loss suggesting those are the two best algorithms for this particular dataset.](../Images/Chapter13_images/class_model_comp.png)

#### Why not ensembles (yet)?

Many of the weaknesses observed above—tree instability, poor minority detection, and probability miscalibration—are precisely what _ensemble algorithms_ are designed to fix.

Random forests, gradient boosting, and boosting-based methods combine many weak models to produce dramatically more stable and accurate classifiers. Because ensembles represent a major conceptual shift, they are covered in their own dedicated chapter.

#### Why not deep neural networks?

Deep neural networks require different training procedures, regularization strategies, and hardware considerations. They are extremely powerful but demand more data, tuning, and theoretical background than is appropriate at this stage. For tabular business data, simpler models often outperform deep networks when properly tuned.

#### Practical takeaway

Logistic regression remains the best default for interpretable probability-based classification. Decision trees offer transparency and flexibility but require careful regularization. Other algorithms are valuable tools, but ensembles usually dominate modern applied classification. But now that you understand the basics of classification modeling, you'll be well prepared to shift to ensemble methods in a later chapter.

#### Model comparison summary

---

## 13.10 Case Studies

See how well you understand the chapter concepts by working through the practice problems below:

This practice uses a **Customer Churn** dataset. Your goal is to build **predictive classification models** that estimate whether a customer will _churn_ (leave the service). You will train and compare a _logistic regression_ model and a _decision tree_, evaluate their performance using classification metrics, and examine how tree depth affects overfitting.

**Dataset attribution:** This dataset is a commonly used telecommunications churn dataset containing customer demographics, service usage, contract details, and a binary churn outcome variable. See details on Kaggle.com

**Prediction goal:** Predict the binary outcome _Churn_ (Yes / No) using all available customer attributes except the target variable itself. You should treat this as a supervised classification problem using a full machine learning workflow with preprocessing pipelines.

For reproducibility, use **random_state = 27** everywhere that a random seed is accepted

**Tasks**

- Inspect the dataset: number of rows and columns, data types, missing values, and the class distribution of _Churn_.
- Create _X_ (features) and _y_ (target), where _y = Churn_. Encode the target as binary (0/1 or No/Yes).
- Split the data into _training_ and _test_ sets (80/20) using _random_state=27_ and _stratify=y_.
- Build an sklearn preprocessing pipeline using a _ColumnTransformer_: scale numeric features with _StandardScaler_ and one-hot encode categorical features using _OneHotEncoder(handle_unknown="ignore")_.
- Train a _logistic regression_ classifier inside the pipeline. Report test-set _accuracy_, _precision_, _recall_, and _ROC AUC_.
- Train a _DecisionTreeClassifier_ with default hyperparameters inside the same preprocessing pipeline. Report the same evaluation metrics.
- Plot and compare the _confusion matrices_ for both models. Comment on the types of errors each model makes.
- Visualize the trained decision tree using _sklearn.tree.plot_tree_ with _max_depth = 3_ for readability.
- Train at least three additional decision trees using different values of _max_depth_ (for example: 2, 5, and unrestricted). Record training and test-set accuracy for each.
- Create a small table or plot showing how training accuracy and test accuracy change as tree depth increases.

**Analytical questions**

1. How many rows and columns are in the Customer Churn dataset?
1. What percentage of customers in the dataset have churned?
1. What are the logistic regression model’s test-set accuracy, precision, recall, and ROC AUC?
1. What are the decision tree model’s test-set accuracy, precision, recall, and ROC AUC?
1. Which model performed better overall? Which model had higher recall for the churn class?
1. Include your decision tree visualization (depth ≤ 3). Which feature is used at the root split?
1. How did test-set accuracy change as _max_depth_ increased?
1. At what depth did the tree begin to show signs of overfitting? Explain using training vs test accuracy.
1. Short reflection (3–5 sentences): Why might logistic regression outperform a deep decision tree on this dataset? How does this relate to bias–variance tradeoff and model interpretability?

### Customer Churn Classification Practice Answers

These answers assume you used the provided Customer Churn dataset, created an 80/20 train/test split, and evaluated models on the holdout test set. Results are shown exactly as computed in the example run you provided (row counts, churn rate, baseline, logistic regression, a depth-3 decision tree, feature importances, and a depth sweep using accuracy and log loss).

#### Q1. Dataset size

The dataset contains **7032 rows** and **21 columns**.

#### Q2. Churn rate

The churn rate (share of customers labeled _Yes_) is **0.2658** (about **26.58%**).

#### Q3. Baseline model (majority class)

The baseline model predicts the most common class (_No_) for every customer. This yields a test-set **accuracy of 0.7342**.

The baseline test-set **log loss is 9.5809**, which is extremely poor because the baseline assigns near-certain probability to the wrong class for all churners (highly overconfident mistakes are heavily penalized by log loss).

#### Q4. Logistic regression results

The logistic regression model achieves test-set **accuracy = 0.8038** and **log loss = 0.4275**, substantially improving both classification accuracy and probability quality compared to the baseline.

From the classification report, logistic regression performs strongly on the majority class (_No_) with _precision = 0.851_ and _recall = 0.888_. For the churn class (_Yes_), performance is weaker with _precision = 0.648_ and _recall = 0.572_, meaning the model misses a meaningful portion of churners at the default threshold.

Macro averages (_macro avg f1 = 0.739_) summarize balanced performance across classes, while weighted averages (_weighted avg f1 = 0.800_) reflect the dominance of the non-churn class in the dataset.

#### Q5. Decision tree (max_depth = 3) results

The depth-3 decision tree achieves test-set **accuracy = 0.7783** and **log loss = 0.4505**. This is better than baseline but worse than logistic regression on both metrics in this run.

The decision tree shows a strong tendency to predict _No_. In the classification report, the churn class (_Yes_) has _recall = 0.350_, meaning it finds only about 35% of actual churners. This is consistent with the confusion matrix showing many churners misclassified as non-churners at the default threshold.

Compared to logistic regression, the tree’s churn recall is much lower (0.350 vs 0.572). If the business goal is to proactively intervene with likely churners, this recall gap can matter more than overall accuracy.

#### Q6. Confusion matrices (interpretation)

Logistic regression confusion matrix (test set): **TN = 917**, **FP = 116**, **FN = 160**, **TP = 214**. The model correctly identifies 214 churners but misses 160 churners.

Decision tree (depth = 3) confusion matrix (test set): **TN = 964**, **FP = 69**, **FN = 243**, **TP = 131**. The tree produces fewer false positives than logistic regression, but it misses far more churners (243 vs 160), which drives its low recall for the churn class.

This illustrates a common tradeoff: one model may be more conservative (fewer false alarms) while another is more sensitive (catches more positives). The “best” choice depends on the business costs of false positives vs false negatives.

#### Q7. Top predictors from the tree

The three most important features (impurity-based importance) in the tree are:

1. **Contract_Month-to-month** (importance _0.612992_)
1. **InternetService_Fiber optic** (importance _0.177452_)
1. **TotalCharges** (importance _0.114434_)

Interpretation: month-to-month contracts dominate the tree’s early splits, suggesting contract type is highly informative for churn risk in this dataset. Fiber optic internet service and billing-related measures (total charges, tenure, monthly charges) also contribute, but to a smaller extent in the learned tree.

#### Q8. Depth sweep and model selection

Your depth sweep shows that training performance improves monotonically as depth increases, but test performance improves only up to a point and then degrades. This is classic overfitting: deeper trees fit noise and become overconfident on the training data, which increases test log loss and reduces test accuracy after the best region.

Based on the test metrics you reported, the “best” depth depends on the metric:

- **Best by test log loss:** **max_depth = 4** (test log loss = _0.434553_).
- **Best by test accuracy:** **max_depth = 6** (test accuracy = _0.790334_).

A practical modeling judgment is to prefer the depth that optimizes _log loss_ when predicted probabilities drive decisions (rankings, thresholds, expected-cost calculations). If the operational workflow only uses hard class predictions, maximizing accuracy may be acceptable, but it can hide poorly calibrated probabilities.

In your sweep, test log loss becomes very large at deeper depths (for example, depth 10+), which strongly indicates the model is producing extremely confident wrong probabilities on the test set. That is why log loss is often a more sensitive diagnostic of overfitting than accuracy.

#### Q9. Short reflection sample answer

Deeper trees can keep reducing training error because they can create very specific splits that memorize patterns in the training data, including noise. However, those highly specific rules often do not generalize, so test performance stops improving and may worsen. This is the bias–variance tradeoff: shallow trees have higher bias (underfit), while deep trees have higher variance (overfit). Validation or test curves help identify the depth range where generalization is best.

This practice uses the **Employee Attrition** dataset (provided as _Employee_Attrition.csv_). Your goal is to build a **predictive** classification model that estimates whether an employee will leave the company (_Attrition_ = Yes/No). You will compare a **logistic regression** model and a **decision tree** model, evaluate them using classification metrics (accuracy, precision, recall, F1) and probabilistic metrics (log loss, ROC/AUC), and explore how tree depth affects overfitting.

**Dataset attribution:** This dataset is widely distributed as an “Employee Attrition / HR Analytics” teaching dataset (often based on IBM HR sample data). Your version is provided in this course as _Employee_Attrition.csv_. See details on Kaggle.com

**Prediction goal:** Predict whether _Attrition_ is Yes (employee leaves) or No (employee stays) using the other columns as predictors. Use a predictive workflow with a stratified train/test split, preprocessing in an sklearn pipeline, and evaluation on a holdout test set.

For reproducibility, use **random_state = 27** everywhere that a random seed is accepted

**Tasks**

- Inspect the dataset: number of rows/columns, data types, missing values, and the distribution of the _Attrition_ label.
- Define _X_ and _y_ where _y = Attrition_ and _X_ contains all remaining predictors. Remove ID-like columns (for example _EmployeeNumber_) if present.
- Split the data into _training_ and _test_ sets (80/20) using _random_state=27_ and _stratify=y_.
- Build an sklearn preprocessing pipeline: scale numeric predictors (_StandardScaler_) and one-hot encode categorical predictors (_OneHotEncoder_ with _handle_unknown="ignore"_). Fit preprocessing only on the training data.
- Establish a baseline classifier that always predicts the most common class. Report baseline test-set _accuracy_ and baseline _log loss_.
- Train a _LogisticRegression_ classifier inside your pipeline. Evaluate test-set accuracy, log loss, and a classification report (precision, recall, F1 by class).
- Train a _DecisionTreeClassifier_ (start with _max_depth=3_) inside your pipeline. Evaluate test-set accuracy, log loss, and a classification report.
- Create confusion matrices for both models and interpret what types of errors each model makes (false positives vs false negatives).
- Perform a depth sweep for the decision tree (for example: 1, 2, 3, 4, 5, 6, 8, 10, 12, 15). Create two validation-curve plots: accuracy vs depth and log loss vs depth (train vs test).
- Choose a “best” tree depth using test performance. Report the best depth according to test _log loss_ and the best depth according to test _accuracy_, and explain why these choices might differ.

**Analytical questions**

1. How many rows and columns are in the Employee Attrition dataset?
1. What proportion of employees have _Attrition = Yes_?
1. What are the baseline model’s test-set _accuracy_ and _log loss_? Briefly explain why log loss can look “extreme” for a baseline that outputs hard labels.
1. What are the logistic regression model’s test-set _accuracy_ and _log loss_? Include a classification report and interpret precision vs recall for the _Attrition = Yes_ class.
1. What are the decision tree model’s test-set _accuracy_ and _log loss_ at _max_depth=3_? Include a classification report and compare it to logistic regression.
1. Include both confusion matrices. Which model produces more false negatives for attrition, and why might that matter to an HR team?
1. Extract and report the top 10 impurity-based feature importances from your best-performing decision tree. Which features appear most influential?
1. From your depth sweep, which _max_depth_ is best by test _log loss_, and which is best by test _accuracy_? Provide the depth-sweep table and the two validation-curve plots.
1. Short reflection (3–5 sentences): Explain how the bias–variance tradeoff appears in your depth-sweep plots (training vs test curves). Identify where overfitting begins and support your claim using accuracy and/or log loss patterns.

### Employee Attrition Classification Practice Answers

These answers assume you used the Employee Attrition dataset provided, treated _Attrition_ as the binary label (Yes/No), used an 80/20 train/test split, and evaluated models using accuracy, log loss, and the classification report. Your specific numeric results may differ slightly if your preprocessing, random seed, or split differs.

1. **Q1 (rows, columns):** The dataset contains _1470_ rows and _35_ columns.
1. **Q2 (attrition rate):** The proportion of employees with _Attrition = Yes_ is _0.1612_ (about _16.12%_). This indicates a moderately imbalanced classification problem where the _No_ class is much more common.
1. **Q3 (baseline model):** Predicting the most common class (_No_) for every case yields _baseline accuracy = 0.8401_. The _baseline log loss = 0.4395_ reflects the fact that a hard, always-majority prediction provides poor probability estimates (and would be even worse if probabilities were treated as 0/1 with no smoothing).
1. **Q4 (logistic regression):** Logistic regression achieved _accuracy = 0.8878_ and _log loss = 0.3511_, improving on the baseline in both overall correctness and probability quality. In the classification report, the _Yes_ class shows _precision = 0.719_ and _recall = 0.489_, meaning the model identifies about half of true attrition cases while keeping false alarms relatively contained. The _No_ class remains very strong (_recall = 0.964_), which is common in imbalanced settings.
1. **Q5 (decision tree, max_depth = 3):** The depth-3 decision tree achieved _accuracy = 0.8435_ and _log loss = 0.3899_. While accuracy is close to the baseline, the key weakness is minority-class detection: the _Yes_ class has _recall = 0.213_, meaning the tree catches only about 21% of actual attrition cases. This usually happens because a shallow tree tends to favor the majority class unless splits strongly isolate the minority class early.
1. **Q6 (confusion matrix interpretation):** From the confusion matrices, logistic regression produces more true positives (correct _Yes_ predictions) than the tree, at the cost of some additional false positives. The decision tree (depth=3) misses many more attrition cases (false negatives), which aligns with its low _Yes_ recall.
1. **Q7 (top feature importances):** The most influential predictors in the depth-3 tree (by impurity-based importance) were: _OverTime_No_ (0.299), _MonthlyIncome_ (0.264), _TotalWorkingYears_ (0.103), _HourlyRate_ (0.102), _JobRole_Sales Executive_ (0.101), and _Age_ (0.095). These suggest the tree is using workload and compensation/career-stage signals to separate lower-risk vs higher-risk groups. Note that impurity importances can over-emphasize features with many possible split points and should be interpreted as a rough heuristic rather than a causal ranking.
1. **Q8 (depth sweep and model selection):** The depth sweep shows classic overfitting: training accuracy rises steadily with depth (up to 0.9966), while test accuracy peaks early and then fluctuates; test log loss worsens rapidly for deeper trees (e.g., > 3 at depth 8 and > 5 by depth 12–15). Using the test-set criteria reported, the best depth is _max_depth = 2_ for both _test log loss_ and _test accuracy_. This indicates that a very shallow tree generalizes best on this dataset and that deeper trees become increasingly overconfident on wrong predictions.
1. **Q9 (reflection guidance):** A deeper tree can always reduce training error because it can keep splitting until it memorizes patterns (including noise). However, those extra splits often fit idiosyncrasies of the training set that do not repeat in new data, so test performance can stagnate or worsen. This is the bias–variance tradeoff: shallow trees have higher bias (underfitting) while deep trees have higher variance (overfitting). The log-loss curve is especially sensitive here because overfit trees often output extreme probabilities that are heavily penalized when incorrect.

Overall takeaway: Logistic regression performed best in this run because it improved both accuracy and probability quality (log loss) while achieving substantially better detection of the minority _Yes_ class than the shallow tree. The depth sweep reinforces that, for trees, controlling complexity is essential—small increases in depth can quickly degrade probability calibration and generalization.

This practice uses a Telco customer support dataset of service tickets. Your goal is to build a **multiclass classification** model that predicts ticket _priority_ (Low, Medium, High) using structured ticket and customer context variables. You will compare multiple classification algorithms from this chapter and evaluate them using both _accuracy_ (threshold-based) and _multiclass log loss_ (probability-based).

**Dataset attribution:** The dataset file for this practice is _Support_tickets.csv_. See details on Kaggle.com

**Prediction goal:** Predict _priority_ (Low, Medium, High). In this practice, treat the classes as _nominal_ (not ordered) and use standard multiclass classification algorithms and metrics.

**Recommended feature set:** Use a mix of numeric and categorical predictors. For clarity, use the human-readable categorical columns (for example: _day_of_week_, _company_size_, _industry_, _customer_tier_, _region_, _product_area_, _booking_channel_, _reported_by_role_, _customer_sentiment_) and numeric operational columns (for example: _org_users_, _past_30d_tickets_, _past_90d_incidents_, _customers_affected_, _error_rate_pct_, _downtime_min_, plus the binary flags such as _payment_impact_flag_ and _security_incident_flag_). Exclude identifier-like columns such as _ticket_id_.

**Note on duplicate encodings:** This dataset includes both readable categorical columns (for example, _industry_) and numeric coded versions (for example, _industry_cat_). For this practice, choose _one approach_ and be consistent. The recommended approach is to use the readable categorical columns with one-hot encoding and ignore the _\_cat_ versions.

For reproducibility, use **random_state = 27** everywhere that a random seed is accepted (for example: _train_test_split_, _LogisticRegression_, _DecisionTreeClassifier_, and _MLPClassifier_).

**Tasks**

- Inspect the dataset: report the number of rows and columns, confirm the target label values in _priority_, and compute the class proportions (counts and percentages).
- Create _X_ and _y_ where _y = priority_. Exclude _ticket_id_ and exclude all _\_cat_ columns (use the readable categories instead).
- Split the data into _training_ and _test_ sets (80/20) using _random_state=27_ and _stratify=y_.
- Build a preprocessing pipeline with _StandardScaler_ for numeric predictors and _OneHotEncoder(handle_unknown="ignore")_ for categorical predictors. For this practice, configure the encoder to return dense output (_sparse_output=False_) so the same preprocessing works across all algorithms.
- Establish a baseline classifier: predict the _most frequent class_ for all test cases. Report baseline _accuracy_. Then compute baseline _multiclass log loss_ by predicting the _training-set class proportions_ as constant probabilities for every test case.
- Train and evaluate a multiclass _logistic regression_ model using a one-vs-rest strategy (via _OneVsRestClassifier_). Report test-set _accuracy_, _classification report_ (macro and weighted), and _log loss_.
- Train and evaluate a multiclass _decision tree classifier_ (start with _max_depth=3_). Report the same metrics as in Task 6. Then run a small depth sweep (for example: 1, 2, 3, 4, 5, 6, 8, 10, 12) and identify the best depth by test _log loss_.
- Extract and plot the tree’s impurity-based _feature importances_. Create a top-15 table of importance values. (If you one-hot encoded categorical variables, you may report the one-hot feature names directly.)
- Train and evaluate the additional classification algorithms from Section 13.9 as multiclass models: _k-NN_, _Naive Bayes_, _SVM_, and a _shallow neural network_ (MLP). Use the same train/test split and the same preprocessing pipeline for fair comparison. Bear in mind, shallow neural networks can take longer to train than the other models in this practice. If your MLP model is slow, apply the time-saving steps below (in order) until training completes quickly.
- Create a single comparison table that lists all models (baseline, logistic regression, decision tree, k-NN, Naive Bayes, SVM, shallow neural network) and includes _test accuracy_ and _test log loss_.
- Create a visual comparison chart. Because accuracy and log loss are on different scales, use a two-axis chart (left axis for accuracy bars, right axis for log loss line) so one extreme log loss value does not flatten the accuracy bars.
- Create a multiclass _confusion matrix_ for your two best-performing models (choose based on log loss and macro averages). Briefly interpret which classes are most frequently confused.
- Write a short model selection checklist (5–8 bullets) describing how you would choose among these models in a real support operation, considering interpretability, probability quality, minority-class performance, and deployment constraints.

**Model training reminders**

- For multiclass logistic regression, use _OneVsRestClassifier(LogisticRegression(...))_ to avoid deprecated multiclass settings and to keep the one-vs-rest idea explicit.
- For SVM, enable probability estimates (_probability=True_) if you want log loss; this increases training time but allows _predict_proba_.
- For Naive Bayes, using dense features is acceptable in this dataset because the one-hot expansion is manageable; that is why the encoder is configured with _sparse_output=False_.
- For the shallow neural network, use _MLPClassifier_ with one hidden layer and early stopping; do not attempt deep architectures here.
- **Ensembles are intentionally excluded:** Random forests, gradient boosting, and stacking typically outperform single models but deserve their own dedicated chapter because they introduce new concepts (bagging, boosting, model aggregation, and tuning strategies).

**Analytical questions**

1. How many rows and columns are in the Support Tickets dataset?
1. What are the counts and percentages of each _priority_ class?
1. What are the baseline model’s test-set _accuracy_ and _log loss_?
1. What are the logistic regression model’s test-set _accuracy_ and _log loss_? Which class has the lowest recall, and why might that be operationally important?
1. What are the decision tree model’s test-set _accuracy_ and _log loss_ at _max_depth=3_?
1. In the depth sweep, which max depth performed best by test _log loss_? Did that same depth also maximize test accuracy?
1. Which 5–10 features (or one-hot encoded feature names) appear most important for the tree? Provide a brief business interpretation of the top two.
1. Compare the multiclass confusion matrices for your two best models. Which priority levels are most commonly confused with each other?
1. Fill in your model comparison table for all models. Which model has the best _log loss_? Which model has the best _macro average F1_?
1. Short reflection (5–8 sentences): If you were implementing this in a real support operation, which model would you choose and why? Address interpretability, probability quality, and the cost of confusing High priority with Low or Medium.

### Support Tickets Multiclass Practice Answers

These answers are based on the Support Tickets dataset results you reported (50,000 rows) and the multiclass priority label with three unordered classes: _low_, _medium_, and _high_.

1. The Support Tickets dataset contains _50,000_ rows and _33_ columns.
1. Priority class distribution: _low = 25,000 (50.0%)_, _medium = 17,500 (35.0%)_, and _high = 7,500 (15.0%)_. This is an imbalanced multiclass label because the majority class (_low_) is much more common than _high_.
1. (Baseline) The baseline model achieved _test accuracy = 0.5000_ and _test log loss = 0.9986_. The baseline accuracy matches the majority class rate (50%), which means it is essentially behaving like a “predict low for everything” strategy.
1. (Logistic regression OvR) The logistic regression model achieved _test accuracy = 0.8536_ and _test log loss = 0.4298_. The class with the lowest recall is _medium_ (_recall = 0.753_, slightly lower than _high_ at 0.770). Operationally, low recall for _medium_ means many true medium-priority tickets are being downgraded to _low_ (or occasionally escalated), which can distort staffing forecasts and response-time commitments for the “middle tier” workload.
1. (Decision tree, max*depth = 3) The depth-3 decision tree achieved \_test accuracy = 0.7752* and _test log loss = 0.5239_. Compared to logistic regression, this tree is less accurate overall and produces weaker probability estimates (higher log loss).
1. (Depth sweep) The best max depth by _test log loss_ was _6_ (test log loss = _0.3476_). The depth that maximized _test accuracy_ was _12_ (test accuracy = _0.9195_). These are not the same depth, which illustrates a common pattern: deeper trees can improve threshold-based accuracy while harming probability quality (log loss rises sharply at depths 10–12).
1. (Top feature importances, tree) The most important one-hot or raw features reported were: _customers_affected_, _downtime_min_, _error_rate_pct_, _customer_tier_Enterprise_, _customer_tier_Plus_, _reported_by_role_c_level_, _payment_impact_flag_, and smaller contributions from _product_area_analytics_. Business interpretation: (1) _customers_affected_ dominates because impact scope is a direct driver of urgency—issues affecting many customers are more likely to be classified as _high_. (2) _downtime_min_ is also a major driver because longer outages typically imply larger revenue, SLA, and reputational risk, which justifies higher priority.
1. (Confusion matrices) Your two strongest overall models by reported performance are _Shallow NN (MLP)_ and _SVM (RBF)_ (highest accuracies). In multiclass triage settings like this, the most common confusion typically occurs between neighboring operational categories: _high vs medium_ and _medium vs low_. That pattern is consistent with your per-class recalls: for the tree (depth 3), _high_ recall is much lower (0.477) while _medium_ recall is high (0.811), suggesting many true _high_ tickets are being predicted as _medium_ rather than _low_. For logistic regression, both _high_ and _medium_ have moderate recall (0.770 and 0.753), indicating the remaining errors are likely confusions among these two classes and the majority class (_low_) rather than extreme _high ↔ low_ swaps.
1. (Model comparison) Your completed table shows the best log loss is from _Shallow NN (MLP)_ with _log loss = 0.1393_. The best macro-average F1 among the models that reported it is also the _Shallow NN (MLP)_ (macro avg F1 = _0.942_). The SVM’s macro F1 is high (0.899), but its log loss is not available in your run (listed as n/a), so it cannot be compared on probability quality in this table.
1. Reflection example (one strong answer): In a real support operation, I would likely choose the _Shallow NN (MLP)_ as the production model because it delivers the strongest overall performance on both dimensions that matter: it has the best accuracy (_0.9446_) and the best probability quality (log loss _0.1393_). Probability quality matters because triage often uses risk thresholds to route tickets into queues (auto-escalate, human review, standard queue), not just a single hard label. However, I would pair the MLP with a simpler reference model (like logistic regression) for interpretability and troubleshooting, because managers often need to explain why a ticket was escalated. I would also put explicit safeguards around _high_ priority: the cost of predicting _high_ as _low_ can be severe (missed outages, SLA violations), so I would examine the confusion matrix and potentially tune thresholds or use cost-sensitive weighting to reduce “high → low” errors. If interpretability were the top priority (for auditability or policy), I would select logistic regression instead because it is easier to explain and still performs well (accuracy _0.8536_, log loss _0.4298_). Overall, the final choice depends on whether the organization values maximum triage accuracy and calibrated probabilities (favoring MLP) or simpler explanations and governance (favoring logistic regression).

---

## 13.11 Learning Objectives

## 13.12 Assignment

Complete the assignment(s) below (if any):

### 13.12 Classification Modeling

#### Task:

1. **Two-class model**: Predict whether students are currently active in school (binary classification)
1. **Multi-class model**: Predict student status as Active, Probation, or Terminated (3-class classification)

#### Dataset Overview:

- IN_SCHOOL_FLAG: Binary indicator (0 = not active, 1 = currently active in school)
- STATUS_DESCRIPTION: Detailed status category (e.g., "Active", "Graduate", "Probation", "Drop")
- SIMPLE_STATUS_DESCRIPTION: Simplified status combining similar categories (e.g., "Active", "Terminated", "Other")

- EXPECTED_START_DATE: When the student was expected to begin
- GRADUATION_DATE: When the student is expected to/did graduate
- BIRTH_DATE: Student's date of birth
- LAST_ACTIVITY_DATE: Most recent date the student attended class

- NUMBER_AVERAGE: Average of numerical grades
- ENROLLMENT_GPA: GPA for current enrollment only
- CUMMULATIVE_GPA: GPA across all enrollments
- CUMMULATIVE_GPA_POINTS: Grade points earned across all enrollments
- CUMMULATIVE_GPA_CREDITS: Credits used to calculate cumulative GPA

- MINUTES_ATTENDED: Total class time attended (cumulative)
- MINUTES_ABSENT: Total class time missed (cumulative)
- DAYS_ABSENT: Number of days absent
- MINUTES_MAKEUP: Minutes of missed class time that were made up
- MODS_ATTENDED_COUNT: Number of months/modules completed
- MOD_NUMBER: Most recent month/module completed (1-12, one course per month)

- HOURS_ATTEMPTED: Credit hours attempted (includes failures and withdrawals)
- HOURS_EARNED: Credit hours successfully completed
- CREDITS_ATTEMPTED: Similar to HOURS_ATTEMPTED
- CREDITS_EARNED: Similar to HOURS_EARNED
- CREDITS_REQUIRED: Total credits needed to graduate
- CREDITS_LEFT: Credits remaining until graduation

- ENROLL_COUNT: Number of times student has started/restarted (>1 means they dropped out before)
- ENROLLMENT_COUNT: Duplicate of ENROLL_COUNT
- REENTRY_NUMBER: Number of times student has re-enrolled after dropping out
- COHORT_YEAR: Year the student (re)started their program

- AR_BALANCE_AMOUNT: Amount owed on student account
- AR_BALANCE: Duplicate of AR_BALANCE_AMOUNT

- PROGRAM_GROUP: Academic program/degree code (e.g., "GAGAB", "MSMAA")
- HS_GRADUATED_FLAG: Whether student graduated from high school (0/1)
- DISABLED_FLAG: Whether student has a registered disability (0/1)
- HISPANIC_FLAG: Whether student identifies as Hispanic (0/1)
- VETERAN_FLAG: Whether student is a veteran (0/1)

#### Assignment Structure:

1. **Data Import & Exploration** (Questions 1-2)
1. **Data Cleaning** (Questions 3-7): Handle missing values, convert dates, bin categories, relabel data
1. **Two-Class Classification** (Questions 8-15): Build a binary classifier to predict if students are active
1. **Multi-Class Data Preparation** (Questions 16-19): Prepare labels for 3-class classification
1. **Multi-Class Classification** (Questions 20-27): Build a 3-class classifier to predict Active/Probation/Terminated

#### Objectives:

1. Download the dataset (student_enrollment_sample.csv) and template file (.ipynb)
1. Complete each task listed in the questions below
1. Answer the verification question for each task in the LMS
1. Upload your completed .ipynb file where specified

---
