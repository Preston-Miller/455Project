# Chapter 14: Ensemble Methods

## 14.1 Introduction

![Conceptual illustration showing the idea of ensemble learning. On the left, several different individual machine learning models (represented as small decision trees, linear lines, and simple charts) each make their own separate predictions, some of which are inaccurate or inconsistent. Arrows from these individual models point toward the right side of the image, where their outputs are combined into a single larger model labeled “Ensemble Model.” The ensemble produces one final, more stable prediction. The image visually emphasizes that many weak or imperfect models can be combined to create a stronger, more reliable overall model.](../Images/Chapter14_images/ensemble_header.png)

In a prior chapter, we built classification models using individual algorithms such as logistic regression, decision trees, k-nearest neighbors, and support vector machines. Each of these models can perform well, but each also has systematic weaknesses that limit how far its performance can be pushed.

Ensemble methods address this limitation by combining the predictions of many models into a single predictor. Rather than relying on one imperfect model, we allow multiple models to contribute evidence toward each prediction.

The core idea is simple: if different models make different mistakes, their errors can partially cancel out when combined. When designed carefully, the resulting ensemble is often more accurate, more stable, and better calibrated than any single model alone.

Ensemble methods address model weaknesses through two primary mechanisms: variance reduction and bias reduction. Variance reduction occurs when averaging multiple models smooths out the instability of individual predictions. Bias reduction occurs when sequential models focus on correcting the errors of previous models. Different ensemble approaches emphasize these mechanisms to different degrees:

By the end of this chapter, you should be able to:

- Explain why ensembles often outperform single models.
- Describe the difference between variance reduction and bias reduction.
- Train and evaluate several common ensemble algorithms using scikit-learn.
- Interpret ensemble performance using accuracy, log loss, and class-level metrics.

To understand why ensembles help, it is useful to first examine the typical weaknesses of single-model approaches. Different algorithms fail in different ways, which creates opportunities for improvement through combination.

Decision trees represent the classic high-variance case: two trees trained on slightly different samples can produce very different structures and predictions. This instability makes trees ideal candidates for ensemble techniques that average across many trees.

Linear models such as logistic regression represent the opposite extreme: they are stable but often too simple to capture real-world complexity. In this case, ensembles can help by combining multiple weak linear boundaries into a more expressive decision function.

Many algorithms are also sensitive to noise, outliers, or small sample fluctuations, which further increases prediction error. Ensembles reduce this sensitivity by spreading risk across multiple learned representations.

This logic closely mirrors the **wisdom of crowds** principle: while individuals may be unreliable, the average of many diverse opinions is often surprisingly accurate. Ensemble learning formalizes this idea in a statistical framework.

From a theoretical perspective, ensembles improve performance by altering the balance between _bias_ and _variance_. Some methods primarily reduce variance, others reduce bias, and some attempt to control both simultaneously.

Throughout this chapter, we will continue using the Lending Club dataset introduced earlier to illustrate ensemble classification techniques. This continuity allows direct comparison between single models and their ensemble counterparts.

Although our examples focus on classification, nearly every ensemble method discussed also has a regression version and is widely used for predicting continuous outcomes such as prices, demand, and financial risk.

---

## 14.2 Baseline Models

### Data Preparation

Before we build baseline models or ensembles, we will apply a small set of data preparation steps that remain consistent across the entire chapter.

This consistency matters because ensemble methods compare many models side-by-side, and we want differences in performance to come from the algorithms (not from changing preprocessing choices).

#### Drop columns that are not useful for modeling

We will drop _loan_status_numeric_ (it can leak information or duplicate the label definition) and remove text-heavy identifier-style fields (_emp_title_ and _title_) that are not used in this chapter.

#### Convert issue date into a numeric recency feature

Because most machine learning algorithms do not use raw date strings directly, we convert _issue_d_ into a numeric feature representing how many days ago the loan was issued relative to the most recent loan in the dataset.

#### Handle missing values

The columns _mths_since_last_delinq_ and _mths_since_last_record_ are missing when a borrower has never had a delinquency or a public record; these missing values are meaningful, not random.

A good approach is to (1) add a binary indicator for whether the borrower has ever had a delinquency or record and (2) fill missing values with a “large” number (such as the observed maximum plus one) to represent “not observed in history.”

For the rest of the predictors, we recommend imputing numeric columns with the median and categorical columns with the most frequent category (or an explicit _Unknown_ label), which we will implement inside the preprocessing pipeline so it is learned only from the training data.

#### Other cleaning opportunities in this dataset

- Convert _term_ from strings like “36 months” into an integer number of months.
- Convert _emp_length_ into an approximate numeric value in years (and keep an _Unknown_ category if it is missing).
- Treat remaining string fields as categorical predictors and rely on one-hot encoding rather than manual recoding.

```python
import pandas as pd
import numpy as np

df = pd.read_csv("lc_small.csv")

drop_cols = ["loan_status_numeric", "emp_title", "title"]
drop_cols = [c for c in drop_cols if c in df.columns]
df = df.drop(columns=drop_cols)

df["issue_d"] = pd.to_datetime(df["issue_d"], errors="coerce")
max_issue_date = df["issue_d"].max()
df["issue_age_days"] = (max_issue_date - df["issue_d"]).dt.days
df = df.drop(columns=["issue_d"])

if "term" in df.columns:
  df["term"] = df["term"].astype(str).str.strip().str.extract(r"(\d+)").astype(float)

if "emp_length" in df.columns:
  emp = df["emp_length"].astype(str).str.strip()
  emp = emp.replace({"nan": np.nan, "None": np.nan})

  emp = emp.replace({
    "10+ years": "10",
    "< 1 year": "0"
  })

  emp = emp.str.extract(r"(\d+)")[0]
  df["emp_length_years"] = pd.to_numeric(emp, errors="coerce")
  df = df.drop(columns=["emp_length"])

for col in ["mths_since_last_delinq", "mths_since_last_record"]:
  if col in df.columns:
    ind_col = col + "_missing"
    df[ind_col] = df[col].isna().astype(int)

    max_val = df[col].max(skipna=True)
    fill_val = (max_val + 1) if pd.notna(max_val) else 0
    df[col] = df[col].fillna(fill_val)
```

After running this cell, you should keep using the updated _df_ object throughout the rest of Chapter 14 so that every baseline model and ensemble is trained on the same cleaned inputs.

### Why we revisit single models

Before we build ensembles, we need a clear baseline for what “good performance” looks like using single models. This section recreates several models from the prior chapter so we can compare them fairly against bagging and boosting later.

Ensembles usually improve performance by reducing common weaknesses in single models, such as overfitting in trees or limited flexibility in linear models. To see that improvement clearly, we must keep the dataset, features, preprocessing, and train/test split identical across models.

#### Models included in the baseline comparison

We will rebuild four familiar classifiers: logistic regression, a shallow decision tree, k-nearest neighbors (k-NN), and Naive Bayes. Each model represents a different modeling philosophy, which helps motivate why combining models can work so well.

- **Logistic regression** (one-vs-rest): a strong linear baseline with interpretable coefficients and typically reasonable probability estimates.
- **Decision tree** (shallow depth): an interpretable nonlinear baseline that can capture interactions but may overfit as depth increases.
- **k-NN**: a distance-based approach that can model complex boundaries but can be sensitive to scaling and noisy features.
- **Naive Bayes**: a fast probabilistic model that makes strong independence assumptions and can struggle when features are correlated.

#### Step 1: Reload the dataset and recreate train/test objects

The code below rebuilds _X_, _y_, the train/test split, and the preprocessing pipeline. It is written to be easy to adapt to your exact Lending Club file and column names.

Important: if your prior chapter already created _df_, _X_, and _y_, you can reuse those and skip the loading and target-recoding parts. The key requirement is that you keep the same features and the same label definition so that later ensemble comparisons are valid.

```python
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

bad_statuses = {"Charged Off", "Default"}
df["loan_good"] = (~df["loan_status"].isin(bad_statuses)).astype(int)

y = df["loan_good"].copy()
X = df.drop(columns=["loan_status", "loan_good"]).copy()

X_train, X_test, y_train, y_test = train_test_split(
  X, y,
  test_size=0.20,
  random_state=27,
  stratify=y
)

cat_cols = X_train.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
num_cols = X_train.select_dtypes(include=["number"]).columns.tolist()

numeric_pipe = Pipeline(steps=[
  ("impute", SimpleImputer(strategy="median")),
  ("scale", StandardScaler())
])

categorical_pipe = Pipeline(steps=[
  ("impute", SimpleImputer(strategy="most_frequent")),
  ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
  transformers=[
    ("num", numeric_pipe, num_cols),
    ("cat", categorical_pipe, cat_cols)
  ],
  remainder="drop",
  sparse_threshold=0.3
)
```

Because one-hot encoding often creates many columns, the preprocessor may output a sparse matrix. That is efficient for most models, but a few algorithms (notably _GaussianNB_) require a dense matrix, so we will handle that explicitly when we build the Naive Bayes pipeline.

#### Step 2: Rebuild baseline models

Next, we define four pipelines that all share the same preprocessing step. This ensures that any performance differences come from the modeling algorithm rather than from inconsistent data preparation.

```python
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB

# Logistic regression (OvR)
lr_base = LogisticRegression(
  solver="liblinear",
  max_iter=2000,
  random_state=27
)
model_lr = Pipeline(steps=[
  ("prep", preprocessor),
  ("lr", OneVsRestClassifier(lr_base))
])

# Decision tree baseline (shallow)
model_tree3 = Pipeline(steps=[
  ("prep", preprocessor),
  ("tree", DecisionTreeClassifier(
    max_depth=3,
    random_state=27
  ))
])

# k-NN baseline (k chosen for a reasonable starting point)
model_knn = Pipeline(steps=[
  ("prep", preprocessor),
  ("knn", KNeighborsClassifier(n_neighbors=15))
])

# Naive Bayes (GaussianNB) requires dense features.
# Convert sparse matrix to dense using FunctionTransformer.
to_dense = FunctionTransformer(
  lambda X: X.toarray() if hasattr(X, "toarray") else np.asarray(X),
  accept_sparse=True
)

model_nb = Pipeline(steps=[
  ("prep", preprocessor),
  ("dense", to_dense),
  ("nb", GaussianNB())
])
```

#### Step 3: Evaluate with accuracy, log loss, and the classification report

We evaluate each model using both threshold-based and probability-based metrics. Accuracy is easy to interpret, but log loss helps us judge probability quality, which matters in lending decisions that use risk thresholds.

We also include a classification report because class imbalance can make accuracy misleading. In credit risk, the positive class (defaults) is usually the minority class, so per-class precision and recall are often more operationally important than overall accuracy.

```python
from sklearn.metrics import accuracy_score, log_loss, classification_report

def eval_model(name, model, X_train, y_train, X_test, y_test):
  model.fit(X_train, y_train)

  y_pred = model.predict(X_test)
  acc = accuracy_score(y_test, y_pred)

  # Log loss requires predicted probabilities.
  # Some models provide predict_proba; if not, we skip log loss.
  ll = None
  if hasattr(model, "predict_proba"):
    y_prob = model.predict_proba(X_test)
    ll = log_loss(y_test, y_prob)

  print("\n" + "=" * 70)
  print(name)
  print("Accuracy:", round(acc, 4))
  if ll is not None:
    print("Log loss:", round(ll, 4))
  else:
    print("Log loss: n/a (no predict_proba)")

  print("\nClassification report:")
  print(classification_report(y_test, y_pred, digits=3, zero_division=0))

  return {
    "model": name,
    "accuracy": acc,
    "log_loss": ll
  }

results = []
results.append(eval_model("Logistic regression (OvR)", model_lr, X_train, y_train, X_test, y_test))
results.append(eval_model("Decision tree (depth=3)", model_tree3, X_train, y_train, X_test, y_test))
results.append(eval_model("k-NN (k=15)", model_knn, X_train, y_train, X_test, y_test))
results.append(eval_model("Naive Bayes (GaussianNB)", model_nb, X_train, y_train, X_test, y_test))

results_df = pd.DataFrame(results)
results_df = results_df.sort_values(by=["log_loss", "accuracy"], ascending=[True, False])
results_df

# Output:
# Decision tree (depth=3)
# Accuracy: 0.9342
# Log loss: 0.2156
#
# Classification report:
#               precision    recall  f1-score   support
#
#           0      0.875     0.272     0.415       180
#           1      0.936     0.996     0.965      1916

#          accuracy                          0.934      2096
#         macro avg      0.905     0.634     0.690      2096
#      weighted avg      0.931     0.934     0.918      2096

#  ======================================================================
#  k-NN (k=15)
#  Accuracy: 0.916
#  Log loss: 0.5692

#  Classification report:
#                precision    recall  f1-score   support

#            0      0.611     0.061     0.111       180
#            1      0.919     0.996     0.956      1916

#      accuracy                          0.916      2096
#    macro avg      0.765     0.529     0.534      2096
#  weighted avg      0.892     0.916     0.883      2096

#  ======================================================================
#  Naive Bayes (GaussianNB)
#  Accuracy: 0.2238
#  Log loss: 27.9785

#  Classification report:
#                precision    recall  f1-score   support

#            0      0.087     0.844     0.157       180
#            1      0.919     0.165     0.280      1916

#      accuracy                          0.224      2096
#    macro avg      0.503     0.505     0.219      2096
#  weighted avg      0.847     0.224     0.270      2096
```

#### Deliverable: A small comparison table of single models

Your comparison table now provides a concrete baseline for the rest of the chapter. In this dataset, logistic regression achieved the strongest overall performance among the single models, with an accuracy of 0.954 and a log loss of 0.130. The decision tree (depth = 3) followed with an accuracy of 0.934 and log loss of 0.216, while k-NN reached 0.916 accuracy but with substantially worse probability quality (log loss = 0.569). Naive Bayes performed very poorly, with only 0.224 accuracy and extremely high log loss (27.98), indicating badly miscalibrated probabilities.

These results also highlight why accuracy alone can be misleading. Both the decision tree and k-NN models achieved high overall accuracy, yet their classification reports show very weak recall for the minority class (only 0.272 for the tree and 0.061 for k-NN). In contrast, logistic regression maintained strong performance on both classes while also producing the most reliable probability estimates. This table will serve as the benchmark for evaluating whether ensemble methods can simultaneously improve minority-class detection and probability quality, not just headline accuracy.

---

## 14.3 Bagging: Bootstrap Aggregation

Bagging, short for **bootstrap aggregation**, is one of the simplest and most powerful ensemble techniques. Its primary goal is to reduce model variance by averaging many noisy but flexible models instead of relying on a single unstable one.

Decision trees are especially good candidates for bagging because small changes in training data can lead to very different tree structures. This instability makes trees high-variance models, which means they often overfit even when they achieve high training accuracy.

#### Key idea

Rather than training one tree on the full dataset, bagging trains many trees on slightly different versions of the training data created by random resampling. Each tree makes its own prediction, and the ensemble combines those predictions into a single result.

- **Bootstrap sampling:** each model is trained on a random sample drawn with replacement from the training set.
- **Independent models:** trees are trained in parallel, not sequentially.
- **Aggregation:** predicted probabilities are averaged across all trees.

Because the individual trees make different mistakes, averaging their predictions tends to cancel out extreme errors. The result is a model that is more stable and usually generalizes better than any single tree.

#### Algorithm overview

Bagging does not directly reduce bias because each tree is still a flexible model that can fit complex patterns. Its strength lies in reducing variance by stabilizing predictions across many slightly different models.

#### Bagging in scikit-learn

Scikit-learn implements bootstrap aggregation through the _BaggingClassifier_ class. We reuse the same preprocessing pipeline and train/test split as the baseline models so that any performance differences can be attributed to bagging rather than changes in data preparation.

```python
from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier

base_tree = DecisionTreeClassifier(
  max_depth=3,
  random_state=27
)

model_bagging = Pipeline(steps=[
  ("prep", preprocessor),
  ("bag", BaggingClassifier(
      estimator=base_tree,
      n_estimators=100,
      bootstrap=True,
      n_jobs=-1,
      random_state=27
  ))
])
```

We intentionally use the same tree depth as the single-tree baseline to isolate the effect of aggregation itself. Any improvement in accuracy or log loss therefore comes from averaging many unstable models rather than increasing the complexity of each individual tree.

The most important bagging parameters control how many models are trained, how the data are resampled, and how predictions are combined. Understanding these settings helps you reason about the bias–variance tradeoff and computational cost.

In classification, bagging averages the predicted class probabilities from all trees and then selects the class with the highest average probability. This probability averaging is the main reason bagging often improves log loss more reliably than accuracy.

#### Evaluate and compare to a single tree

```python
bagging_results = eval_model(
  "Bagging (100 trees, depth=3)",
  model_bagging,
  X_train,
  y_train,
  X_test,
  y_test
)

results_df = pd.concat(
  [results_df, pd.DataFrame([bagging_results])],
  ignore_index=True
).sort_values(by=["log_loss", "accuracy"], ascending=[True, False])

results_df

# Output:
# ======================================================================
# Bagging (100 trees, depth=3)
# Accuracy: 0.9356
# Log loss: 0.1949

#  Classification report:
#                precision    recall  f1-score   support

#                0      0.979     0.256     0.405       180
#                1      0.935     0.999     0.966      1916

#          accuracy                          0.936      2096
#        macro avg      0.957     0.628     0.686      2096
#      weighted avg      0.938     0.936     0.918      2096
```

![Table showing bagging model performance on the Lending Club dataset. Columns are model name, accuracy, and log loss. Bagging (100 trees, depth=3) has accuracy 0.936 and log loss 0.1949, the best overall. The classification report shows that the bagging model has a high precision for the positive class (0.935) and a high recall (0.999), indicating that it is able to correctly identify most of the positive cases.](../Images/Chapter14_images/bagging_results_df.png)

In this experiment, bagging improves probability quality more than raw classification accuracy. The single decision tree achieved an accuracy of _0.9342_ with log loss _0.2156_, while the bagged ensemble of 100 trees reached a similar accuracy of _0.9356_ but a noticeably lower log loss of _0.1949_. This confirms that averaging predictions stabilizes probability estimates even when overall accuracy changes only marginally. The classification report also shows that recall for the minority class (class 0) remains low at _0.256_, illustrating that bagging primarily reduces variance rather than fundamentally changing class separation.

In the next section, we extend this idea further by allowing trees to influence each other during training using _boosting_, which explicitly targets both variance and bias rather than averaging independent models.

---

## 14.4 Random Forests

Bagging reduces variance by averaging many high-variance models trained on resampled data. Random forests extend this idea further by also injecting randomness into the feature selection process at each split.

This additional randomness decorrelates the trees, making the average prediction more stable and often more accurate than bagging alone. In practice, random forests are one of the strongest general-purpose classification algorithms available.

Why does feature randomness improve ensemble performance? When all trees in a bagging ensemble consider the same set of features at each split, they tend to identify the same strong predictors and make similar decisions. This correlation means that when one tree makes an error, other trees are likely to make the same error, reducing the benefit of averaging. By randomly restricting which features each tree can consider at each split, random forests force trees to explore different decision boundaries. Some trees may focus on one set of features while others focus on different features, creating diversity in their predictions. When these decorrelated trees are averaged, their different mistakes partially cancel out, leading to more stable and often more accurate predictions than bagging alone.

#### Key ideas behind random forests

- Each tree is trained on a bootstrap sample of the training data.
- At every split, only a random subset of features is considered.
- This feature randomness reduces correlation between trees.
- Predicted class probabilities are averaged across all trees.

If all trees see the same strong predictors at every split, they tend to make similar mistakes. Random feature selection forces trees to explore different decision boundaries, improving generalization.

#### Training a random forest classifier

We now train a random forest using the same preprocessing pipeline and data split as the baseline and bagging models. This allows a clean comparison of performance and probability quality.

```python
from sklearn.ensemble import RandomForestClassifier

rf_model = Pipeline(steps=[
  ("prep", preprocessor),
  ("rf", RandomForestClassifier(
      n_estimators=200,
      max_depth=None,
      min_samples_leaf=1,
      random_state=27,
      n_jobs=-1
  ))
])

rf_model.fit(X_train, y_train)
```

The random forest model is controlled by several key hyperparameters. _n_estimators_ specifies how many decision trees are trained in parallel; larger values usually improve stability and accuracy but increase training time. _max_depth_ limits how deep each tree can grow, with _None_ allowing fully grown trees that capture complex interactions. _min_samples_leaf_ sets the minimum number of observations required in a leaf node and acts as a regularization control to prevent overly specific splits. Finally, _n_jobs = -1_ enables parallel training across all available CPU cores to significantly speed up fitting.

Unlike single decision trees, random forests intentionally introduce randomness through bootstrap sampling of rows and random selection of feature subsets at each split. Because this randomness is fundamental to how the algorithm reduces correlation between trees, the model does not rely on a fixed random seed for correctness or performance. Setting _random_state_ is therefore optional and is used only to make results exactly reproducible for teaching or experimentation. By default, random forests grow deep trees and rely on averaging across many diverse models to control overfitting, which often produces strong accuracy but slightly less well-calibrated probabilities than simpler linear models.

#### Evaluating performance

We evaluate the model using accuracy, log loss, and a full classification report. Log loss is especially important for credit risk modeling because decisions depend on probability thresholds rather than only class labels. For example, a lender may approve loans with default probability below 5%, deny loans above 20%, and require manual review for probabilities in between. Accurate probability estimates are therefore critical for making these threshold-based decisions.

```python
from sklearn.metrics import accuracy_score, log_loss, classification_report

y_rf_pred = rf_model.predict(X_test)
y_rf_prob = rf_model.predict_proba(X_test)

rf_acc = accuracy_score(y_test, y_rf_pred)
rf_ll = log_loss(y_test, y_rf_prob)

print("Random forest accuracy:", round(rf_acc, 4))
print("Random forest log loss:", round(rf_ll, 4))
print("\nClassification report:")
print(classification_report(y_test, y_rf_pred, digits=3, zero_division=0))

# Output:
#  Random forest accuracy: 0.9213
#  Random forest log loss: 0.1944

#  Classification report:
#                precision    recall  f1-score   support

#                0      1.000     0.083     0.154       180
#                1      0.921     1.000     0.959      1916

#           accuracy                          0.921      2096
#         macro avg      0.960     0.542     0.556      2096
#       weighted avg      0.928     0.921     0.890      2096
```

The random forest achieves a test-set accuracy of _0.9213_ and a log loss of _0.1944_. While the accuracy is slightly lower than the single decision tree baseline (_0.9342_), the random forest achieves substantially better probability quality, with log loss improving from _0.2156_ to _0.1944_. This confirms that averaging many decorrelated trees improves probability calibration and model stability, even when overall classification accuracy changes only marginally.

The log loss of _0.1944_ indicates that the model’s probability estimates are reasonably well calibrated, though not as strong as those produced by linear models in earlier sections. This reflects a common tradeoff with tree-based ensembles: higher predictive power but slightly less stable probability estimates.

Examining the classification report reveals an important limitation that is hidden by the high accuracy. For the minority class (class _0_), recall is only _0.083_, meaning that the model correctly identifies fewer than 9 percent of truly bad loans.

At the same time, precision for the minority class is _1.000_, which means that when the model does predict a loan as bad, it is almost always correct. This behavior indicates an extremely conservative decision boundary that avoids false positives but misses most risky borrowers.

For the majority class (class _1_), recall is _1.000_ and precision is _0.921_, showing that the model nearly always labels good loans correctly. As a result, overall accuracy is dominated by performance on the majority class.

The macro-average recall of _0.542_ highlights this imbalance, because it weights both classes equally and exposes how poorly the minority class is detected. In contrast, the weighted-average F1 score of _0.890_ remains high because it is driven by the much larger good-loan population.

From a business perspective, this model would substantially reduce false alarms but would fail to flag most high-risk borrowers. Whether this tradeoff is acceptable depends on the cost of missed defaults versus the cost of rejecting safe customers.

#### Confusion matrix

The confusion matrix highlights how errors are distributed across the good and bad loan classes. This is critical for understanding whether the model sacrifices minority-class detection for higher overall accuracy.

```python
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

cm_rf = confusion_matrix(y_test, y_rf_pred)

plt.figure(figsize=(5, 4))
ConfusionMatrixDisplay(cm_rf, display_labels=["bad", "good"]).plot(values_format="d", cmap='Blues')
plt.title("Random Forest Confusion Matrix")
plt.tight_layout()
plt.show()
```

![Confusion matrix for the random forest model. The matrix shows the number of true negatives (TN), false positives (FP), false negatives (FN), and true positives (TP) for the bad and good loan classes.](../Images/Chapter14_images/random_forest_cm.png)

The confusion matrix shows that the random forest correctly identifies _1,916_ good loans but only _15_ bad loans. At the same time, it misclassifies _165_ bad loans as good and never incorrectly flags a good loan as bad.

This confirms that the model is extremely conservative when predicting the bad class. It avoids false alarms entirely, but at the cost of missing most risky borrowers, which explains the low recall observed earlier for the minority class.

#### Feature importance in random forests

Random forests provide built-in estimates of feature importance based on how much each variable reduces impurity across all trees. These values are known as mean decrease in impurity scores.

Although not causal, feature importance is often useful for model validation and business interpretation. It can reveal which borrower characteristics most strongly influence predicted default risk.

```python
import pandas as pd
import numpy as np

rf_estimator = rf_model.named_steps["rf"]
feature_names = rf_model.named_steps["prep"].get_feature_names_out()

importances = pd.DataFrame({
  "feature": feature_names,
  "importance": rf_estimator.feature_importances_
}).sort_values(by="importance", ascending=False)

top_features = importances.head(15)
top_features
```

![Feature importance for the random forest model. The features are sorted by importance score, with the most important features at the top.](../Images/Chapter14_images/random_forest_importance.png)

Feature importances from a random forest reflect which inputs most reduced impurity across the forest’s splits. In other words, higher values indicate variables that the model relied on more often and more strongly to separate loans that ended in good standing from loans that became bad.

The most important signals are concentrated in repayment and balance variables, such as _total_rec_prncp_ (0.0813) and _total_pymnt_ (0.0570). These features capture whether the borrower has already paid back principal and how much total payment has been received, which naturally aligns with the model’s goal of distinguishing loans that are performing well from loans that are not.

Time and “age of loan” also matter: _issue_age_days_ (0.0714) ranks second. This suggests the model uses loan seasoning as an important context variable, because the risk profile and repayment pattern of a newly issued loan can differ from one that has been active much longer.

Contract terms and pricing variables—such as _installment_ (0.0414), _int_rate_ (0.0389), and _loan_amnt_ (0.0342)—are also influential. Operationally, this implies that payment burden and loan pricing are meaningful drivers of outcomes, and that risk segmentation can often be improved by considering affordability proxies like payment size and debt ratios.

Borrower leverage and revolving-credit behavior show up through _dti_ (0.0299), _revol_util_ (0.0295), _revol_bal_ (0.0284), and _total_rev_hi_lim_ (0.0278). These features support decisions about monitoring and intervention, because high utilization or high revolving balances can indicate liquidity strain that increases default risk.

Portfolio exposure and capacity indicators—such as _tot_cur_bal_ (0.0298), _annual_inc_ (0.0288), and _total_acc_ (0.0244)—suggest the model considers broader financial context beyond the focal loan. In practice, these can inform policy rules or review thresholds (for example, requiring additional documentation or manual review when high utilization coincides with weaker income or thinner credit depth).

Two important cautions apply when interpreting this list. First, impurity-based importances can be biased toward continuous variables and toward features with many potential split points, so the ranking is best treated as a screening tool rather than a definitive causal claim.

Second, several top features (for example, _total_rec_prncp_ and _total_pymnt_) may be “outcome-adjacent” because they reflect repayment progress after the loan is issued. If your business goal is early risk prediction at origination, you should exclude post-origination variables and recompute importances using only information available at decision time.

Additional caveats apply when interpreting feature importance rankings:

- **One-hot encoding can inflate importance of categorical features:** When a categorical variable with many levels is one-hot encoded, it creates multiple binary features. Random forests may split on several of these binary features, and the sum of their individual importances can make the original categorical variable appear more important than it actually is. To get a fair comparison, sum the importances of all one-hot encoded columns that belong to the same original categorical feature.
- **Impurity-based vs. permutation importance:** Random forests report _impurity-based_ importance (mean decrease in impurity), which measures how much each feature reduces node impurity across all splits. This is fast to compute but can be biased toward high-cardinality features. _Permutation importance_ (available in scikit-learn via _permutation_importance_) measures how much model performance degrades when a feature's values are randomly shuffled. Permutation importance is more reliable for comparing features fairly but requires refitting or scoring the model multiple times, making it computationally expensive.
- **When to trust vs. question feature importance rankings:** Trust rankings when (1) features are preprocessed consistently (no one-hot encoding artifacts), (2) you have sufficient data (importance estimates stabilize with more trees and more data), and (3) the ranking aligns with domain knowledge. Question rankings when (1) features are highly correlated (importance can be arbitrarily distributed among correlated features), (2) sample size is small (estimates are noisy), or (3) the ranking contradicts domain expertise (may indicate data quality issues or model problems).
- **Importance rankings are not consistent across model types:** Different algorithms can produce different importance rankings for the same features, even on the same data. A random forest may rank features differently from a gradient boosting model because each algorithm learns different tree structures and uses different splitting strategies. Logistic regression coefficients capture yet another perspective—linear association strength—which may not align with tree-based importance at all. For this reason, treat any single model’s importance ranking as one view of the data rather than a definitive ordering, and compare rankings across model types when making high-stakes feature decisions.

A practical decision workflow is to use this ranking to (1) identify a small set of high-signal variables for monitoring dashboards, (2) flag potential data leakage candidates to remove when building a true pre-origination model, and (3) guide feature engineering by creating clearer affordability and revolving-credit measures that capture the same underlying risk patterns.

#### Visualizing the most important features

Plotting the top features helps translate the model into business-relevant insights. Highly ranked variables often correspond to borrower risk, loan size, and repayment capacity.

```python
import matplotlib.pyplot as plt

plt.figure(figsize=(7, 5))
plt.barh(top_features["feature"][::-1], top_features["importance"][::-1])
plt.title("Top Random Forest Feature Importances")
plt.xlabel("Importance score")
plt.tight_layout()
plt.show()
```

![Plot of the top random forest feature importances. The features are sorted by importance score, with the most important features at the top.](../Images/Chapter14_images/random_forest_plot.png)

Random forests typically outperform single trees in both accuracy and log loss because they reduce variance while preserving nonlinear decision boundaries. However, they are slower to train and less interpretable than individual trees.

In the next section, we move from parallel ensembles to sequential ensembles using boosting, where models actively learn from previous mistakes rather than operating independently.

Before we move on, let's see how random forest compares to our prior algorithm results.

```python
# Assumes these already exist from earlier code blocks:
# rf_acc, rf_ll (computed in your metrics block)
# results_df (created earlier and already contains prior model rows)

new_row = pd.DataFrame([{
  "model": "Random Forest",
  "accuracy": rf_acc,
  "log_loss": rf_ll
}])

# If the model already exists in the table, replace it; otherwise append it.
if (results_df["model"] == "Random Forest").any():
  results_df.loc[results_df["model"] == "Random Forest", ["accuracy", "log_loss"]] = [rf_acc, rf_ll]
else:
  results_df = pd.concat([results_df, new_row], ignore_index=True)

# Re-sort using your same rule
results_df = results_df.sort_values(by=["log_loss", "accuracy"], ascending=[True, False])

results_df
```

As you can see, logistic regression is the best model overall, followed by random forest and then bagging based on log loss.

---

## 14.5 Boosting with AdaBoost

Bagging and random forests reduce error by training many models independently and averaging their predictions. Boosting takes a different approach by training models _sequentially_, where each new model focuses on correcting the mistakes made by the previous ones.

The most widely used classical boosting algorithm is **AdaBoost** (Adaptive Boosting). Instead of treating all observations equally, AdaBoost increases the importance of misclassified examples so that later models concentrate on the hardest cases.

#### Core ideas behind boosting

- **Sequential learning:** models are trained one after another, not in parallel.
- **Reweighting mistakes:** misclassified observations receive higher weights.
- **Weak learners:** each model is intentionally simple, often a shallow decision tree.

By combining many weak learners that are each only slightly better than random guessing, boosting constructs a strong classifier that can reduce both bias and variance.

#### AdaBoost algorithm overview

#### AdaBoost in scikit-learn

Scikit-learn implements AdaBoost using the _AdaBoostClassifier_ class. We again reuse the same cleaned dataset, preprocessing pipeline, and train/test split so that performance can be compared directly with bagging and random forests.

```python
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier

base_tree = DecisionTreeClassifier(
  max_depth=1,
  random_state=27
)

model_adaboost = Pipeline(steps=[
  ("prep", preprocessor),
  ("ada", AdaBoostClassifier(
      estimator=base_tree,
      n_estimators=200,
      learning_rate=0.5,
      random_state=27
  ))
])

model_adaboost.fit(X_train, y_train)
```

The base learner is intentionally restricted to a decision stump (_max_depth = 1_) so that each individual model is weak. The hyperparameter _n_estimators_ controls how many boosting rounds are performed, while _learning_rate_ determines how strongly each new learner influences the final model.

Smaller learning rates slow down learning but often improve generalization, while larger values can lead to faster convergence but higher risk of overfitting.

#### Comparing boosting to bagging and random forests

Bagging and random forests reduce error primarily by averaging many independent models to stabilize predictions. AdaBoost instead reduces error by repeatedly focusing on the hardest-to-classify observations.

In practice, boosting often achieves higher accuracy than bagging when patterns are subtle and nonlinear, but it can be more sensitive to noisy labels because mislabeled observations receive increasing weight over time.

The next ensemble method extends this idea further by replacing reweighting with direct optimization of a loss function using gradient descent, leading to gradient boosting models.

#### Deliverable: Add AdaBoost to the running model comparison table

To compare boosting fairly, we evaluate AdaBoost using the same train/test split, the same preprocessing pipeline, and the same evaluation function used for earlier models. We then append AdaBoost results to the running comparison table so we can see whether boosting improved accuracy, probability quality (log loss), or both.

```python
def eval_model_row(name, model, X_train, y_train, X_test, y_test):
  model.fit(X_train, y_train)

  y_pred = model.predict(X_test)
  acc = accuracy_score(y_test, y_pred)

  ll = None
  if hasattr(model, "predict_proba"):
    y_prob = model.predict_proba(X_test)
    ll = log_loss(y_test, y_prob)

  return {"model": name, "accuracy": acc, "log_loss": ll}

row = eval_model_row(
  "AdaBoost (stumps, n=200)",
  model_adaboost,
  X_train, y_train,
  X_test, y_test
)

# Upsert into the existing results_df (replace if present, otherwise append)
if (results_df["model"] == row["model"]).any():
  results_df.loc[results_df["model"] == row["model"], ["accuracy", "log_loss"]] = [row["accuracy"], row["log_loss"]]
else:
  results_df = pd.concat([results_df, pd.DataFrame([row])], ignore_index=True)

# Re-sort using the same rule used throughout the chapter
results_df = results_df.sort_values(by=["log_loss", "accuracy"], ascending=[True, False])

results_df
```

![Table comparing classification models with columns for model name, accuracy, and log loss. Logistic regression (OvR) has accuracy 0.954 and log loss 0.130. Random forest has accuracy 0.921 and log loss 0.194. Bagging with 100 trees (depth 3) has accuracy 0.936 and log loss 0.195. A single decision tree (depth 3) has accuracy 0.934 and log loss 0.216. AdaBoost with 200 stumps has accuracy 0.951 and log loss 0.497. k-NN (k=15) has accuracy 0.916 and log loss 0.569. Naive Bayes has accuracy 0.224 and log loss 27.979, indicating very poor probability calibration.](../Images/Chapter14_images/adaboost_results_df.png)

In this experiment, AdaBoost achieves an accuracy of _0.9509_, which is slightly higher than the single decision tree (_0.9342_), bagging (_0.9356_), and the random forest (_0.9213_). This reflects boosting’s ability to reduce classification error by concentrating successive models on the observations that earlier models misclassified.

However, AdaBoost’s log loss is _0.4970_, which is substantially worse than bagging (_0.1949_), random forests (_0.1944_), and logistic regression (_0.1296_). This indicates that although AdaBoost predicts the correct class more often, its probability estimates are less well calibrated and tend to be more overconfident on difficult cases. The mechanism behind this overconfidence is AdaBoost’s reweighting strategy: as the algorithm repeatedly increases weights on misclassified observations, later trees become increasingly focused on correcting these hard cases. This can cause the ensemble to assign extreme probabilities (very close to 0 or 1) to difficult observations, which hurts log loss even when the final class prediction is correct.

This contrast highlights a key tradeoff: boosting can deliver strong accuracy gains, but not necessarily better probability quality. The next ensemble method addresses this limitation by directly optimizing a loss function using gradient descent, which often improves both accuracy and probability calibration.

---

## 14.6 Gradient Boosting

![Diagram illustrating gradient boosting for classification. Models are trained sequentially rather than in parallel. The first tree makes an initial prediction, and each subsequent tree is trained on the residual errors from the previous model. Arrows show how errors are passed forward and gradually reduced. The final prediction is formed by combining the weighted outputs of all trees, highlighting how small corrective models accumulate to improve accuracy and probability estimates over time.](../Images/Chapter14_images/gb_concept.png)

Boosting methods build strong predictors by combining many weak models in sequence. Instead of training many models independently and averaging them, gradient boosting trains each new model to correct the errors made by the ensemble so far.

For classification, the “errors” are defined by a loss function such as log loss, not just whether a case was misclassified. This makes gradient boosting especially valuable when you care about probability quality, not only the final predicted label.

#### How gradient boosting works

Gradient boosting can be understood as a repeated cycle of “predict, measure loss, and correct.” Each new tree is trained on the residual signal that remains after the current ensemble has already done its best.

- Start with a simple baseline prediction for every case. For classification, this often begins with the overall class rate (a constant probability).
- Compute how wrong the current model is using a loss function such as log loss. These loss gradients indicate which cases the model is currently under- or over-confident about.
- Train a shallow decision tree to predict the direction and size of the correction needed. This tree is intentionally weak so it improves the ensemble gradually.
- Scale the tree’s contribution using a learning rate. Smaller learning rates usually improve stability but require more trees.
- Add the scaled correction into the ensemble and repeat many times. Over iterations, many small fixes can produce a highly accurate model.

Compared to AdaBoost, gradient boosting is more general because it is framed as direct optimization of a loss function. That framing makes it easier to add regularization, tune probability quality, and extend the algorithm in production-grade systems.

#### Gradient boosting in scikit-learn

The code below trains a _GradientBoostingClassifier_ using the same training data, preprocessing pipeline, and evaluation objects already created earlier. This keeps the comparison fair because differences in performance are driven by the algorithm rather than by changes in features, splits, or data preparation.

```python
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, log_loss, classification_report

model_gbdt = Pipeline(steps=[
  ("prep", preprocessor),
  ("gbdt", GradientBoostingClassifier(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=3,
    subsample=1.0,
    random_state=27
  ))
])

model_gbdt.fit(X_train, y_train)

y_gbdt_pred = model_gbdt.predict(X_test)
y_gbdt_prob = model_gbdt.predict_proba(X_test)

gbdt_acc = accuracy_score(y_test, y_gbdt_pred)
gbdt_ll = log_loss(y_test, y_gbdt_prob)

print("GBDT accuracy:", round(gbdt_acc, 4))
print("GBDT log loss:", round(gbdt_ll, 4))
print("\nClassification report:")
print(classification_report(y_test, y_gbdt_pred, digits=3, zero_division=0))

# Output:
# GBDT accuracy: 0.968
# GBDT log loss: 0.0942

# Classification report:
#               precision    recall  f1-score   support

#           0      0.991     0.633     0.773       180
#           1      0.967     0.999     0.983      1916

#     accuracy                          0.968      2096
#   macro avg      0.979     0.816     0.878      2096
# weighted avg      0.969     0.968     0.965      2096
```

#### Key hyperparameters for GradientBoostingClassifier

Gradient boosting performance is driven by a small set of hyperparameters that control how many trees you add and how aggressively each one updates the ensemble. The goal is to add enough trees to learn meaningful patterns, but not so aggressively that the model becomes overconfident or overfits noise.

In this chapter workflow, you can think of _learning_rate_ and _n_estimators_ as a pair. A smaller learning rate generally calls for a larger number of trees, which can improve probability quality when tuned carefully.

So far, we have used scikit-learn’s _GradientBoostingClassifier_, which provides a clear and educational implementation of gradient boosting. However, in practice, many data scientists use specialized libraries such as XGBoost, LightGBM, or CatBoost. These are not different algorithms—they are optimized implementations of gradient boosting that add advanced regularization techniques, better computational efficiency, and more sophisticated hyperparameter controls. XGBoost (eXtreme Gradient Boosting) is perhaps the most widely used of these libraries and represents the same core gradient boosting idea we just learned, but with engineering improvements that often lead to better performance and faster training.

#### XGBoost vs. scikit-learn GradientBoostingClassifier

Scikit-learn’s gradient boosting is an excellent educational implementation, but modern production systems often use optimized libraries such as XGBoost. XGBoost is still gradient boosting, but it adds engineering and regularization improvements that often lead to better accuracy, better probability control, and much faster training on large datasets.

#### XGBoost in Python

The next code cell trains an XGBoost classifier using the same training and test data objects. The configuration below is intentionally conservative so it trains quickly and produces stable probabilities for comparison, and you can tune it later if needed.

```python
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, log_loss, classification_report

model_xgb = Pipeline(steps=[
  ("prep", preprocessor),
  ("xgb", XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    n_estimators=400,
    learning_rate=0.05,
    max_depth=3,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_lambda=1.0,
    min_child_weight=1.0,
    gamma=0.0,
    n_jobs=-1,
    random_state=27
  ))
])

model_xgb.fit(X_train, y_train)

y_xgb_pred = model_xgb.predict(X_test)
y_xgb_prob = model_xgb.predict_proba(X_test)

xgb_acc = accuracy_score(y_test, y_xgb_pred)
xgb_ll = log_loss(y_test, y_xgb_prob)

print("XGBoost accuracy:", round(xgb_acc, 4))
print("XGBoost log loss:", round(xgb_ll, 4))
print("\nClassification report:")
print(classification_report(y_test, y_xgb_pred, digits=3, zero_division=0))

# Output:
# XGBoost accuracy: 0.9828
# XGBoost log loss: 0.0539

# Classification report:
#               precision    recall  f1-score   support

#           0      0.993     0.806     0.890       180
#           1      0.982     0.999     0.991      1916

#     accuracy                          0.983      2096
#   macro avg      0.988     0.903     0.940      2096
# weighted avg      0.983     0.983     0.982      2096
```

#### Key hyperparameters for XGBClassifier

XGBoost exposes additional hyperparameters that control tree complexity and regularization more explicitly. These settings help you balance accuracy with generalization and also help prevent overly confident probability predictions.

#### Adding both models to the cumulative comparison table

To keep your model comparison table cumulative, do not rebuild it from scratch. Instead, append new rows into the existing _results_df_, then re-sort by log loss and accuracy so the best probability models rise to the top.

```python
import numpy as np

def append_result_row(results_df, model_name, acc, ll):
  results_df.loc[len(results_df)] = {
    "model": model_name,
    "accuracy": float(acc),
    "log_loss": float(ll) if ll is not None and not np.isnan(ll) else np.nan
  }
  results_df = results_df.sort_values(
    by=["log_loss", "accuracy"],
    ascending=[True, False]
  ).reset_index(drop=True)
  return results_df

results_df = append_result_row(results_df, "Gradient Boosting (GBDT)", gbdt_acc, gbdt_ll)
results_df = append_result_row(results_df, "XGBoost (XGBClassifier)", xgb_acc, xgb_ll)

results_df
```

### Interpreting the Gradient Boosting Results

The updated comparison table shows that gradient boosting methods produced the strongest overall performance on this Lending Club prediction task. The best model by both accuracy and log loss is **XGBoost (XGBClassifier)**, with _accuracy = 0.9828_ and _log loss = 0.0539_, indicating both excellent classification performance and very high-quality probability estimates.

The scikit-learn **Gradient Boosting (GBDT)** model also performs extremely well with _accuracy = 0.9680_ and _log loss = 0.0942_. This places it ahead of every earlier model in the chapter, including the best single-model baseline, _logistic regression_ (_accuracy = 0.9542_, _log loss = 0.1296_), which had previously been the top performer for probability quality.

A key takeaway is that gradient boosting improves both goals simultaneously: it increases accuracy and also lowers log loss. For example, compared to logistic regression, GBDT improves accuracy by about _0.0138_ (0.9680 − 0.9542) while reducing log loss by about _0.0354_ (0.1296 − 0.0942), and XGBoost improves even further with an additional log loss reduction of about _0.0403_ relative to GBDT (0.0942 − 0.0539).

In contrast, bagging-style methods improve probability quality modestly but do not match boosting on this dataset. _Random Forest_ (_accuracy = 0.9213_, _log loss = 0.1944_) and _Bagging_ (_accuracy = 0.9356_, _log loss = 0.1949_) reduce variance through averaging, but their probability estimates remain less precise than the boosted models, which are explicitly optimizing log loss during training.

AdaBoost’s results illustrate an important caution about probability quality. Even though _AdaBoost (stumps, n=200)_ achieves high accuracy (_0.9509_), its log loss is much worse (_0.4970_), which suggests overconfident or poorly calibrated probabilities for at least some cases.

Overall, the table supports a practical conclusion: when your business decision depends on reliable probability scores (such as pricing risk, prioritizing reviews, or setting thresholds), gradient boosting is often a top choice. Among gradient boosting implementations, XGBoost frequently leads because it adds regularization and optimization features that improve generalization and probability behavior, which is consistent with its strong showing here.

---

## 14.7 Stacking

![Diagram illustrating model stacking (meta-learning). Training data is fed into three different base models (logistic regression, random forest, and k-nearest neighbors), each producing probability predictions. These predictions are combined into a new dataset that is passed to a meta-model (logistic regression), which learns how to weight the base models’ outputs. The meta-model produces the final prediction, shown with a check mark to indicate improved performance.](../Images/Chapter14_images/stacking_concept.png)

Bagging and boosting combine models by averaging or sequentially correcting errors. **Stacking** (also called **meta-learning**) combines models by training a second model to learn how to best mix their predictions.

The idea is to treat each base model as a feature generator. Each base model makes a prediction (often a probability), and a **meta-model** learns how to weight those predictions to make a final decision.

#### Why stacking can outperform single models

Different algorithms tend to make different kinds of mistakes. A stacking ensemble can learn patterns like “trust the linear model when the signal is mostly additive” and “trust the tree-based model when interactions matter.”

The biggest risk is **information leakage**. If the meta-model is trained on base-model predictions made from the same training rows that fit those base models, the meta-model can overfit by learning overly optimistic predictions.

To prevent leakage, stacking uses **out-of-fold predictions**: the base models generate predictions for each training row only when that row was held out from the base model’s training fold. Scikit-learn handles this automatically.

#### Stacking in scikit-learn

In this example, we reuse the same train/test split and the same _preprocessor_ pipeline. We will combine three base learners (logistic regression, random forest, and k-NN) and then train a logistic regression meta-model on their predicted probabilities.

```python
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline

base_lr = LogisticRegression(
  solver="liblinear",
  max_iter=2000,
  random_state=27
)

base_rf = RandomForestClassifier(
  n_estimators=200,
  max_depth=None,
  min_samples_leaf=1,
  n_jobs=-1,
  random_state=27
)

base_knn = KNeighborsClassifier(n_neighbors=15)

meta_lr = LogisticRegression(
  solver="liblinear",
  max_iter=2000,
  random_state=27
)

model_stacking = Pipeline(steps=[
  ("prep", preprocessor),
  ("stack", StackingClassifier(
      estimators=[
        ("lr", base_lr),
        ("rf", base_rf),
        ("knn", base_knn)
      ],
      final_estimator=meta_lr,
      stack_method="predict_proba",
      cv=5,
      n_jobs=-1,
      passthrough=False
  ))
])

model_stacking.fit(X_train, y_train)
```

#### Interpreting the key hyperparameters

The _estimators_ argument defines the level-0 learners. Each is trained using cross-validation folds so that the meta-model sees out-of-fold predictions rather than in-sample predictions.

The _final_estimator_ is the meta-model. We use logistic regression because it is fast, stable, and produces well-behaved probability estimates. Conceptually, it learns how to weight base-model probabilities to improve decisions.

The _stack_method="predict_proba"_ option tells stacking to feed predicted probabilities (not class labels) into the meta-model. This is usually preferred when you care about log loss and probability quality.

The _cv_ argument controls how out-of-fold predictions are generated. More folds can reduce leakage risk and stabilize the meta-features but increases training time because base learners are refit multiple times.

Setting _passthrough=False_ means the meta-model uses only base-model outputs. If you set it to True, the original features are also passed to the meta-model, which can improve performance but increases overfitting risk and reduces interpretability.

#### Adding stacking results to the chapter comparison table

Next, we evaluate the stacking model on the test set and add its accuracy and log loss as a new row in the existing _results_df_ so we can compare it to every prior model using the same evaluation setup.

```python
from sklearn.metrics import accuracy_score, log_loss

y_stack_pred = model_stacking.predict(X_test)
stack_acc = accuracy_score(y_test, y_stack_pred)

stack_ll = None
if hasattr(model_stacking, "predict_proba"):
  y_stack_prob = model_stacking.predict_proba(X_test)
  stack_ll = log_loss(y_test, y_stack_prob)

row = {"model": "Stacking (LR + RF + kNN → LR)", "accuracy": stack_acc, "log_loss": stack_ll}

if "results_df" not in globals():
  results_df = pd.DataFrame([row])
else:
  results_df = pd.concat([results_df, pd.DataFrame([row])], ignore_index=True)

results_df = results_df.sort_values(by=["log_loss", "accuracy"], ascending=[True, False]).reset_index(drop=True)
results_df
```

![Table showing the performance of the stacking model on the Lending Club dataset. Columns are model name, accuracy, and log loss. The stacking model has accuracy 0.9828 and log loss 0.0539, the best overall.](../Images/Chapter14_images/stacking_results_df.png)

In this run, the stacking ensemble (_Stacking (LR + RF + kNN → LR)_) achieved a test-set accuracy of _0.9561_ with a log loss of _0.1363_. This places stacking slightly above the standalone logistic regression model in accuracy (_0.9542_) but with a slightly worse log loss than logistic regression (_0.1296_), meaning stacking made marginally fewer classification mistakes at the 0.50 threshold but produced slightly less well-calibrated probabilities overall.

Compared to variance-reduction methods, stacking is clearly competitive. It outperforms random forests (_0.9213_ accuracy, _0.1944_ log loss) and bagging (_0.9356_ accuracy, _0.1949_ log loss) on both metrics, suggesting that combining diverse base learners can improve both classification performance and probability quality. It also substantially improves on a single shallow tree (_0.9342_ accuracy, _0.2156_ log loss), reinforcing the idea that ensembles can stabilize weak or high-variance learners without requiring deeper trees.

However, the strongest probability models in this comparison are still the gradient boosting family. The gradient boosting tree model (_0.9680_ accuracy, _0.0942_ log loss) and especially XGBoost (_0.9828_ accuracy, _0.0539_ log loss) outperform stacking by a meaningful margin. This indicates that, for this dataset, sequential boosting is capturing important nonlinear structure and producing better-calibrated risk estimates than a meta-learner combining a small set of heterogeneous models.

Operationally, the stacking result is still valuable because it demonstrates a practical middle ground: it provides strong accuracy and good log loss without requiring a specialized boosting library, and it offers a simple “mixture of experts” intuition (the meta-model learns which base learner to trust in different regions of the feature space). But if the business objective prioritizes probability quality for ranking, pricing, or threshold tuning, the lower log loss values from gradient boosting methods (especially _0.0539_ for XGBoost) provide a strong argument for choosing boosting over stacking in this particular setting.

When is stacking most beneficial? Stacking tends to provide the greatest value in the following scenarios:

- When base learners make complementary errors—that is, when different models make mistakes on different observations, allowing the meta-learner to combine their strengths.
- When you have diverse model families (linear + tree-based + distance-based) that capture different aspects of the data structure, rather than multiple similar models.
- When probability quality matters more than training speed, since stacking requires refitting base learners multiple times during cross-validation, which can be computationally expensive.

If stacking takes too long, reduce _cv_ (for example, from 5 to 3) or simplify base learners (fewer random forest trees, smaller k for k-NN). Stacking refits base learners multiple times, so training time can increase quickly.

Conversely, when you have more data and want better out-of-fold predictions for the meta-learner, consider increasing _cv_ (for example, from 5 to 10). More folds provide more training data for the meta-learner and can improve its ability to learn how to combine base learners effectively, though at the cost of longer training time.

---

## 14.8 Ensembles for Regression

All of the ensemble concepts we have covered in this chapter—bagging, random forests, boosting, and stacking—apply equally well to regression problems. The core principles remain the same: combining multiple models reduces variance, improves stability, and often leads to better predictions. The main difference is that regression ensembles average numeric predictions (such as predicted prices, sales volumes, or continuous outcomes) rather than combining class probabilities. This means that instead of voting on class labels or averaging probabilities, regression ensembles typically compute the mean or weighted mean of the numeric predictions from each base model.

In classification, ensembles usually combine predicted probabilities and then choose the most likely class. In regression, ensembles combine predicted numeric values, often by averaging (bagging/random forests) or by adding incremental corrections (boosting).

To illustrate regression ensembles in a realistic business case, we will reuse the **insurance** dataset from earlier regression work. The goal is to predict _charges_ (annual medical insurance cost) from customer attributes such as age, BMI, smoking status, and region.

For regression, our evaluation metrics change. We will use _RMSE_ to measure typical prediction error in dollars, _MAE_ to measure absolute error robustness, and _R²_ to measure variance explained.

#### Step 1: Load data, split train/test, and build a reusable preprocessing pipeline

```python
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")

y = df["charges"].copy()
X = df.drop(columns=["charges"]).copy()

X_train, X_test, y_train, y_test = train_test_split(
  X, y,
  test_size=0.20,
  random_state=27
)

cat_cols = X_train.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
num_cols = X_train.select_dtypes(include=["number"]).columns.tolist()

num_pipe = Pipeline(steps=[
  ("imp", SimpleImputer(strategy="median")),
  ("scaler", StandardScaler())
])

cat_pipe = Pipeline(steps=[
  ("imp", SimpleImputer(strategy="most_frequent")),
  ("ohe", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
  transformers=[
    ("num", num_pipe, num_cols),
    ("cat", cat_pipe, cat_cols)
  ],
  remainder="drop"
)
```

The preprocessing pipeline handles missing data through imputation and converts categorical attributes into one-hot encoded features. Scaling is applied to numeric features to support algorithms like k-NN and linear regression, while tree-based ensembles will typically be insensitive to the scaling step.

#### Step 2: Define a reusable evaluation function and a results table

```python
import time

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

results_reg_df = pd.DataFrame(columns=["model", "rmse", "mae", "r2", "train_seconds"])

def upsert_result_row_reg(results_df, row):
  if (results_df["model"] == row["model"]).any():
    idx = results_df.index[results_df["model"] == row["model"]][0]
    for k, v in row.items():
      results_df.at[idx, k] = v
  else:
    results_df = pd.concat([results_df, pd.DataFrame([row])], ignore_index=True)

  results_df = results_df.sort_values(
    by=["rmse", "r2"],
    ascending=[True, False]
  ).reset_index(drop=True)

  return results_df

def eval_and_update_results_df_reg(model_name, model, X_train, y_train, X_test, y_test, results_df):
  t0 = time.perf_counter()
  model.fit(X_train, y_train)
  train_seconds = time.perf_counter() - t0

  y_pred = model.predict(X_test)

  rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
  mae = float(mean_absolute_error(y_test, y_pred))
  r2 = float(r2_score(y_test, y_pred))

  row = {
    "model": model_name,
    "rmse": rmse,
    "mae": mae,
    "r2": r2,
    "train_seconds": train_seconds
  }

  results_df = upsert_result_row_reg(results_df, row)
  return results_df
```

RMSE and MAE are measured in the same units as the label, which makes them easy to interpret in business terms. R² is dimensionless and summarizes the share of variation in charges explained by the model.

#### Step 3: Train baseline regressors and ensemble regressors

The models below mirror the families we used for classification, but in regression form. After running the cell, you will have a single comparison table that includes both single-model baselines and ensembles.

```python
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.neighbors import KNeighborsRegressor

from sklearn.ensemble import BaggingRegressor, RandomForestRegressor, AdaBoostRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import StackingRegressor

model_lr_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("lr", LinearRegression())
])

model_tree_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("tree", DecisionTreeRegressor(
    max_depth=3,
    random_state=27
  ))
])

model_knn_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("knn", KNeighborsRegressor(n_neighbors=15))
])

base_tree_reg = DecisionTreeRegressor(
  max_depth=3,
  random_state=27
)

model_bag_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("bag", BaggingRegressor(
    estimator=base_tree_reg,
    n_estimators=200,
    bootstrap=True,
    n_jobs=-1,
    random_state=27
  ))
])

model_rf_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("rf", RandomForestRegressor(
    n_estimators=300,
    max_depth=None,
    min_samples_leaf=1,
    n_jobs=-1,
    random_state=27
  ))
])

model_ada_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("ada", AdaBoostRegressor(
    estimator=DecisionTreeRegressor(max_depth=2, random_state=27),
    n_estimators=300,
    learning_rate=0.05,
    loss="linear",
    random_state=27
  ))
])

model_gbdt_reg = Pipeline(steps=[
  ("prep", preprocessor),
  ("gbdt", GradientBoostingRegressor(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=3,
    random_state=27
  ))
])

stack_base_lr = Pipeline(steps=[
  ("prep", preprocessor),
  ("lr", LinearRegression())
])

stack_base_rf = Pipeline(steps=[
  ("prep", preprocessor),
  ("rf", RandomForestRegressor(
    n_estimators=200,
    n_jobs=-1,
    random_state=27
  ))
])

stack_base_knn = Pipeline(steps=[
  ("prep", preprocessor),
  ("knn", KNeighborsRegressor(n_neighbors=25))
])

model_stack_reg = StackingRegressor(
  estimators=[
    ("lr", stack_base_lr),
    ("rf", stack_base_rf),
    ("knn", stack_base_knn)
  ],
  final_estimator=LinearRegression(),
  cv=5,
  n_jobs=-1
)

try:
  from xgboost import XGBRegressor

  model_xgb_reg = Pipeline(steps=[
    ("prep", preprocessor),
    ("xgb", XGBRegressor(
      n_estimators=600,
      learning_rate=0.05,
      max_depth=4,
      subsample=0.9,
      colsample_bytree=0.9,
      reg_lambda=1.0,
      random_state=27,
      n_jobs=-1
    ))
  ])
except Exception as e:
  model_xgb_reg = None
  print("XGBoost not available in this environment:", e)
```

Stacking is handled slightly differently in regression because we combine predicted numeric values. The key leakage control is _cv=5_, which forces the meta-model to train on out-of-fold predictions rather than predictions produced on the same rows used to train a base learner.

#### Step 4: Add every model to the regression comparison table

```python
models_reg_to_compare = [
  ("Linear regression", model_lr_reg),
  ("Decision tree (depth=3)", model_tree_reg),
  ("k-NN (k=15)", model_knn_reg),
  ("Bagging (200 trees, depth=3)", model_bag_reg),
  ("Random forest", model_rf_reg),
  ("AdaBoost (trees, n=300)", model_ada_reg),
  ("Gradient boosting (GBDT)", model_gbdt_reg),
  ("Stacking (LR + RF + kNN → LR)", model_stack_reg)
]

if model_xgb_reg is not None:
  models_reg_to_compare.append(("XGBoost (XGBRegressor)", model_xgb_reg))

for name, model in models_reg_to_compare:
  results_reg_df = eval_and_update_results_df_reg(
    name, model,
    X_train, y_train,
    X_test, y_test,
    results_reg_df
  )

results_reg_df
```

![Table comparing regression model performance on the insurance charges dataset. Columns include model name, test RMSE, MAE, R-squared, and training time in seconds. Gradient Boosting achieves the best overall accuracy with the lowest RMSE (≈ 4,994), lowest MAE (≈ 2,667), and highest R² (≈ 0.831), with moderate training time (≈ 0.77 s). Bagging and XGBoost follow closely, with slightly higher RMSE values around 5,183 and 5,276, respectively. The shallow decision tree performs competitively (RMSE ≈ 5,294) with very fast training, while stacking performs similarly in accuracy but requires substantially more training time (≈ 5.69 s). Random forest shows weaker accuracy than boosting-based models (RMSE ≈ 5,432) and longer training time. AdaBoost performs worse than other ensembles, with higher error (RMSE ≈ 5,775) and MAE. Linear regression and k-NN perform worst overall, with the highest RMSE values (≈ 6,282 and 6,693) and lowest R² scores. Overall, boosting-based models provide the best trade-off between accuracy and efficiency for this regression task.](../Images/Chapter14_images/rmse_training_df.png)

The results show that ensemble methods outperform single models for predicting insurance charges, with gradient boosting achieving the strongest overall performance.

Gradient boosting produced the lowest test RMSE (4,994) and MAE (2,667) and the highest R² value (0.831), indicating the most accurate and well-calibrated predictions among all models evaluated.

Bagging and XGBoost followed closely, with RMSE values of 5,183 and 5,276 respectively, demonstrating that averaging and boosting both substantially reduce error compared to a single decision tree.

The shallow decision tree performed surprisingly well given its simplicity, but it was still outperformed by all major ensemble approaches, particularly boosting-based models.

Stacking achieved similar accuracy to random forests but required significantly more training time, illustrating a common tradeoff between marginal accuracy gains and computational cost.

AdaBoost improved upon linear regression and k-NN but underperformed compared to newer boosting variants, suggesting sensitivity to noise and limited capacity when modeling complex cost drivers.

Overall, the results reinforce that boosting-based ensembles offer the best balance of predictive accuracy and practical efficiency for structured regression problems such as healthcare cost estimation.

#### Step 5: Visualize RMSE and training time

In real projects, model choice is not only about accuracy. Training time and operational complexity matter, especially when models must be retrained frequently or deployed in cost-sensitive environments.

```python
import matplotlib.pyplot as plt
import seaborn as sns

plot_df = results_reg_df.copy()

sns.set_theme(style="white")

fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(11, 9), sharex=True)

# RMSE plot
sns.barplot(
  data=plot_df,
  x="model",
  y="rmse",
  ax=axes[0]
)
axes[0].set_title("Regression model comparison: RMSE (lower is better)")
axes[0].set_ylabel("Test RMSE")

# Training time plot
sns.barplot(
  data=plot_df,
  x="model",
  y="train_seconds",
  ax=axes[1]
)
axes[1].set_title("Regression model comparison: training time")
axes[1].set_ylabel("Training time (seconds)")
axes[1].set_xlabel("")

sns.despine(top=True, right=True)

plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()
```

![Bar chart with two vertically stacked panels comparing regression models on the insurance dataset. The top panel shows test RMSE (lower is better), where Gradient Boosting has the lowest error (about 4,994), followed by Bagging (about 5,183) and XGBoost (about 5,276), while Linear Regression and k-NN have the highest errors (above 6,200 and 6,600). The bottom panel shows training time in seconds, where Stacking is by far the slowest (about 5.7 seconds), Random Forest is moderate (about 1.5 seconds), Gradient Boosting and Bagging are under one second, and Linear Regression, k-NN, and a single decision tree train almost instantly.](../Images/Chapter14_images/rmse_training_plot.png)

The RMSE comparison shows a clear performance advantage for boosting-based ensemble methods on the insurance cost prediction task.

Gradient boosting achieves the lowest test error at approximately 4,994, followed by bagging at about 5,183 and XGBoost at about 5,276, while linear regression and k-NN perform worst with RMSE values above 6,200 and 6,600 respectively.

The training-time comparison reveals substantial computational differences, with stacking requiring nearly 5.7 seconds to train, random forests about 1.5 seconds, and gradient boosting and bagging under one second, while linear regression, k-NN, and a single decision tree train in only a few hundredths of a second.

These results illustrate a fundamental tradeoff between predictive accuracy and computational cost when selecting ensemble models.

If prediction accuracy is the primary objective, gradient boosting is the best choice because it delivers the lowest error with relatively modest training time.

If training speed and simplicity are critical, a shallow decision tree or linear regression model may be preferable despite their higher error.

Stacking provides strong performance but is difficult to justify operationally in this case because its error is higher than gradient boosting while its training time is several times longer.

Overall, gradient boosting represents the best balance of accuracy, stability, and computational efficiency for this regression problem, making it the recommended default ensemble approach for structured cost prediction tasks.

---

## 14.9 Overfitting, Interpretability, and Tradeoffs

Ensemble models often deliver higher predictive accuracy than single models, but this improvement comes with important practical tradeoffs. In real systems, model quality must be balanced against interpretability, computational cost, memory usage, and prediction latency.

Understanding these tradeoffs is essential for choosing models that perform well not only in experiments, but also in production environments.

#### Ensembles vs single models

Single models such as linear regression or shallow decision trees are easy to train, fast to deploy, and straightforward to explain. However, they often suffer from high bias or high variance, limiting their predictive performance.

Ensembles reduce these limitations by combining many models, stabilizing predictions and capturing more complex patterns. This typically lowers error and improves probability calibration, but increases system complexity.

#### Explainability

Interpretability refers to how easily humans can understand why a model made a particular prediction. This is especially important in regulated domains such as lending, healthcare, and insurance.

- Linear models provide direct coefficient-based explanations.
- Decision trees offer rule-based explanations through their splits.
- Random forests and boosting models provide global feature importance but limited local explanations.
- Advanced tools such as _SHAP_ and _LIME_ can approximate local explanations for complex ensembles, but add additional computational overhead.

#### Computation, memory, and latency

Ensembles require more computation during both training and prediction because many models must be evaluated for each case. This can affect scalability in high-volume or real-time systems.

The table below provides a rough comparison of typical training times relative to a single decision tree, assuming similar hyperparameters and dataset size. These are approximate guidelines; actual times depend heavily on dataset size, feature count, tree depth, and hardware:

Memory usage also increases because dozens or hundreds of trees must be stored, compared to a small set of coefficients for linear models.

Prediction latency becomes critical in applications such as fraud detection or recommendation systems, where decisions must be made in milliseconds. In these settings, simpler models may outperform ensembles operationally even if their statistical accuracy is lower.

#### Model selection decision guide

In practice, model selection is rarely about choosing the single most accurate algorithm.

Instead, practitioners must weigh performance gains against operational constraints, regulatory requirements, and user trust.

Ensembles often dominate in offline benchmarking, but simpler models may be preferred when transparency, speed, or maintainability are paramount.

A well-designed analytics system therefore treats ensemble methods as powerful tools rather than universal solutions.

---

## 14.10 When NOT to use ensembles

While ensemble methods often improve predictive performance, there are several scenarios where they may not be worth the added complexity, cost, or tradeoffs:

- **Very small datasets (< 1000 rows):** With limited data, ensembles may overfit more easily than simpler models. The bootstrap sampling in bagging and random forests can create too much overlap in training sets, reducing diversity. A single well-regularized model (such as logistic regression with regularization or a shallow decision tree) may generalize better.
- **When interpretability is legally required:** In regulated industries such as finance (fair lending), healthcare (diagnostic explanations), and hiring (anti-discrimination laws), stakeholders must be able to explain model decisions. Ensemble methods, especially deep boosting or stacking, are "black boxes" that require additional explainability tools (such as SHAP or LIME) to meet regulatory requirements. A simple linear model or single decision tree may be preferable when direct interpretability is mandatory.
- **Real-time prediction systems with strict latency requirements:** Applications such as fraud detection, autonomous vehicles, or high-frequency trading require predictions in milliseconds. Ensembles must evaluate many models sequentially or in parallel, increasing latency. A single fast model (such as logistic regression or a shallow tree) may meet accuracy requirements while staying within latency budgets.
- **When a simple linear model already performs well:** If logistic regression or linear regression achieves acceptable accuracy (for example, 95%+ accuracy or low RMSE) and meets business requirements, the marginal improvement from ensembles may not justify the added complexity, training time, and maintenance burden. Always compare ensembles to simple baselines before assuming complexity is necessary.

The key principle is to start simple and add complexity only when it provides meaningful business value. Ensemble methods are powerful tools, but they are not always the right solution.

---

## 14.11 Case Studies

See what you can learn from the practice problems below:

This case uses the same **Customer Churn** dataset from the previous chapter, but extends the analysis using **ensemble classification models**. Your goal is to examine how bagging, random forests, boosting, and stacking compare to single models for predicting customer churn.

**Dataset attribution:** Telecommunications customer churn dataset with demographics, service usage, contract attributes, and a binary churn outcome variable. See details on Kaggle.com The customer churn dataset is available in the prior chapter if you need to reload it.

**Prediction goal:** Predict whether a customer will churn (_Yes_ or _No_) using all available features except the target variable.

For reproducibility, use **random_state = 27** everywhere a random seed is accepted.

**Tasks**

- Reuse the same preprocessing pipeline and train/test split from Chapter 13.
- Train a _RandomForestClassifier_ and report accuracy, precision, recall, ROC AUC, and log loss.
- Train a _BaggingClassifier_ with shallow decision trees as base learners and evaluate the same metrics.
- Train an _AdaBoostClassifier_ using decision stumps.
- Train a _GradientBoostingClassifier_.
- Train a _StackingClassifier_ using logistic regression, random forest, and k-NN as base learners.
- Add all models to a shared comparison table including accuracy, log loss, and macro-averaged F1 score.
- Plot confusion matrices for the two best-performing models.

**Analytical questions**

1. Which ensemble method achieved the highest accuracy?
1. Which model produced the lowest log loss?
1. Did any ensemble significantly improve recall for the churn class?
1. How did stacking compare to the best single model?
1. Which model would you deploy in a real telecom retention system and why?

### Customer Churn Ensemble Models – Case Study Answers

These answers assume you used the Customer Churn dataset, an 80/20 stratified train/test split with **random_state = 27**, and identical preprocessing pipelines for all models. The values referenced below come directly from the evaluation results you reported for logistic regression, decision trees, bagging, random forests, AdaBoost, gradient boosting, and stacking.

#### Q1. Highest accuracy among ensemble methods

Among the ensemble models, the highest test-set accuracy was achieved by **stacking (LR + RF + kNN → LR)** with **accuracy = 0.7949**. This was slightly higher than gradient boosting (_0.7942_) and AdaBoost (_0.7928_), but still marginally below logistic regression’s accuracy of _0.7970_.

#### Q2. Lowest log loss

The model with the lowest test-set log loss was **gradient boosting (GBDT)** with **log loss = 0.4127**. This is slightly better than logistic regression’s log loss of _0.4152_ and noticeably better than bagging (_0.4306_), stacking (_0.4284_), random forest (_0.4717_), and AdaBoost (_0.5102_).

#### Q3. Churn-class recall improvements

No ensemble model produced a meaningful improvement in recall for the churn class beyond the best single model. The highest churn recall was **0.5241**, achieved by both **logistic regression** and **gradient boosting (GBDT)**. Other ensemble methods performed worse: stacking (_0.4973_), AdaBoost (_0.5053_), random forest (_0.4759_), and bagging (_0.4064_).

#### Q4. Stacking versus the best single model

Stacking did not outperform the best single model in this case. Compared to logistic regression, stacking had slightly lower accuracy (_0.7949 vs 0.7970_), worse probability quality (_log loss 0.4284 vs 0.4152_), and lower churn recall (_0.4973 vs 0.5241_). This illustrates that stacking is not guaranteed to improve performance when the base learners already capture most of the predictive signal.

#### Q5. Deployment recommendation

In a real telecom retention system, **gradient boosting (GBDT)** would be the most defensible choice. It achieved the lowest log loss (_0.4127_), tied for the highest churn recall (_0.5241_), and delivered competitive accuracy (_0.7942_). Because retention actions are typically driven by risk thresholds and expected value calculations, well-calibrated probabilities are more important than small differences in accuracy, making gradient boosting particularly suitable for operational deployment.

This case study uses the **Employee Attrition** dataset (_Employee_Attrition.csv_) to evaluate whether ensemble learning methods improve prediction quality over single models. Your goal is to build and compare multiple **ensemble classifiers** that estimate whether an employee will leave the company (_Attrition_ = Yes/No), and to analyze tradeoffs in accuracy, probability calibration, recall for high-risk employees, and computational cost.

**Dataset attribution:** This dataset is widely distributed as an “Employee Attrition / HR Analytics” teaching dataset based on IBM HR sample data and is provided in this course as _Employee_Attrition.csv_. See details on Kaggle.com The Employee Attrition dataset is available in the prior chapter if you need to reload it.

**Prediction goal:** Predict whether _Attrition_ is Yes (employee leaves) or No (employee stays) using the remaining columns as predictors. Use a supervised learning workflow with preprocessing pipelines, a stratified train/test split, and evaluation on a holdout test set.

For reproducibility, use **random_state = 27** everywhere that a random seed is accepted.

**Tasks**

- Inspect the dataset: number of rows and columns, data types, missing values, and the class distribution of _Attrition_.
- Define _X_ and _y_, where _y = Attrition_. Remove identifier-style columns (for example _EmployeeNumber_) if present.
- Create an 80/20 train/test split using _random_state=27_ and _stratify=y_.
- Build a preprocessing pipeline using _ColumnTransformer_: scale numeric features with _StandardScaler_ and one-hot encode categorical features using _OneHotEncoder(handle_unknown="ignore")_.
- Train a baseline _logistic regression_ model and record test-set accuracy, log loss, ROC AUC, and recall for the attrition class.
- Train the following ensemble models inside the same preprocessing pipeline:
- Evaluate each model using: accuracy, precision, recall, F1-score, ROC AUC, and log loss. Store all results in a single comparison table.
- Plot confusion matrices for logistic regression, gradient boosting, and stacking. Compare how many high-risk employees (Attrition = Yes) are missed by each model.
- Compare training time and model complexity across all methods. Identify which models would be difficult to deploy in a real-time HR analytics system.
- Select one model as the recommended production system for predicting employee attrition. Justify your choice using probability quality, recall, interpretability, and operational constraints.

**Analytical questions**

1. How many rows and columns are in the Employee Attrition dataset, and what percentage of employees have _Attrition = Yes_?
1. Which ensemble model achieved the highest test-set accuracy? Which achieved the lowest log loss?
1. Which model achieved the highest recall for employees who actually left the company? Why is this metric especially important for HR planning?
1. Compare gradient boosting and random forests in terms of probability calibration (log loss) and training time. Which appears more suitable for operational forecasting?
1. Did stacking meaningfully outperform the best single model? Support your answer using at least two quantitative metrics.
1. Which features were most important in the tree-based ensemble models, and do they align with HR intuition about employee turnover?
1. Reflection (4–6 sentences): Discuss how ensemble learning changes the bias–variance tradeoff compared to a single decision tree in this dataset. Would you expect similar improvements on much smaller datasets? Why or why not?

### Employee Attrition Ensemble Case Answers

These answers assume you used the provided Employee Attrition dataset, removed ID-like columns (such as EmployeeNumber), created an 80/20 stratified train/test split using **random_state = 27**, and evaluated all models on the same holdout test set. Results are reported exactly as shown in your experiment output.

#### Q1. Dataset size

The dataset contains **1,470 rows** and **35 columns**.

#### Q2. Attrition rate

The attrition rate (share of employees with _Attrition = Yes_) is **0.1612**, corresponding to **237 attritions out of 1,470 employees** (about **16.12%**).

#### Q3. Baseline and data split context

After preprocessing and removing ID-like fields, the feature matrix contained **30 predictors**.

The stratified split produced **1,176 training rows** and **294 test rows**.

#### Q4. Logistic regression performance

Logistic regression achieved test-set **accuracy = 0.8878** and **log loss = 0.3511**, with _ROC AUC = 0.7991_ and _macro F1 = 0.7587_.

For the attrition class (_Yes_), precision was **0.7188** and recall was **0.4894**, meaning the model correctly identified about 49% of employees who eventually left.

This model also trained extremely quickly (_0.025 seconds_), making it a strong baseline in both performance and operational efficiency.

#### Q5. Ensemble model comparison (accuracy and log loss)

Among all ensemble models evaluated (bagging, random forest, AdaBoost, gradient boosting, and stacking), the results varied substantially across accuracy, probability quality, and training cost.

The highest overall accuracy was achieved by **logistic regression (0.8878)**, narrowly outperforming stacking (0.8844) and gradient boosting (0.8810).

The lowest log loss was achieved by the **stacking model (0.3207)**, indicating the best-calibrated probability estimates among all models tested.

#### Q6. Attrition recall comparison

Recall for the attrition class varied widely across models:

- Logistic regression: **0.4894**
- Stacking: _0.4468_
- Gradient boosting: _0.3404_
- AdaBoost: _0.2979_
- Random forest: _0.1702_
- Bagging: _0.1064_

Logistic regression detected the largest share of actual attrition cases, while bagging and random forests were extremely conservative and missed most departing employees.

#### Q7. Feature importance insights

Both random forest and gradient boosting highlighted similar dominant predictors of attrition risk.

Top features included:

- MonthlyIncome
- Age
- DistanceFromHome
- TotalWorkingYears
- YearsAtCompany
- OverTime (Yes/No)
- JobSatisfaction
- StockOptionLevel

These results suggest that compensation, career stage, workload, commute burden, and job satisfaction all play major roles in employee retention.

#### Q8. Stacking versus the best single model

The best single model by log loss was logistic regression (_log loss = 0.3511_, accuracy = 0.8878).

The stacking ensemble improved probability quality substantially (_log loss = 0.3207_) while achieving nearly identical accuracy (_0.8844_).

However, stacking required **over 12 seconds** of training time, compared to _0.025 seconds_ for logistic regression.

#### Q9. Deployment recommendation

If the primary objective is **operational simplicity, interpretability, and fast retraining**, logistic regression is the most practical choice. It achieves the highest accuracy, the highest attrition recall, strong probability quality, and negligible training cost.

If the organization instead prioritizes **probability calibration for ranking employees by risk** (for example, allocating retention incentives to the top 5–10% most at-risk employees), the stacking model is preferable despite its computational cost.

In real HR systems, the final choice should reflect business costs: missing a true attrition case (false negative) may be more expensive than contacting an employee who would have stayed anyway (false positive).

#### Q10. Reflection (sample answer)

This case illustrates that ensemble methods do not automatically dominate simpler models. While stacking improved probability calibration, it did not materially improve accuracy or recall beyond logistic regression. Ensemble models are most valuable when nonlinear interactions dominate or when probability ranking quality is critical. In structured HR data with strong linear signals, simpler models can remain highly competitive while being easier to deploy and explain.

This case uses a Telco customer support dataset of service tickets. Your goal is to build and compare **ensemble-based multiclass classification models** that predict ticket _priority_ (Low, Medium, High) using structured ticket and customer context variables. You will evaluate models using both _accuracy_ (threshold-based) and _multiclass log loss_ (probability-based).

**Dataset attribution:** The dataset file for this case is _Support_tickets.csv_. See details on Kaggle.com The Support Ticket Priority dataset is available in the prior chapter if you need to reload it.

**Prediction goal:** Predict _priority_ (Low, Medium, High). Treat the classes as _nominal_ (not ordered) and use standard multiclass classification metrics.

**Recommended feature set:** Use a mix of numeric and categorical predictors. Prefer the human-readable categorical columns (for example: _day_of_week_, _company_size_, _industry_, _customer_tier_, _region_, _product_area_, _booking_channel_, _reported_by_role_, _customer_sentiment_) and numeric operational columns (for example: _org_users_, _past_30d_tickets_, _past_90d_incidents_, _customers_affected_, _error_rate_pct_, _downtime_min_, plus binary flags such as _payment_impact_flag_ and _security_incident_flag_). Exclude identifier-like columns such as _ticket_id_.

**Note on duplicate encodings:** This dataset includes both readable categorical columns and numeric-coded versions (for example, _industry_ and _industry_cat_). Use only one representation. The recommended approach is to use the readable categorical columns with one-hot encoding.

For reproducibility, use **random_state = 27** everywhere a random seed is accepted.

**Tasks**

- Inspect the dataset: report the number of rows and columns, list the unique values of _priority_, and compute class counts and percentages.
- Create _X_ and _y_ where _y = priority_. Remove _ticket_id_ and all _\_cat_ columns.
- Split the data into training and test sets (80/20) using _random_state=27_ and _stratify=y_.
- Build a preprocessing pipeline using _StandardScaler_ for numeric predictors and _OneHotEncoder(handle_unknown="ignore", sparse_output=False)_ for categorical predictors.
- Train a baseline classifier that always predicts the most frequent priority class. Report test-set accuracy and multiclass log loss using class proportions as constant probabilities.
- Train and evaluate a multiclass _RandomForestClassifier_. Report accuracy, macro F1, classification report, and log loss.
- Train and evaluate a _BaggingClassifier_ with shallow decision trees as base learners. Use at least 100 estimators and report the same metrics.
- Train and evaluate a _GradientBoostingClassifier_ (GBDT). Report accuracy, macro F1, and log loss.
- Train and evaluate an _AdaBoostClassifier_ using shallow trees (stumps). Report the same metrics.
- Train a _StackingClassifier_ with base learners: logistic regression, random forest, and k-NN, and a logistic regression meta-model. Report test metrics and training time.
- Create a single comparison table including all models (baseline, bagging, random forest, AdaBoost, gradient boosting, stacking) with accuracy, macro F1, and log loss.
- Create two bar charts: one for accuracy and one for log loss across all models.
- Create confusion matrices for the two best-performing models (based on log loss and macro F1) and interpret the most common misclassifications.
- Write a short deployment checklist (5–8 bullets) describing how you would choose a model in a real support organization, considering probability calibration, minority-class behavior, interpretability, and runtime constraints.

**Analytical questions**

1. How many rows and columns are in the dataset, and what are the class proportions for Low, Medium, and High priority?
1. How does the baseline model perform in terms of accuracy and log loss, and why is log loss especially informative in multiclass settings?
1. Which ensemble method achieved the lowest multiclass log loss?
1. Which model achieved the highest macro-average F1 score?
1. Which priority class is hardest to predict correctly across models?
1. Compare bagging and random forests. Which performs better and why?
1. Did boosting improve probability calibration relative to bagging?
1. Did stacking improve performance over the best single ensemble method?
1. Which two priority levels are most frequently confused in the confusion matrices?
1. Short reflection (5–8 sentences): Which model would you deploy in a real support operation and why? Discuss the cost of misclassifying High-priority tickets, interpretability tradeoffs, and probability quality.

### Telco Support Ticket Priority Case Answers

These answers assume you used the Support Tickets dataset, removed identifier and duplicate-coded _\_cat_ columns, created an 80/20 stratified train/test split with **random_state = 27**, and evaluated all models on the same holdout test set. The target label _priority_ is multiclass (High, Medium, Low) and metrics include _accuracy_, _macro F1_, and _multiclass log loss_.

#### Q1. Dataset size

The dataset contains **50,000 rows** and **33 columns** before preprocessing.

#### Q2. Class distribution

The target has three classes: **high**, **low**, and **medium**.

Class counts are: **low = 25,000**, **medium = 17,500**, and **high = 7,500**.

Class proportions are: **low = 0.50**, **medium = 0.35**, and **high = 0.15**. This imbalance matters because a model can appear strong on accuracy by doing well on the majority class (_low_) while under-serving the _high_ class.

#### Q3. Baseline model

The baseline predicts the most frequent class (_low_) for every ticket, which yields test-set **accuracy = 0.5000**.

Using the training-set class proportions as constant probabilities for every ticket, the baseline multiclass **log loss = 0.9986**. This is a useful reference point for probability quality: any practical model should reduce log loss substantially below this value.

#### Q4. Logistic regression results

Multiclass logistic regression achieves **accuracy = 0.8648**, **macro F1 = 0.8469**, and **log loss = 0.3406**.

From the classification report, the lowest recall is for the **high** class (_recall = 0.785_). Operationally, this matters because missed _high_-priority tickets are typically more costly than confusing _low_ and _medium_.

#### Q5. Bagging results (depth = 3)

Bagging with shallow trees performs worse than the stronger ensembles in this run: **accuracy = 0.7841**, **macro F1 = 0.7479**, and **log loss = 0.4883**.

The largest weakness is on the _high_ class (_recall = 0.545_), indicating many urgent tickets are not being flagged as _high_ at the default decision rule.

#### Q6. Best depth by log loss and accuracy

In your ensemble-focused run, the strongest probability performance comes from boosting and stacking rather than tuning a single shallow tree depth. In the results you reported, the best model by log loss is **Stacking** (log loss **0.1653**), and the best model by accuracy is **Gradient boosting (GBDT)** (accuracy **0.9394**). This illustrates why “best” can differ by metric: log loss rewards well-calibrated probabilities, while accuracy rewards correct hard labels.

#### Q7. Feature importance and interpretation

Feature importance depends on the specific model you extract it from (trees/forests/GBDT provide impurity-based importances, while logistic regression often uses coefficients). A practical business interpretation approach is: identify the top drivers for predicting _high_ priority (for example, operational severity signals such as _downtime_min_, _customers_affected_, _error_rate_pct_, and incident flags) and confirm they align with how the support team triages tickets. The key decision implication is whether the model’s most important predictors are “actionable” (can be improved upstream) and “trustworthy” (not simply proxies for data entry artifacts).

#### Q8. Confusion matrices for the two best models

For **Stacking**, the confusion matrix shows the most common confusions are between **high ↔ medium** and **medium ↔ low**, with very little direct confusion between _high_ and _low_. Concretely, stacking misclassifies _high_ as _medium_ **145** times (out of 1,500 high tickets), and misclassifies _medium_ as _low_ **205** times (out of 3,500 medium tickets).

For **Gradient boosting (GBDT)**, the same pattern holds: most errors are “adjacent” (high vs medium, medium vs low), not extreme. GBDT misclassifies _high_ as _medium_ **205** times, and misclassifies _medium_ as _low_ **185** times. In a ticket triage setting, these are often less damaging than confusing _high_ with _low_, but they still affect staffing and response times.

#### Q9. Best model by log loss and by macro F1

Best by probability quality (lowest log loss) is **Stacking (LR + RF + kNN → LR)** with **log loss = 0.1653**, _macro F1 = 0.9196_, and _accuracy = 0.9280_.

Best by balanced classification performance (highest macro F1) is **Gradient boosting (GBDT)** with **macro F1 = 0.9300**, _accuracy = 0.9394_, and _log loss = 0.2047_.

Random forest is the next-closest strong option (_accuracy = 0.9205_, _macro F1 = 0.9092_, _log loss = 0.2727_) and trains much faster than stacking/GBDT in your run.

#### Q10. Reflection sample answer

In a real support operation, I would start with **stacking** if probability quality drives decisions (for example, queue ranking, staffing forecasts, or cost-sensitive thresholding), because it achieved the best _log loss_ (0.1653) while still maintaining high accuracy (0.9280) and strong macro F1 (0.9196). If the priority assignment is primarily used as a hard label and the goal is to maximize correct classification across classes, I would choose **gradient boosting** because it produced the best overall accuracy (0.9394) and macro F1 (0.9300). In both cases, I would pay special attention to the business cost of misrouting _high_ tickets; the confusion matrices suggest most errors are between adjacent levels (high vs medium, medium vs low), which is preferable to confusing high with low. Finally, deployment constraints may justify **random forest** as a strong compromise because it performs well and trains far faster than stacking or GBDT in this run, which can matter for frequent retraining or large-scale production pipelines.

---

## 14.12 Learning Objectives

## 14.13 Assignment

Complete the assignment below:

### 14.13 Ensemble Methods

- Understand how ensemble methods combine multiple models to improve predictions
- Build and evaluate bagging classifiers
- Build and evaluate random forest classifiers
- Build and evaluate boosting classifiers (AdaBoost, Gradient Boosting, XGBoost)
- Build and evaluate stacking classifiers
- Compare ensemble methods to single baseline models
- Interpret feature importance from ensemble models
- Understand when to use different ensemble methods

- Use _random_state=27_ for every object that will accept it (for reproducibility)

- **Transaction ID**: Unique identifier for each transaction (drop this – not a feature)
- **Date**: Transaction date
- **Day of Week**: Day of the week the transaction occurred
- **Time**: Hour of day (0–23)
- **Type of Card**: Card type (Visa, MasterCard, etc.)
- **Entry Mode**: How the card was used (Tap, PIN, CVC)
- **Amount**: Transaction amount
- **Type of Transaction**: Transaction type (POS, Online, ATM)
- **Merchant Group**: Category of merchant (Entertainment, Services, Restaurant, etc.)
- **Country of Transaction**: Country where transaction occurred
- **Shipping Address**: Shipping address country
- **Country of Residence**: Customer’s country of residence
- **Gender**: Customer gender (M, F)
- **Age**: Customer age
- **Bank**: Bank name
- **Fraud**: Binary target variable (1 = Fraud, 0 = No Fraud)

---
