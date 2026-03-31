# Chapter 11: MLR for Predictive Inference

## Learning Objectives

- Students will be able to explain how the goals and evaluation criteria for predictive modeling differ from those of causal/explanatory modeling
- Students will be able to create train/test splits to measure out-of-sample generalization and prevent overfitting
- Students will be able to build reproducible preprocessing pipelines using scikit-learn that learn parameters from training data only
- Students will be able to fit linear regression models using scikit-learn and evaluate predictive performance using MAE and RMSE on held-out test data
- Students will be able to assess data readiness for prediction by checking missingness, identifying feature types, and preventing target leakage

---

## 11.1 Introduction

![Wide-format illustration showing a regression surface fitted to data points, emphasizing prediction rather than explanation. The visual contrasts noisy real-world observations with a smooth best-fit plane, highlighting the goal of minimizing prediction error rather than interpreting individual coefficients.](../Images/Chapter11_images/predictive_regression_header.png)

In the previous two chapters, you learned how to build multiple linear regression (MLR) models and how to evaluate whether those models satisfy the assumptions required for reliable _causal or explanatory interpretation_. In that setting, the primary goal was to understand _why_ an outcome changes and how individual features contribute to that change, holding other factors constant.

In this chapter, we shift to a different—but equally important—goal: _prediction_. When prediction is the objective, the central question is no longer whether each coefficient can be interpreted causally. Instead, the focus is on how accurately a model can generate outcomes for new, unseen data.

Although both causal and predictive modeling often use the same mathematical tool—multiple linear regression—the _modeling mindset_ changes substantially. Decisions about data cleaning, feature inclusion, assumption checking, and model evaluation are guided by predictive performance rather than statistical inference.

This chapter revisits linear regression from a predictive perspective. You will learn which assumptions matter less when prediction is the goal, which issues still threaten model reliability, and how concepts such as overfitting, generalization, and out-of-sample error shape modeling decisions.

We will also introduce a new modeling workflow centered on _scikit-learn_, the most widely used Python library for predictive machine learning. Unlike _statsmodels_, which emphasizes coefficient estimates and hypothesis tests, scikit-learn is designed around training models, generating predictions, and evaluating performance on held-out data.

Throughout the chapter, we will use the same _insurance_ dataset you encountered in earlier chapters. This continuity allows you to directly compare causal and predictive modeling approaches using identical data, making the tradeoffs between interpretation and accuracy more concrete.

By the end of this chapter, you will be able to build a regression model optimized for predictive accuracy, evaluate its performance using appropriate metrics, and explain how and why predictive modeling decisions differ from those made in causal regression analysis.

---

## 11.2 Causal to Prediction

In the previous two chapters, you learned how multiple linear regression can be used to _explain_ relationships and support causal reasoning. In predictive modeling, the objective is different. The primary goal is not to understand why an outcome occurs, but to accurately estimate what the outcome will be for new, unseen observations.

When prediction is the goal, a model is evaluated by how well it _generalizes_ beyond the data used to train it. A predictive model is considered successful if it produces low error on future data, even if its internal mechanics are difficult to interpret.

This shift in objective fundamentally changes the questions we ask during modeling:

- **Causal modeling asks:** Is this coefficient statistically significant? Can this effect be interpreted while holding other variables constant?
- **Predictive modeling asks:** Does this feature improve out-of-sample accuracy? Does removing it reduce prediction error?

Because of this difference, some concepts that were central in earlier chapters play a smaller role here. Statistical significance, p-values, and perfectly satisfied assumptions are no longer the primary criteria for success. What matters most is whether the model performs well on data it has never seen before.

This does not mean that predictive modeling ignores rigor or discipline. Instead, rigor is enforced through different mechanisms: careful data preparation, protection against information leakage, separation of training and testing data, and evaluation using appropriate error-based metrics.

In predictive regression, features are not included because they have a meaningful causal interpretation, but because they help the model make better predictions. A feature may be highly predictive even if its coefficient is unstable, correlated with other features, or difficult to explain in isolation.

Model complexity is also judged differently. In causal analysis, unnecessary complexity can obscure interpretation and weaken inference. In predictive modeling, additional complexity is acceptable as long as it improves generalization and does not lead to overfitting.

Throughout this chapter, you will see that multiple linear regression can be used effectively as a predictive tool, even when some classical assumptions are imperfectly satisfied. The emphasis shifts from “Is this model theoretically ideal?” to “Does this model reliably predict outcomes we care about?”

We will continue using the insurance dataset to illustrate these ideas. Rather than focusing on coefficient interpretation, we will focus on building, evaluating, and refining a model that predicts medical charges as accurately as possible.

Before building predictive models, however, we must revisit how data preparation changes when prediction—not explanation—is the primary objective.

---

## 11.3 Data Preparation

When prediction is the goal, data preparation is evaluated by a single criterion: does this step help the model generalize to new, unseen data? This is a meaningful shift from causal regression, where preparation steps are often motivated by interpretability, assumption validity, or defensible inference.

In predictive regression, preprocessing decisions must also be _repeatable_. Any transformation applied during model development must be applied in exactly the same way when the model is used to make future predictions. This requirement will later motivate the use of pipelines, but we begin here with clear, explicit preparation steps using pandas.

We will continue using the insurance dataset introduced earlier in the book. For consistency across sections, we load the dataset once and reuse it throughout this chapter.

```python
import pandas as pd

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df.head()
```

Before making any modeling decisions, predictive workflows begin with basic structural checks: dataset size, column names, data types, and a clear separation between the label and candidate predictors.

```python
display(df.info())

# Separate label and predictors conceptually (no splitting yet)
y = df["charges"]
X = df.drop(columns=["charges"])

# Output:
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 1338 entries, 0 to 1337
# Data columns (total 7 columns):
#  #   Column    Non-Null Count  Dtype
# ---  ------    --------------  -----
#  0   age       1338 non-null   int64
#  1   sex       1338 non-null   object
#  2   bmi       1338 non-null   float64
#  3   children  1338 non-null   int64
#  4   smoker    1338 non-null   object
#  5   region    1338 non-null   object
#  6   charges   1338 non-null   float64
# dtypes: float64(2), int64(2), object(3)
# memory usage: 73.3+ KB
# None
```

Unlike causal modeling, predictive modeling does not require us to justify each predictor theoretically. At this stage, we keep all available features and allow performance metrics later in the chapter to guide feature removal decisions.

#### Checking for missing values

Even when documentation suggests a dataset is complete, you should verify missingness directly. In predictive modeling, this step is not only diagnostic but operational: the same checks must hold when new data arrives in the future.

```python
missing_by_column = X.isna().sum().sort_values(ascending=False)
print(missing_by_column)

print("Any missing values?", X.isna().any().any())

# Output:
# age         0
# sex         0
# bmi         0
# children    0
# smoker      0
# region      0
# dtype: int64
# Any missing values? False
```

This dataset contains no missing values, so no imputation is required for the walkthrough model. However, this verification step is still important because predictive pipelines must be robust to future data that may not be as clean as historical data.

#### Scaling considerations for prediction

In causal regression, scaling is often used to improve numerical stability or to interpret standardized coefficients. In predictive regression, scaling serves a different purpose: it ensures that features are on comparable numeric ranges for algorithms that are sensitive to feature magnitude.

At this stage, we do not apply scaling yet. Instead, we explicitly identify which columns are numeric and which are categorical. This classification will later allow us to apply different preprocessing steps to different feature types in a structured and repeatable way.

```python
numeric_features = X.select_dtypes(include=["int64", "float64"]).columns
categorical_features = X.select_dtypes(include=["object"]).columns

print("Numeric features:", list(numeric_features))
print("Categorical features:", list(categorical_features))

# Output:
# Numeric features: ['age', 'bmi', 'children']
# Categorical features: ['sex', 'smoker', 'region']
```

#### Leakage awareness

Information leakage occurs when a model is trained using information that would not be available at prediction time. Leakage can dramatically inflate apparent performance during development while producing poor results in real-world use.

The most obvious form of leakage is including the outcome variable as a predictor. By explicitly defining _charges_ as the label and removing it from the feature matrix at the start of the workflow, we create a simple but effective guardrail against this error.

More subtle leakage risks arise when preprocessing decisions are informed by the full dataset rather than by training data alone. We will address those risks directly in the next section, where we introduce train/test splits and explain how they protect against overfitting.

#### Summary and Overview

At this point, we do not want to go too far into implementation details. Fully production-ready predictive pipelines rely on faster, more structured data representations than Pandas DataFrames, and we have not introduced those tools yet. Instead, the goal here is to establish a _conceptual checklist_: the core data preparation considerations that must be addressed when building a predictive regression model, regardless of the specific tools used to implement them.

---

## 11.4 Regression Assumptions

The table below summarizes each of the regression assumptions and diagnostic tests including how they are relevant to predictive regression modeling.

In earlier chapters, regression assumptions were introduced as requirements for valid inference and defensible causal interpretation. When prediction is the goal, those same assumptions do not disappear—but their purpose and priority change.

Rather than asking whether assumptions are satisfied to justify coefficient-level conclusions, predictive modeling asks a different question: do transformations inspired by these assumptions improve out-of-sample accuracy and generalization?

As a result, assumptions in predictive regression function less like strict rules and more like _diagnostic signals_ that suggest potential feature engineering opportunities.

#### What matters less for prediction

Several assumptions that were critical for causal inference play a reduced role in predictive modeling. For example, residual normality is not required for accurate predictions, and multicollinearity is not inherently problematic as long as correlated features collectively improve predictive performance.

Similarly, unstable or difficult-to-interpret coefficients are acceptable in predictive regression. Features are retained or removed based on their contribution to generalization error, not on whether their individual effects can be cleanly isolated.

#### What still matters for prediction

Other assumptions remain important because they directly affect model performance. Linearity of the relationship between features and the label matters insofar as violations indicate that the model is systematically missing structure that could be captured through transformation.

Heteroscedasticity also remains relevant, not because it biases coefficients, but because it signals uneven error variance that can reduce predictive reliability for certain regions of the feature space.

Autocorrelation continues to matter in predictive contexts involving time or sequence, because dependence between observations can cause models to overestimate their true generalization ability.

#### What changes meaning in predictive regression

In predictive modeling, assumptions are best understood as guides for feature engineering rather than criteria for model validity. Each assumption suggests specific transformations that may improve prediction, even if they complicate interpretation.

For example, nonlinearity motivates polynomial features and interaction terms, heteroscedasticity motivates variance-stabilizing transformations, and skewed distributions motivate logarithmic or other monotonic transforms.

Importantly, these transformations are not justified because they “fix” assumptions, but because they can reduce systematic error on unseen data.

#### Common predictive transformations motivated by diagnostics

Based on the diagnostics explored in the prior chapter, predictive regression commonly considers the following transformations: nonlinear feature expansions (such as squared terms), interaction effects between features, transformations of the label to stabilize variance, and retention of correlated features when they jointly improve prediction.

In the insurance dataset, this includes transformations such as squared age or BMI terms, interactions involving smoking status, and logarithmic transformations of medical charges to reduce skew and error heterogeneity.

We intentionally do not implement these transformations yet. In predictive modeling, feature engineering must be learned from training data and applied consistently to new data, which requires pipeline-based workflows that we introduce later in this chapter. The next sections focus on generalization, evaluation, and tooling before we build the full predictive model end to end.

---

## 11.5 Train/Test Splits

In predictive modeling, the central question is not “How well does my model fit the data I already have?” but “How well will my model perform on new data I have never seen before?” This ability to perform well on unseen data is called _generalization_.

A model that fits the training data extremely well but performs poorly on new data is said to be _overfitting_. Overfitting occurs when the model learns noise, quirks, or accidental patterns in the training data rather than the true underlying signal.

To measure generalization directly, we must evaluate the model on data that was not used during training. This is accomplished by splitting the dataset into two parts:

- _Training set_: used to fit the model and learn preprocessing steps.
- _Test set_: held aside and used only for final evaluation.

In causal regression, analysts often fit models using the full dataset because the goal is coefficient estimation and inference. In predictive regression, however, fitting on all available data would prevent us from measuring whether the model truly generalizes.

From this point forward, all predictive models in this chapter will be trained using only the training set and evaluated using only the test set.

#### Creating a train/test split

We begin by loading the insurance dataset and separating the label (_charges_) from the predictor features. Then we create a train/test split using an 80/20 partition.

```python
import pandas as pd
from sklearn.model_selection import train_test_split

# Separate label and predictors
y = df["charges"]
X = df.drop(columns=["charges"])

# Train/test split (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(
  X, y, test_size=0.2, random_state=42
)

print("Training set size:", X_train.shape)
print("Test set size:", X_test.shape)
```

The _random_state_ parameter ensures that the split is reproducible, which is important for teaching, debugging, and fair model comparison.

At this stage, no preprocessing has been performed. The data is still in raw Pandas DataFrame form. This is intentional: preprocessing must be learned from the training data only and applied consistently to the test data, which we will implement using pipelines in a later section.

These four objects will be reused throughout the remainder of the chapter:

- _X_train_: features used to train the model.
- _y_train_: labels used to train the model.
- _X_test_: features reserved for evaluation.
- _y_test_: true labels for evaluation.

In the next section, we will define how predictive performance is measured and why error-based metrics are more informative than traditional statistical measures such as p-values or R² when the goal is generalization.

---

## 11.6 Pipelines in sklearn

![Conceptual header image representing preprocessing steps](../Images/Chapter11_images/regression_processing.png)

Now that we have separated training and test data, the next step is to define how raw input features will be transformed into a numeric format suitable for modeling. In predictive regression, these transformations must be applied _consistently_ to training data, test data, and all future data seen in production.

In earlier chapters, we used Pandas functions such as _get_dummies()_ and manual scaling to prepare data. While this approach works for causal analysis and small experiments, it does not scale well to real predictive systems because it is slow, error-prone, and difficult to reproduce exactly at inference time.

The sklearn library provides specialized objects for building fast, reliable, and reusable preprocessing workflows. These objects operate on NumPy arrays internally, which makes them much faster than DataFrame-based pipelines and suitable for large-scale machine learning systems.

A major advantage of this approach is that preprocessing is _learned from training data only_ and then reused automatically for the test set and future observations, preventing subtle forms of information leakage.

We will now construct a preprocessing pipeline for the insurance dataset using the training and test sets created in the previous section.

#### Defining numeric and categorical feature groups

We begin by identifying which columns are numeric and which are categorical in the training data. This ensures that transformations are learned only from the training distribution.

```python
# Identify feature types using training data only
num_cols = X_train.select_dtypes(include=["int64", "float64"]).columns
cat_cols = X_train.select_dtypes(include=["object"]).columns

num_cols, cat_cols

# Output:
# (Index(['age', 'bmi', 'children'], dtype='object'),
# Index(['sex', 'smoker', 'region'], dtype='object'))
```

#### Building the preprocessing components

Next, we define separate preprocessing steps for numeric and categorical features. Numeric features will be standardized, and categorical features will be one-hot encoded. Missing-value imputers are included for completeness, even though this dataset contains no missing values.

```python
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer

numeric_preprocess = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="median")),
  ("scaler", StandardScaler())
])

categorical_preprocess = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("onehot", OneHotEncoder(handle_unknown="ignore"))
])
```

#### Combining transformations with ColumnTransformer

The ColumnTransformer applies the appropriate preprocessing pipeline to each group of features and concatenates the results into a single numeric matrix.

```python
preprocessor = ColumnTransformer(
  transformers=[
    ("num", numeric_preprocess, num_cols),
    ("cat", categorical_preprocess, cat_cols)
  ]
)
```

#### Fitting preprocessing on training data only

We now fit the preprocessing pipeline using only the training data and apply it to both training and test sets.

```python
X_train_ready = preprocessor.fit_transform(X_train)
X_test_ready = preprocessor.transform(X_test)

X_train_ready.shape, X_test_ready.shape

# Output:
# ((1070, 11), (268, 11))
```

Notice that the result is a NumPy matrix rather than a Pandas DataFrame. This representation is faster and is the standard input format expected by sklearn models.

At this point, our data is fully numeric, consistently scaled, safely encoded, and ready for model training.

In the next section, we will use these transformed features to train a predictive linear regression model using sklearn’s modeling API.

---

## 11.7 MLR in sklearn

In earlier chapters, you used Statsmodels to fit regression models designed for explanation and statistical inference. For predictive modeling, we shift to _scikit-learn (sklearn)_, a library optimized for performance, automation, and large-scale model deployment.

Sklearn treats regression as one component in a larger system: data preprocessing, feature transformation, model fitting, and prediction are designed to work together as a single pipeline.

This design reflects a fundamental difference in priorities: Statsmodels emphasizes coefficient interpretation and statistical testing, while sklearn emphasizes prediction accuracy, generalization, and repeatable workflows.

**Key conceptual difference:** Statsmodels fits models to DataFrames for human inspection; sklearn fits models to numeric matrices optimized for computation.

Sklearn’s linear regression model implements ordinary least squares just like Statsmodels, but it exposes only what is needed for prediction: coefficients, intercept, and prediction functions.

In predictive modeling, the regression model is rarely used alone. Instead, it is embedded inside a pipeline so that raw input data can be transformed and predicted in one consistent operation.

#### Adding a regression model to the preprocessing pipeline

We now extend the preprocessing pipeline created in the previous section by attaching a linear regression model as the final step.

This creates a complete predictive system that accepts raw insurance records and outputs predicted medical charges.

```python
from sklearn.linear_model import LinearRegression

# Extend the existing preprocessing pipeline with a regression model
predictive_model = Pipeline(steps=[
  ("prep", preprocessor),
  ("lr", LinearRegression())
])

# Fit only on training data
predictive_model.fit(X_train, y_train)
```

![A version of the python output of the fitted predictive_model pipeline where the objects have not been expanded to show their contents](../Images/Chapter11_images/pipeline_object_closed.png)

![A version of the python output of the fitted predictive_model pipeline where the objects have been expanded to show their contents. The contents include the settings of each object. For example, the SimpleImputer object reveals that imputation is done using the median of the column as demonstrated in the code.](../Images/Chapter11_images/pipeline_object_opened.png)

At this point, the pipeline has learned two things from the training data: how to transform raw features into numeric form, and how to combine those features to predict charges.

No transformation parameters or regression coefficients were learned from the test data, preserving the integrity of future evaluation.

Once trained, the pipeline behaves like a single model object that can be used to generate predictions on any new dataset with the same structure.

```python
# Generate predictions for the test set
y_pred = predictive_model.predict(X_test)

# Preview first 5 predictions
y_pred[:5]

# Output:
# array([ 8969.55027444, 7068.74744287, 36858.41091155, 9454.67850053, 26973.17345656])
```

The output is a NumPy array of predicted charges, one for each row in the test set (only the first five rows are displayed above because of the index [:5]).

In the next section, we will evaluate how accurate these predictions are using appropriate performance metrics for regression.

This completes the construction phase of our predictive regression pipeline: raw data → preprocessing → numeric matrix → trained model → predictions.

---

## 11.8 Performance Metrics

In causal regression, model fit or quality is often discussed using statistical significance, confidence intervals, and R². In predictive modeling, these quantities are secondary. What matters most is how large the prediction errors are on new data.

For this reason, predictive regression models are evaluated using _error-based metrics_ that directly measure how far predictions are from true outcomes.

#### Mean Absolute Error (MAE)

The _Mean Absolute Error (MAE)_ is the average absolute difference between predicted values and actual values.

MAE is easy to interpret because it is expressed in the same units as the label. In this dataset, MAE is measured in dollars of medical charges.

#### Root Mean Squared Error (RMSE)

The _Root Mean Squared Error (RMSE)_ squares errors before averaging and then takes the square root. This penalizes large mistakes more heavily than MAE.

RMSE is especially useful when large prediction errors are particularly costly or risky.

#### Choosing between MAE and RMSE

Although MAE and RMSE both measure prediction error in the same units as the label, they emphasize different types of mistakes.

MAE treats all errors equally and answers the question: “How wrong are we on average?” RMSE penalizes large errors more heavily and answers the question: “How severe are our worst mistakes?”

This difference matters in practice because different business problems care about different kinds of errors.

In short, use MAE when typical accuracy is the goal and robustness to outliers is important. Use RMSE when large errors are especially dangerous or costly and should strongly influence model selection.

#### The limited role of R² in prediction

R² measures the proportion of variance explained in the dataset being evaluated. While useful for understanding model fit, it does not directly describe how large prediction errors are.

Two models can have similar R² values but very different MAE or RMSE values. For this reason, R² plays a supporting role in predictive modeling, while MAE and RMSE are primary.

#### A baseline for comparison

Before evaluating our trained model, we establish a simple baseline: always predicting the mean of the training labels. This represents a model that completely ignores all features and assumes every observation is “average.”

This baseline is not arbitrary. When prediction error is measured using squared error (and therefore RMSE), the mean is mathematically the optimal constant prediction—it minimizes expected squared error among all possible single-value predictions.

As a result, the mean predictor represents the best performance achievable without using any input features at all.

If a trained model cannot outperform this baseline on the test set, then the features and modeling process have added no predictive value, and the model has little practical usefulness.

#### Computing baseline performance

```python
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Baseline predictor: mean of training labels
baseline_value = y_train.mean()
y_pred_baseline = np.full_like(y_test, fill_value=baseline_value, dtype=float)

baseline_mae = mean_absolute_error(y_test, y_pred_baseline)
baseline_rmse = np.sqrt(mean_squared_error(y_test, y_pred_baseline))
baseline_r2 = r2_score(y_test, y_pred_baseline)

print(f"Baseline MAE:  {baseline_mae:,.2f}")
print(f"Baseline RMSE: {baseline_rmse:,.2f}")
print(f"Baseline R²:   {baseline_r2:.4f}")

# Output:
# Baseline MAE:  9,593.34
# Baseline RMSE: 12,465.61
# Baseline R²:   -0.0009
```

#### Evaluating our predictive regression model

We now evaluate the predictions generated by the pipeline trained in the previous section.

```python
model_mae = mean_absolute_error(y_test, y_pred)
model_rmse = mean_squared_error(y_test, y_pred, squared=False)
model_r2 = r2_score(y_test, y_pred)

print(f"Model MAE:  {model_mae:,.2f}")
print(f"Model RMSE: {model_rmse:,.2f}")
print(f"Model R²:   {model_r2:.4f}")

# Output:
# Model MAE:  4,181.19
# Model RMSE: 5,796.28
# Model R²:   0.7836
```

As you can see, MAE, RMSE, and R² each improved dramatically indicating this model was worth generating.

#### Detecting overfitting through train/test comparison

After computing performance metrics on both training and test sets, the next critical step is comparing these metrics to assess whether the model generalizes well or is overfitting. Overfitting occurs when a model performs much better on training data than on test data, indicating it has learned training-specific patterns that do not generalize to new observations.

While the academic literature emphasizes qualitative assessment of the gap between training and test performance, practitioners often use quantitative heuristics to flag potential overfitting. These heuristics provide concrete thresholds that can be implemented in automated workflows, though they should be interpreted with domain knowledge and context rather than as absolute rules.

It is important to note that these thresholds are practical heuristics rather than strict statistical rules. The appropriate threshold may vary depending on the problem domain, data size, model complexity, and business context. For example, in high-stakes applications like medical diagnosis, even small gaps between train and test performance may warrant concern, while in exploratory research, larger gaps might be acceptable. The key is to use these heuristics as diagnostic signals that prompt further investigation rather than as definitive pass/fail criteria.

When overfitting is detected, common remedies include reducing model complexity (fewer features, simpler algorithms), increasing regularization, collecting more training data, or using ensemble methods that combine multiple simpler models. The next section demonstrates one such approach: greedy backward feature removal, which systematically reduces model complexity to improve generalization.

At this stage, our model is functional but not yet optimized. Many features may contribute little to predictive accuracy or even harm generalization.

In the next section, we will refine the model using systematic feature engineering and removal to further improve out-of-sample performance.

#### Bibliography

The following references provide foundational coverage of overfitting detection, model evaluation, and cross-validation methods in machine learning and statistical learning theory.

1. Arlot, S., & Celisse, A. (2010). A survey of cross-validation procedures for model selection. _Statistics Surveys_, 4, 40-79. https://doi.org/10.1214/09-SS054
1. Hastie, T., Tibshirani, R., & Friedman, J. (2009). _The Elements of Statistical Learning: Data Mining, Inference, and Prediction_ (2nd ed.). Springer. https://doi.org/10.1007/978-0-387-84858-7
1. James, G., Witten, D., Hastie, T., & Tibshirani, R. (2021). _An Introduction to Statistical Learning: with Applications in R_ (2nd ed.). Springer. https://doi.org/10.1007/978-1-0716-1418-1
1. Kohavi, R. (1995). A study of cross-validation and bootstrap for accuracy estimation and model selection. _Proceedings of the 14th International Joint Conference on Artificial Intelligence_, 2, 1137-1143. https://www.ijcai.org/Proceedings/95-2/Papers/016.pdf
1. Murphy, K. P. (2022). _Probabilistic Machine Learning: An Introduction_. MIT Press. https://probml.github.io/pml-book/book1.html

---

## 11.9 Greedy Backward Feature Removal

![Flowchart illustrating greedy backward feature selection for predictive regression. The diagram starts with a full set of input features, then repeatedly removes one feature at a time. At each step, a model is trained on a training subset and evaluated on a validation set using MAE and RMSE. The feature whose removal produces the lowest validation error is permanently discarded. This process repeats until a stopping point is chosen based on error trends or desired model simplicity, resulting in a reduced feature set for the final predictive model.](../Images/Chapter11_images/greedy_header.png)

In predictive regression, we choose features based on whether they improve prediction accuracy on new data. This section demonstrates a practical workflow called _greedy backward feature removal_, where we start with a full set of predictors and then remove one feature at a time while tracking how prediction error changes.

#### What “greedy” means

In optimization, a _greedy_ method makes the best local choice available at each step, using the information it can measure _right now_. In our context, “best local choice” means: remove the single feature that produces the lowest validation error after removal (or, equivalently, produces the largest improvement in validation error) at that step.

Greedy methods are popular because they are straightforward and computationally manageable, but they are not guaranteed to find the absolute best subset of features. A greedy method does not test every possible combination of features; instead, it commits to one removal at a time, which can sometimes miss better combinations that only appear when removing multiple features together.

#### Why we use a validation set

To decide which feature to remove next, we need data that was not used to fit the model. That is the role of a _validation set_: a holdout sample used during model development to compare modeling choices, such as feature removal decisions.

A validation set is different from a test set. The _test set_ is reserved for the final evaluation at the end of the workflow. If we repeatedly used the test set to choose features, we would indirectly “train on the test set” by tailoring decisions to its outcomes, which inflates performance estimates and weakens the credibility of reported results.

In this section, we therefore split our original training data into two parts: a smaller _training subset_ used to fit the model, and a _validation subset_ used to evaluate feature removals. Only after we choose a stopping point will we evaluate the final model on the test set.

#### Feature engineering before feature selection

Before we begin removing features, we first expand the feature space using simple and interpretable _feature engineering_. This gives the model the opportunity to learn nonlinear patterns and subgroup-specific effects that a purely linear specification cannot capture.

Specifically, we create nonlinear transformations of _age_ and _bmi_ (such as squared and logarithmic terms) and interaction terms between smoking status and both the original and transformed variables. These engineered features act as additional candidate predictors that may or may not improve predictive accuracy.

In a predictive workflow, feature engineering and feature selection are tightly coupled: we generate potentially useful transformations first, then allow the greedy removal process to decide which of them are worth keeping based on validation-set error. Features that do not improve MAE or RMSE are treated as noise, even if they would be interesting to interpret in a causal analysis.

```python
import numpy as np
import pandas as pd

# -------------------------------------------------------------------
# Feature engineering: nonlinear terms + smoker interactions
# -------------------------------------------------------------------

X_eng = X.copy()

# 1) Create an explicit smoker indicator for interactions
X_eng["smoker_yes"] = (X_eng["smoker"] == "yes").astype(int)

# 2) Nonlinear terms (examples used earlier in the book)
X_eng["age_sq"] = X_eng["age"] ** 2

# Use log(BMI). BMI is positive in this dataset; if you want to be extra safe:
# X_eng["bmi_ln"] = np.log(np.clip(X_eng["bmi"], a_min=1e-6, a_max=None))
X_eng["bmi_ln"] = np.log(X_eng["bmi"])

# 3) Interaction terms: smoker × age, smoker × bmi (and optionally with nonlinear terms)
X_eng["age_x_smoker"] = X_eng["age"] * X_eng["smoker_yes"]
X_eng["bmi_x_smoker"] = X_eng["bmi"] * X_eng["smoker_yes"]

# Interactions with nonlinear transforms (often useful)
X_eng["age_sq_x_smoker"] = X_eng["age_sq"] * X_eng["smoker_yes"]
X_eng["bmi_ln_x_smoker"] = X_eng["bmi_ln"] * X_eng["smoker_yes"]

# Important: keep the original categorical columns too (sex, region, smoker)
# because they may still carry predictive signal beyond interactions.
X = X_eng
```

#### What we will build

- A loop that removes one feature at a time using a greedy rule based on validation-set error.
- A trace table that records which feature was removed at each step and how _MAE_ and _RMSE_ changed.
- A chart that plots MAE and RMSE as features are removed, with a label above each point showing the feature removed at that step.
- A stopping-criteria summary (evidence-based and domain-based) to guide where to stop removing features.

We will use the insurance dataset and the same preprocessing pipeline approach introduced earlier (scaling numeric features and one-hot encoding categorical features). The key difference is that we will now treat prediction error (MAE and RMSE) as the primary evidence for keeping or removing features.

Next, we will implement the greedy removal loop and generate the trace data that we will later visualize.

#### Greedy Backward Removal in Python

This code assumes you have already loaded the insurance dataset into a Pandas DataFrame named _df_ and created _X_ and _y_ as shown earlier in the chapter (with _charges_ as the label and all other columns as predictors).

It also assumes that the feature engineering step described above has already been applied, so that nonlinear and interaction features are included in _X_ before feature selection begins.

We will use three datasets during development: (1) a training subset used to fit models, (2) a validation subset used to choose which feature to remove next, and (3) a test set reserved for later final evaluation. In this section, we create the validation split and generate the greedy-removal trace.

```python
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Assumes these already exist from earlier code: df, y, X

# 1) Hold out a final test set (not used for feature-removal decisions)
X_train_full, X_test, y_train_full, y_test = train_test_split(
  X, y, test_size=0.20, random_state=42
)

# 2) Create a validation split from the training data (used for greedy decisions)
X_train, X_val, y_train, y_val = train_test_split(
  X_train_full, y_train_full, test_size=0.25, random_state=42
)
# Note: 0.25 of the 0.80 training-full = 0.20 of the total dataset (roughly 60/20/20 split overall)

# 3) Identify numeric vs categorical columns (insurance has both)
num_cols = X_train.select_dtypes(include=["int64", "float64"]).columns.tolist()
cat_cols = X_train.select_dtypes(include=["object"]).columns.tolist()

# 4) Define preprocessing templates
numeric_preprocess = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="median")),
  ("scaler", StandardScaler())
])

categorical_preprocess = Pipeline(steps=[
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

# 5) Helper: build a full pipeline for a given subset of raw input columns
def make_model(selected_features):
  selected_num = [c for c in selected_features if c in num_cols]
  selected_cat = [c for c in selected_features if c in cat_cols]

  preprocessor = ColumnTransformer(
    transformers=[
      ("num", numeric_preprocess, selected_num),
      ("cat", categorical_preprocess, selected_cat)
    ],
    remainder="drop"
  )

  model = Pipeline(steps=[
    ("prep", preprocessor),
    ("lr", LinearRegression())
  ])

  return model

# 6) Helper: fit on training subset and score on validation subset
def fit_and_score(selected_features):
  m = make_model(selected_features)
  m.fit(X_train[selected_features], y_train)

  y_hat = m.predict(X_val[selected_features])
  mae = mean_absolute_error(y_val, y_hat)
  rmse = mean_squared_error(y_val, y_hat, squared=False)

  return mae, rmse

# 7) Greedy backward removal trace
all_features = X_train.columns.tolist()
current_features = all_features.copy()

trace_rows = []

# Baseline (no removals yet)
base_mae, base_rmse = fit_and_score(current_features)
trace_rows.append({
  "step": 0,
  "removed_feature": "(none)",
  "n_features": len(current_features),
  "val_mae": base_mae,
  "val_rmse": base_rmse,
  "remaining_features": current_features.copy()
})

for step in range(1, len(all_features)):
  best_candidate = None
  best_mae = None
  best_rmse = None

  for f in current_features:
    candidate_features = [c for c in current_features if c != f]
    mae, rmse = fit_and_score(candidate_features)

    if (best_rmse is None) or (rmse < best_rmse) or (rmse == best_rmse and mae < best_mae):
      best_candidate = f
      best_mae = mae
      best_rmse = rmse

  current_features.remove(best_candidate)

  trace_rows.append({
    "step": step,
    "removed_feature": best_candidate,
    "n_features": len(current_features),
    "val_mae": best_mae,
    "val_rmse": best_rmse,
    "remaining_features": current_features.copy()
  })

trace = pd.DataFrame(trace_rows)
display(trace)
```

![A view of the trace dataframe showing the MAE and RMSE scores of each best model beginning with all features and then additional models after removing the worst performing feature one at a time](../Images/Chapter11_images/feature_importance_df.png)

The trace table records the validation-set error after each greedy removal step. Step 0 is the baseline (all features). Step 1 removes one feature and records the resulting MAE and RMSE. The process continues until only one feature remains.

Notice that this method is computationally expensive because it refits many models: at each step, it tries removing every remaining feature and chooses the best local option. Even so, this is still far more feasible than testing every possible subset of features, which grows exponentially as the number of features increases.

In the next section, we will visualize the trace by plotting MAE and RMSE across steps and labeling each point with the feature removed at that step. This makes it easier to choose a reasonable stopping point based on evidence (error changes) and domain knowledge (which features were removed).

#### Visualizing MAE and RMSE Across Removals

The trace table is useful, but the key idea is easier to see visually. In a predictive workflow, we often look for a point where removing additional features provides little benefit (or begins to harm performance). This is sometimes called an “elbow” in the error curve.

In the plot below, each point represents one step in the greedy backward removal process. The y-values show validation-set error (MAE and RMSE). The label above each point shows the feature that was removed to reach that step.

Why include feature names on the plot? Because stopping decisions are not only statistical; they are also practical. If the next feature to remove is something you believe is essential for real-world prediction (based on domain knowledge), you might stop earlier—even if the curve suggests small gains from removing it.

```python
import matplotlib.pyplot as plt

# Assumes you already ran the prior chunk and have:
# trace (DataFrame) with columns: step, removed_feature, n_features, val_mae, val_rmse

# X-axis is the step number (0 = full model, then one feature removed each step)
x = trace["step"].to_numpy()
mae = trace["val_mae"].to_numpy()
rmse = trace["val_rmse"].to_numpy()
labels = trace["removed_feature"].tolist()

plt.figure(figsize=(10.5, 4.8))

# Plot MAE and RMSE as separate lines
plt.plot(x, mae, marker="o", linewidth=1.5, label="Validation MAE")
plt.plot(x, rmse, marker="o", linewidth=1.5, label="Validation RMSE")

plt.xlabel("Greedy removal step (higher = fewer features)")
plt.ylabel("Error on validation set")
plt.title("Greedy backward removal trace (insurance dataset)")
plt.legend(frameon=False)

# Add feature-name labels above each point (skip step 0 label since nothing was removed)
for i in range(1, len(x)):
  # RMSE labels (place above RMSE point)
  plt.annotate(
    labels[i],
    (x[i], rmse[i]),
    textcoords="offset points",
    xytext=(0, 8),
    ha="center",
    fontsize=8
  )

# Light grid for readability
plt.grid(True, linewidth=0.3)
plt.tight_layout()
plt.show()
```

![A view of the line plot created using the code above which shows the trace results of which features should be removed one at a time and the effect on MAE and RMSE. The lowest MAE and RMSE scores are highlighted](../Images/Chapter11_images/stopping_point.png)

The highlighted points show the lowest validation MAE and lowest validation RMSE observed during the greedy removal process. In this run, MAE reaches its minimum early, while RMSE reaches its minimum several steps later after additional features have been removed.

This pattern is common in practice. MAE often improves quickly and then flattens, while RMSE may continue to improve slightly as the model becomes simpler and large errors are reduced. Because RMSE penalizes large mistakes more heavily, it often favors a more conservative model with fewer features.

If your application places high cost on large prediction errors (for example, forecasting medical expenses, insurance risk, or financial losses), you may prioritize the stopping point near the minimum RMSE. If interpretability and average accuracy are more important, you may prefer a stopping point near the minimum MAE.

In this example, both curves remain relatively flat for several steps before rising sharply once too many features have been removed. This flat region represents a practical “sweet spot” where the model is simpler but still highly accurate. A reasonable stopping point would lie somewhere in this region, before the sharp increase in error.

Rather than selecting the single absolute minimum mechanically, predictive modeling often uses this plot to guide judgment: choose the simplest model that achieves near-minimum error while avoiding the steep degradation that signals underfitting.

In the next section, we formalize several stopping criteria—both evidence-based (using changes in MAE and RMSE) and domain-based (using knowledge about which features are meaningful or stable)—and show how to produce a final selected feature set.

#### Choosing a Stopping Point and Finalizing the Feature Set

A greedy backward procedure always produces a complete removal path: a full model, then a model with one feature removed, then two removed, and so on until only one feature remains. The remaining question is: _where should you stop?_

In predictive modeling, stopping is not based on statistical significance. Instead, stopping is based on a tradeoff between (1) predictive accuracy on new data and (2) simplicity and robustness. Removing features can sometimes reduce overfitting and improve generalization, but removing too many features eventually harms accuracy.

#### What is a validation set, and why did we include it?

A _validation set_ is a subset of data that is _not used to train the model_ but is used repeatedly to guide modeling decisions—such as selecting features. We used a validation set so that the greedy procedure could compare different feature sets using a consistent, held-out dataset.

This prevents a common mistake: selecting features using the same data that was used to fit the model. If you evaluate candidate feature sets on training data, you will often select a model that looks great in development but performs worse on truly new data. Using a validation set reduces this risk.

Later in the book, you will learn stronger methods such as cross-validation, which reduce sensitivity to a single validation split. For now, a train/validation/test structure is a practical and understandable foundation.

#### Stopping criteria

There is no universal “best” stopping rule. In practice, modelers use a combination of evidence-based criteria (MAE/RMSE changes) and judgment-based criteria (domain knowledge, operational constraints). The table below summarizes common approaches.

#### Producing a final selection using the trace

Because we already computed the full greedy trace, selecting a final model is primarily a matter of choosing the step you want. In the code below, we show two simple ways to choose a stopping point: (1) keep a fixed number of features, or (2) stop when improvement falls below a chosen threshold.

```python
import numpy as np

# Assumes: trace (DataFrame) exists from the greedy procedure
# Columns: step, removed_feature, n_features, val_mae, val_rmse, remaining_features

# --- Option A: Keep a fixed number of features ---
def choose_step_keep_n(trace_df, n_keep):
  # Find the row where n_features == n_keep (closest if not exact)
  idx = (trace_df["n_features"] - n_keep).abs().idxmin()
  return int(trace_df.loc[idx, "step"])

# --- Option B: Percent-change threshold on MAE (similar can be done for RMSE) ---
def choose_step_by_threshold(trace_df, metric_col="val_mae", min_improvement_pct=1.0):
  # Improvement is measured as percent decrease from previous step to current step
  vals = trace_df[metric_col].to_numpy()
  # Start at step 1 because step 0 has no previous comparison
  for i in range(1, len(vals)):
    prev = vals[i - 1]
    curr = vals[i]
    # Percent improvement (positive means error decreased)
    pct_improve = (prev - curr) / prev * 100.0
    if pct_improve < min_improvement_pct:
      # Stop at the previous step (last meaningful improvement)
      return i - 1
  # If we never drop below threshold, stop at the best (lowest) metric
  return int(trace_df[metric_col].idxmin())

# Choose a stop in either way (examples)
stop_step_a = choose_step_keep_n(trace, n_keep=8)
stop_step_b = choose_step_by_threshold(trace, metric_col="val_rmse", min_improvement_pct=0.01)

print("Stop step (keep N):", stop_step_a)
print("Stop step (threshold):", stop_step_b)

# Pull the selected features from the trace
selected_features_a = trace.loc[trace["step"] == stop_step_a, "remaining_features"].iloc[0]
selected_features_b = trace.loc[trace["step"] == stop_step_b, "remaining_features"].iloc[0]

print("Selected features (keep N):", selected_features_a)
print("Selected features (threshold):", selected_features_b)

# Output:
# Stop step (keep N): 5
# Stop step (threshold): 4
# Selected features (keep N): ['sex', 'bmi', 'children', 'smoker', 'region', 'age_sq', 'bmi_ln', 'bmi_ln_x_smoker']
# Selected features (threshold): ['sex', 'bmi', 'children', 'smoker', 'region', 'smoker_yes', 'age_sq', 'bmi_ln', 'bmi_ln_x_smoker']
```

#### Fit the final model and store it asfinal_model

Up to this point, we have trained many temporary models to compare feature removals, but we have not saved a single “final” model object. In the code below, we (1) choose a stopping step, (2) extract the remaining features at that step, and (3) refit one pipeline on the combined training+validation data. We store that trained pipeline as _final_model_, which we will reuse in the next section to make out-of-sample predictions.

```python
# Assumes you already ran earlier code and have: trace, X_train_full, X_test, y_train_full, y_test, and make_model()

stop_step = choose_step_keep_n(trace, n_keep=8)
selected_features = trace.loc[trace["step"] == stop_step, "remaining_features"].iloc[0]

final_model = make_model(selected_features)
final_model.fit(X_train_full[selected_features], y_train_full)

y_pred = final_model.predict(X_test[selected_features])

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print("Stop step:", stop_step)
print("Selected features:", selected_features)
print("Test MAE:", round(mae, 2))
print("Test RMSE:", round(rmse, 2))
```

The key output from this cell is the trained pipeline stored in _final_model_ and the list of _selected_features_. In the next section, we will create a one-row DataFrame with those same feature names and call _final_model.predict()_ to generate a prediction for a brand-new case.

In this chapter, we used a validation set specifically to guide feature removal decisions. That means test-set evaluation should be saved for the end, after you commit to a final model.

The greedy backward approach used here is intentionally simple and transparent, making it ideal for learning the mechanics of predictive feature selection. However, it is not the most robust method available. In later chapters, you will improve this workflow using cross-validation and more reliable feature importance techniques such as _Permutation Feature Importance (PFI)_, which estimates a feature’s contribution by measuring how much prediction error increases when that feature is randomly shuffled. These methods provide stronger evidence of true predictive value, especially in high-dimensional or highly correlated datasets. For now, the key takeaway is that predictive feature selection should be guided by _out-of-sample error behavior_, not by p-values, and that this chapter’s approach should be viewed as a conceptual foundation rather than a final best practice.

---

## 11.10 Making Predictions

After you train a predictive model, the next practical question is: how do you use it to make predictions for a new, unseen case? In scikit-learn, this is done with the _predict()_ method.

The most important idea is that you should not manually redo your preprocessing steps. Instead, you know your model is a pipeline (preprocessing + regression), so you can pass raw inputs in the same format as your original _X_ matrix and let the pipeline apply the exact same transformations automatically.

#### Why pipelines matter for prediction

When you trained your pipeline, it learned everything needed to transform raw inputs into the model’s internal feature representation. That includes numeric imputation and scaling, categorical encoding, and any other preprocessing steps you included. If you skip the pipeline and try to transform inputs manually, it is easy to apply slightly different rules and accidentally produce inconsistent predictions.

#### Prediction workflow

- Collect inputs for a single new case (for example, age, BMI, smoker status, etc.).
- Store those inputs in a one-row Pandas DataFrame with the same column names as the training _X_.
- Call _pipeline.predict(new_case_df)_ to generate the prediction.

#### Example: interactive input and prediction (console)

The code below demonstrates a simple way to collect user input, build a one-row DataFrame, and generate a prediction. It assumes you already trained a pipeline named _final_model_ (the same kind of pipeline used in this chapter), and that the pipeline expects the same raw columns that exist in _X_.

```python
import pandas as pd

# Assumes you already trained this earlier in the section:
# final_model = Pipeline(steps=[("prep", preprocessor), ("lr", LinearRegression())])
# and that your training features were stored as:
# feature_cols = X.columns.tolist()

def get_float(prompt):
  return float(input(prompt))

def get_int(prompt):
  return int(input(prompt))

def get_str(prompt):
  return input(prompt).strip()

# Example (insurance-style) inputs; adjust these prompts to match your dataset columns
age = get_int("Enter age (e.g., 37): ")
bmi = get_float("Enter BMI (e.g., 28.5): ")
children = get_int("Enter number of children (e.g., 2): ")
sex = get_str("Enter sex (male/female): ")
smoker = get_str("Smoker? (yes/no): ")
region = get_str("Enter region (northeast/northwest/southeast/southwest): ")

new_case = pd.DataFrame([{
  "age": age,
  "bmi": bmi,
  "children": children,
  "sex": sex,
  "smoker": smoker,
  "region": region
}])

# If your X included engineered features, rebuild them here in the same way
# (Only needed if engineered columns were explicitly added to X before training.)
new_case["smoker_yes"] = (new_case["smoker"] == "yes").astype(int)
new_case["age_sq"] = new_case["age"] ** 2
new_case["bmi_ln"] = (new_case["bmi"]).apply(lambda v: __import__("numpy").log(v))
new_case["age_x_smoker"] = new_case["age"] * new_case["smoker_yes"]
new_case["bmi_x_smoker"] = new_case["bmi"] * new_case["smoker_yes"]
new_case["age_sq_x_smoker"] = new_case["age_sq"] * new_case["smoker_yes"]
new_case["bmi_ln_x_smoker"] = new_case["bmi_ln"] * new_case["smoker_yes"]

pred = final_model.predict(new_case)[0]
print("Predicted value:", round(pred, 2))
```

#### How this connects to deployment

This interactive example is not meant to be a final user interface. Its purpose is to show the mechanics: collect raw inputs, create a one-row DataFrame with matching column names, and pass it into _predict()_. Later, when you deploy models into apps or websites, the same logic still applies—your app will gather inputs through a form instead of _input()_, but the pipeline will still transform and predict in exactly the same way.

Practical tip: Save the list of training columns (for example, _feature_cols = X.columns.tolist()_) so you can quickly verify that your prediction DataFrame has the same columns. A mismatch in column names is one of the most common causes of prediction errors during deployment.

---

## 11.11 Case Studies

Try the practice problems below to see how well you understand the chapter content.

This practice uses the **Diamonds** dataset that ships with the Seaborn Python package. Your goal is to build a **predictive** regression model that estimates diamond _price_ as accurately as possible on new data. Unlike causal modeling, you will evaluate success using _MAE_ and _RMSE_ on holdout data and you will use a _greedy backward feature removal_ workflow to reduce overfitting.

**Dataset attribution:** The Diamonds dataset is distributed with the Seaborn data repository and can be loaded with _seaborn.load_dataset("diamonds")_. If you want the underlying CSV source, Seaborn hosts it in its public GitHub repository under _seaborn-data_.

To load the dataset, use this code:

```python
import pandas as pd
import seaborn as sns

df = sns.load_dataset("diamonds")
df.head()
```

**Prediction goal:** Predict _price_ using a mix of numeric features (_carat_, _depth_, _table_, _x_, _y_, _z_) and categorical features (_cut_, _color_, _clarity_). Use a predictive workflow: train/validation/test splits, preprocessing in an sklearn pipeline, and error-based evaluation.

**Tasks**

- Inspect the dataset: rows/columns, data types, and summary statistics for _price_.
- Create _X_ and _y_ where _y = price_ and _X_ includes the predictors listed above. Do not use any columns that trivially reveal the label (none should in this dataset), and document your chosen feature set.
- Split your data into _train_, _validation_, and _test_ sets (roughly 60/20/20 overall). The validation set will be used for greedy feature-removal decisions, and the test set must be held until the end.
- Build an sklearn preprocessing pipeline that (a) scales numeric predictors (StandardScaler) and (b) one-hot encodes categorical predictors (OneHotEncoder with _handle_unknown="ignore"_). Fit preprocessing only on training data via the pipeline to prevent leakage.
- Establish a baseline error on the validation set by predicting the _training mean_ of _price_. Report baseline _MAE_ and _RMSE_.
- Fit a baseline predictive linear regression model (with all features) using your pipeline. Report validation _MAE_, validation _RMSE_, and test-set metrics only at the end.
- **Feature engineering (before selection):** Create at least two interpretable nonlinear numeric features (for example, _carat_sq_ and _log_carat_, or _x_sq_ and _y_sq_). If you create a log term, ensure the input is positive (use clipping if needed). Add these engineered features to _X_ before feature selection.
- Run _greedy backward feature removal_ using the validation set: at each step, try removing each remaining feature one-at-a-time, refit the model, and choose the removal that yields the lowest validation _RMSE_ (use MAE as a tie-breaker). Record a trace table with _step_, _removed_feature_, _n_features_, _val_mae_, and _val_rmse_.
- Create a line plot of validation _MAE_ and _RMSE_ across greedy steps. Add a label above each point showing which feature was removed at that step. Highlight the lowest MAE point and the lowest RMSE point to make them easy to identify.
- Choose a stopping point using one evidence-based rule (for example, keep N features, or stop when improvement falls below a percent threshold) and one domain/judgment-based reason (for example, “we stopped before removing _carat_ because it is core to the pricing mechanism”). Document your final selected feature set.
- Refit the final model using the selected features (training + validation can be recombined after the decision). Evaluate once on the untouched _test_ set and report test _MAE_ and test _RMSE_.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Diamonds dataset?
1. What is the mean value of _price_ in the dataset?
1. What were your baseline (mean-predictor) validation _MAE_ and validation _RMSE_? Report both values rounded to 2 decimals.
1. For your full-feature linear regression model (with preprocessing), what were validation _MAE_ and validation _RMSE_? Report both values rounded to 2 decimals.
1. List the nonlinear engineered features you created. Did adding them improve validation _RMSE_ relative to the full-feature model without engineering? Answer yes/no and report the two RMSE values (2 decimals).
1. In your greedy removal trace, at which step did validation _RMSE_ reach its minimum? Provide (a) the step number, (b) the minimum validation RMSE value (2 decimals), and (c) the number of features remaining at that step.
1. Name the first three features removed by the greedy procedure (steps 1–3). Why might these features have been the easiest to remove without harming validation error?
1. What stopping criterion did you use (keep N, percent threshold, or elbow judgment)? State it clearly and list your final selected feature set.
1. What are the final model’s _test MAE_ and _test RMSE_? Report both values to 2 decimals and compare them to the validation values at your chosen stopping point (better, worse, or similar?).
1. Short reflection (3–5 sentences): Why do we use a validation set during greedy feature removal and reserve the test set for one final evaluation? What would go wrong if we repeatedly used the test set to choose features?

### Diamonds Predictive Practice Answers

These answers were computed using the Diamonds dataset and a predictive regression workflow with a 60/20/20 split (train/validation/test) using _random_state=42_. The model was a scikit-learn pipeline: numeric median imputation + standard scaling; categorical most-frequent imputation + one-hot encoding; then _LinearRegression_. Greedy backward feature removal was performed using validation-set _RMSE_ as the primary criterion (tie-break on _MAE_).

1. The Diamonds dataset contains _53940_ rows and _10_ columns.
1. The mean value of _price_ is _3932.7997_.
1. (Baseline) Predicting the training-mean price for every observation yields test-set _MAE = 3033.0615_ and _RMSE = 3967.0696_ (with _R² = -0.0000_ up to rounding).
1. (Greedy removal, first step) The first feature removed by the greedy rule was _y_ (diamond width), because removing it produced the lowest validation error among all single-feature removals at that step.
1. (Stopping point) Both validation _MAE_ and validation _RMSE_ reached their minimum at _step 1_ (after removing _y_), so a reasonable stopping point is immediately after that first removal.
1. (Selected feature set at the stop) The remaining raw input features were _carat_, _cut_, _color_, _clarity_, _depth_, _table_, _x_, and _z_ (i.e., all predictors except _y_).
1. (Final predictive performance) After selecting the stopping point, refitting the pipeline on the combined training+validation data, and evaluating once on the untouched test set, the model achieved _MAE = 806.6730_, _RMSE = 1250.6809_, and _R² = 0.9197_ on the test set.
1. (Interpretation) The selected model dramatically outperforms the mean-baseline because it uses meaningful predictors (carat, cut, color, clarity, and dimensions) that explain most of the systematic variation in price, reducing both average error (MAE) and large-miss error (RMSE).

This practice uses the **Red Wine Quality** dataset (_winequality-red.csv_). You will extend your earlier multiple linear regression work by applying a **predictive modeling workflow** based on train/validation/test splits and _greedy backward feature removal_.

**Dataset attribution:** This dataset originates from the UCI Machine Learning Repository (Wine Quality Data Set) and was published by Cortez et al. in “Modeling wine preferences by data mining from physicochemical properties” (Decision Support Systems, 2009). It contains physicochemical measurements of red wines along with a sensory quality score.

The red wine quality dataset is available in the prior chapter if you need it.

In this chapter, you are practicing **MLR for predictive inference**. Unlike causal modeling, your goal is not to interpret coefficients or test hypotheses, but to build a model that generalizes well to new data by minimizing _out-of-sample prediction error_.

**Tasks**

- Inspect the dataset: rows, columns, data types, and summary statistics for _quality_.
- Create a label vector _y_ using _quality_ and a feature matrix _X_ using all remaining numeric predictors.
- Split the data into three sets: training (60%), validation (20%), and test (20%) using fixed random seeds.
- Compute a baseline model that predicts the training-set mean of _quality_ for every observation. Evaluate its MAE, RMSE, and R² on the test set.
- Build a preprocessing + linear regression pipeline that standardizes numeric features and fits an MLR model.
- Apply greedy backward feature removal using validation-set RMSE (tie-break using MAE) to generate a full removal trace.
- Plot validation MAE and RMSE across removal steps and identify a reasonable stopping point.
- Refit the final model using the selected feature set on the combined training + validation data.
- Evaluate the final model once on the untouched test set.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Red Wine Quality dataset?
1. What is the mean value of _quality_?
1. (Baseline) What are the test-set MAE, RMSE, and R² when predicting the training-set mean for every observation?
1. (Greedy removal) Which feature is removed first by the greedy backward procedure?
1. (Stopping point) At which step do validation MAE and RMSE reach their minimum values?
1. (Selected features) Which raw input features remain at the chosen stopping point?
1. (Final model) What are the test-set MAE, RMSE, and R² of the final selected model?
1. (Interpretation) In 2–3 sentences, explain why the selected model outperforms the baseline mean predictor.

### Red Wine Quality Predictive Practice Answers

These answers were computed by loading _winequality-red.csv_, splitting the data into train/validation/test sets (60/20/20 with _random_state=42_), fitting a predictive regression pipeline (impute median + standardize + linear regression), and using greedy backward feature removal based on validation-set error (MAE and RMSE).

1. The Red Wine Quality dataset contains _1599_ rows and _12_ columns.
1. The mean value of _quality_ is _5.6360_.
1. (Baseline) Predicting the training-mean quality for every observation yields test-set _MAE = 0.6371_ and _RMSE = 0.8282_ (with _R² = 0.0000_ up to rounding).
1. (Greedy removal, first step) The first feature removed by the greedy rule was _fixed acidity_, because removing it produced the lowest validation error among all single-feature removals at that step.
1. (Stopping point) Validation _MAE_ reached its minimum at _step 4_, and validation _RMSE_ also reached its minimum at _step 4_ (after removing _volatile acidity_ at that step), so a reasonable stopping point is _step 4_.
1. (Selected feature set at the stop) The remaining raw input features were _citric acid_, _chlorides_, _free sulfur dioxide_, _total sulfur dioxide_, _pH_, _sulphates_, and _alcohol_.
1. (Final predictive performance) After selecting the stopping point, refitting the pipeline on the combined training+validation data, and evaluating once on the untouched test set, the model achieved _MAE = 0.5149_, _RMSE = 0.6592_, and _R² = 0.3659_ on the test set.
1. (Interpretation) The selected model outperforms the mean-baseline because it uses informative physicochemical predictors (especially _alcohol_, _sulphates_, acidity-related measures, and sulfur dioxide measures) that capture systematic differences in quality; greedy removal reduces noise features that do not improve validation-set MAE/RMSE.

This practice uses the **Bike Sharing** daily dataset (_day.csv_). You will build a predictive multiple linear regression model for total daily rentals (_cnt_) using the full predictive workflow from Chapter 11, including train/validation/test splits, preprocessing pipelines, baseline comparison, feature engineering, and greedy backward feature removal.

**Dataset attribution:** This dataset is distributed as part of the Bike Sharing Dataset hosted by the UCI Machine Learning Repository (Fanaee-T and Gama). It includes daily rental counts and weather/context variables derived from the Capital Bikeshare system in Washington, D.C. You will use the _day.csv_ file provided with your course materials.

The bike sharing daily dataset is available in the prior chapter if you need it.

**Important modeling note:** Do not include _casual_ or _registered_ as predictors because they directly sum to _cnt_ and would leak the answer into the model.

**Goal:** Build a predictive regression model that minimizes out-of-sample error (MAE and RMSE), not one that maximizes statistical significance or interpretability.

**Tasks**

- Inspect the dataset: number of rows, number of columns, and summary statistics for _cnt_.
- Define predictors using the following raw features: _season_, _yr_, _mnth_, _holiday_, _weekday_, _workingday_, _weathersit_, _temp_, _atemp_, _hum_, and _windspeed_.
- Split the data into training (60%), validation (20%), and test (20%) sets using random_state = 42.
- Construct a preprocessing pipeline that imputes missing values, standardizes numeric variables, and one-hot encodes categorical variables.
- Compute a baseline model that predicts the training-set mean of _cnt_ for all observations and evaluate its test-set MAE, RMSE, and R².
- Apply greedy backward feature removal using validation-set MAE and RMSE to determine which features to remove and where to stop.
- Refit the final selected model on the combined training + validation data and evaluate it once on the test set.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Bike Sharing daily dataset?
1. What is the mean value of _cnt_?
1. (Baseline) What are the test-set MAE, RMSE, and R² when predicting the training-set mean for every observation?
1. (Greedy removal) Which feature is removed first by the greedy backward procedure?
1. (Stopping point) At which step do validation MAE and RMSE reach their minimum values?
1. (Selected features) Which raw input features remain at the chosen stopping point?
1. (Final model) What are the test-set MAE, RMSE, and R² of the final selected model?
1. (Interpretation) In 2–3 sentences, explain why the selected model outperforms the baseline mean predictor.

### Bike Sharing (Chapter 11) Practice Answers

These answers were computed using the Chapter 11 predictive workflow: (1) a 60/20/20 split (train/validation/test) with _random_state=42_, (2) a preprocessing pipeline that standardizes numeric predictors and one-hot encodes categorical predictors (_season_, _mnth_, _weekday_, _weathersit_), and (3) greedy backward feature removal based on validation-set _MAE_ and _RMSE_.

1. The Bike Sharing daily dataset contains _731_ rows and _16_ columns.
1. The mean value of _cnt_ is _4504.3488_.
1. (Baseline) Predicting the training-set mean _cnt_ for every observation yields test-set _MAE = 1711.9909_ and _RMSE = 2022.1728_ (with _R² = -0.0198_).
1. (Greedy removal, first step) The first raw input feature removed by the greedy rule was _weekday_, because removing it produced the lowest validation error among all single-feature removals at that step.
1. (Stopping point) Both validation _MAE_ and validation _RMSE_ reached their minimum at _step 4_ in this run.
1. (Selected features at the stop) The remaining raw input features were _season_, _yr_, _mnth_, _holiday_, _weathersit_, _temp_, and _windspeed_.
1. (Final predictive performance) After selecting the stopping point, refitting the pipeline on the combined training+validation data, and evaluating once on the untouched test set, the model achieved _MAE = 613.3657_, _RMSE = 825.0000_, and _R² = 0.8303_ on the test set.
1. (Interpretation) The selected model strongly outperforms the mean-baseline because it uses real predictive signals (seasonality, year trend, weather conditions, and temperature/wind effects) that explain systematic variation in daily rentals. Removing weaker features reduces noise and can improve generalization, lowering both average error (MAE) and large-miss error (RMSE).

---

## 11.12 Assignment

Complete the assignment below:

### 11.12 MLR Prediction

- Understand the difference between explanatory and predictive modeling
- Implement train/test splits to measure generalization
- Build predictive models using sklearn pipelines
- Evaluate models using out-of-sample metrics (MAE, RMSE)
- Detect and understand overfitting
- Compare train vs test performance

- **SalePrice** – Sale price of the property in dollars (target variable)
- **MSSubClass** – Building class
- **MSZoning** – Zoning classification
- **LotFrontage** – Street frontage
- **LotArea** – Lot size
- **Street** – Road type
- **Alley** – Alley access
- **LotShape** – Property shape
- **LandContour** – Flatness
- **Utilities** – Utilities available
- **LotConfig** – Lot configuration
- **LandSlope** – Slope
- **Neighborhood** – Location in Ames
- **Condition1** – Proximity to road/railroad
- **Condition2** – Secondary proximity
- **BldgType** – Dwelling type
- **HouseStyle** – House style
- **OverallQual** – Overall quality
- **OverallCond** – Overall condition
- **YearBuilt** – Construction year
- **YearRemodAdd** – Remodel year
- **RoofStyle** – Roof type
- **RoofMatl** – Roof material
- **Exterior1st** – Exterior covering
- **Exterior2nd** – Secondary exterior covering
- **MasVnrType** – Masonry veneer type
- **MasVnrArea** – Masonry veneer area
- **ExterQual** – Exterior quality
- **ExterCond** – Exterior condition
- **Foundation** – Foundation type
- **BsmtQual** – Basement quality
- **BsmtCond** – Basement condition
- **BsmtExposure** – Basement exposure
- **BsmtFinType1** – Basement finish type
- **BsmtFinSF1** – Finished basement area
- **BsmtFinType2** – Secondary finish type
- **BsmtFinSF2** – Secondary finished area
- **BsmtUnfSF** – Unfinished basement area
- **TotalBsmtSF** – Total basement area
- **Heating** – Heating type
- **HeatingQC** – Heating quality
- **CentralAir** – Central air
- **Electrical** – Electrical system
- **1stFlrSF** – First floor area
- **2ndFlrSF** – Second floor area
- **LowQualFinSF** – Low quality finished area
- **TotalSF** – Total square footage
- **BsmtFullBath** – Basement full baths
- **BsmtHalfBath** – Basement half baths
- **FullBath** – Full baths
- **HalfBath** – Half baths
- **BedroomAbvGr** – Bedrooms above grade
- **KitchenAbvGr** – Kitchens above grade
- **KitchenQual** – Kitchen quality
- **TotRmsAbvGrd** – Total rooms above grade
- **Functional** – Home functionality
- **Fireplaces** – Fireplaces
- **FireplaceQu** – Fireplace quality
- **GarageType** – Garage type
- **GarageYrBlt** – Garage build year
- **GarageFinish** – Garage finish
- **GarageCars** – Garage capacity
- **GarageArea** – Garage area
- **GarageQual** – Garage quality
- **GarageCond** – Garage condition
- **PavedDrive** – Driveway type
- **WoodDeckSF** – Deck area
- **OpenPorchSF** – Open porch area
- **EnclosedPorch** – Enclosed porch area
- **3SsnPorch** – Three-season porch area
- **ScreenPorch** – Screen porch area
- **PoolArea** – Pool area
- **PoolQC** – Pool quality
- **Fence** – Fence quality
- **MiscFeature** – Miscellaneous feature
- **MiscVal** – Miscellaneous value
- **MoSold** – Month sold
- **YrSold** – Year sold
- **SaleType** – Sale type
- **SaleCondition** – Sale condition

- Answer all questions below by completing the associated tasks
- Use the notebook (.ipynb) file template below so that all code cells include the "# Question [n]" comment
- Upload your completed notebook with all code cells run with output showing where directed below

---
