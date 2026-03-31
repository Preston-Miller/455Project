# Chapter 9: MLR Concepts and Mechanics

## Learning Objectives

- Students will be able to explain how multiple linear regression extends simple linear regression to estimate the conditional effect of each feature while holding others constant
- Students will be able to implement multiple linear regression in both Excel and Python (statsmodels) and interpret coefficient estimates, p-values, and R- squared
- Students will be able to dummy-code categorical variables and explain why one category must be dropped to avoid multicollinearity
- Students will be able to standardize numeric features to enable meaningful comparison of coefficient magnitudes across variables
- Students will be able to compute and interpret in-sample error metrics (MAE, RMSE) for regression models

---

## 9.1 Introduction

![Educational diagram illustrating the progression from simple linear regression to multiple linear regression. On the left, a single predictor variable (X₁) points to an outcome variable (Y), represented by a straight regression line. On the right, multiple predictor variables (X₁, X₂, X₃) converge toward the same outcome (Y), forming a shaded regression plane, visually emphasizing how multiple linear regression incorporates several inputs simultaneously to explain or predict an outcome.](../Images/Chapter9_images/MLR concept.png)

![Animated 3D Trisurface Plot](../Images/Chapter9_images/trisurface.gif)

![Animated 3D Scatterplot](../Images/Chapter9_images/scatter.gif)

![Animated 3D Plane Prediction](../Images/Chapter9_images/plane.gif)

Now that you have learned how to collect, explore, and clean data, the next step in the data project process is to analyze that data in order to extract insights and generate predictions about outcomes of interest. This stage of the process is where modeling becomes central.

If you have taken a basic statistics course in high school or college (or even just algebra), you have likely worked with descriptive statistics and visualizations involving single variables (univariate analysis) and relationships between pairs of variables (bivariate analysis). While these approaches are useful for exploration and intuition, they are limited because they cannot account for the combined and interdependent effects of multiple features acting simultaneously.

**Modeling** — The process of developing mathematical or computational functions that quantify the relationship between multiple input features and an outcome of interest. Modeling involves creating functions that combine multiple features into a single equation. _Model training_ refers to the process of using historical data to estimate the parameters (weights) that determine how strongly each feature contributes to the outcome. Once trained, a model can be applied to new data to explain relationships or to generate predictions for new observations.

Multiple linear regression is one of the most widely used modeling techniques because it serves two distinct but related purposes, depending on the analyst’s goal:

- **Regression for causal (explanatory) inference:** The goal is to understand how changes in individual features are associated with changes in the outcome, holding other variables constant. In this setting, coefficient interpretation, statistical significance, and regression assumptions play a central role.
- **Regression for predictive inference:** The goal is to generate accurate predictions for new or unseen data. In this setting, predictive performance, generalization, and error metrics matter more than strict adherence to classical regression assumptions.

Although the same mathematical model underlies both uses, the questions we ask, the assumptions we emphasize, and the metrics we focus on differ substantially. This chapter begins by introducing the core concepts and mechanics of multiple linear regression. Subsequent chapters will then explore how regression is used differently for explanation versus prediction.

To illustrate the idea of multivariate modeling, consider the insurance dataset shown below. Earlier, you learned how to fit a regression line when predicting an outcome from a single feature. When predicting _charges_ using two features—_age_ and _BMI_—the fitted model is no longer a line but a plane in three-dimensional space. Multiple linear regression generalizes this idea to many features, producing a multidimensional surface that best fits the data.

This chapter introduces one of the oldest, most interpretable, and still most widely used modeling techniques in analytics: _Multiple Linear Regression (MLR)_. Although modern data science increasingly relies on more complex machine learning models, linear regression remains foundational because it provides transparency, interpretability, and a clear link between data, assumptions, and decisions. Mastering MLR provides essential intuition that carries forward into more advanced modeling approaches.

---

## 9.2 Linear Regression

#### Background Theory

To understand multiple linear regression, it is useful to briefly review linear regression. You may recognize the familiar equation _y_ = m*x* + b from algebra courses, where _m_ represents the slope of a line and _b_ represents the y-intercept. In data analytics, linear regression is less about drawing a line and more about estimating how changes in one variable are associated with changes in an outcome, on average, using historical data.

A residual (i.e., error) is the difference between an actual value and the value predicted by the regression line. Each plotted point represents a single case (a row in a dataset) with values for one input feature and one outcome. For example, if _y_ represents income and _x_ represents age, each point reflects an individual’s observed age and income. The fitted regression line—also called a line of best fit or trendline—produces predicted _y_ values for given _x_ values.

Linear regression chooses the line that minimizes the sum of squared residuals. Residuals are squared so that negative and positive errors do not cancel each other out and so that larger errors are penalized more heavily. Minimizing squared error also produces a unique, stable solution that can be computed efficiently, even for large datasets.

**Model** — A formula—typically composed of a set of weights, a constant, and an error term—that estimates the expected value of an outcome given input features. A model uses historical data to estimate relationships between inputs and outcomes. These relationships are not guaranteed rules, but approximations that include uncertainty and error.

Suppose we train a simple linear regression model using age to predict income and obtain the following equation:

If you would like a refresher on how slope and intercept are calculated, optional review videos are available on Khan Academy, including calculating slope and y-intercept and standard error.

These reviews are optional and not required if you already understand the general concept.

This equation represents an income prediction model trained on historical age and income data. Interpreting the equation:

- _y_ is the predicted income.
- 6000 represents the estimated increase in income for each additional year of age.
- _x_ is the observed age value.
- 5 is the predicted income when age equals zero, which illustrates a mathematical intercept rather than a realistic scenario.

This model can generate predictions for new data by substituting an age value into the equation. For example, the predicted income for a 40-year-old is:

This results in a predicted income of $240,005. The table below shows predictions for ages between 5 and 65.

![Table showing ages from 5 to 65 with corresponding predicted income values from a linear regression equation.](../Images/Chapter9_images/ymxb_demo.png)

Do you trust all of these predictions? While predictions for ages between roughly 45 and 65 may seem reasonable, a predicted income for a 5-year-old clearly is not. These implausible predictions do not mean regression is useless; instead, they indicate that model assumptions may not hold across all input values.

Evaluating assumptions such as linearity, constant variance, and appropriate data ranges is critical before relying on regression results. These ideas become even more important when extending from one predictor to many.

While the core principles of linear regression have remained stable for decades, modern analytics workflows increasingly rely on AI-assisted tools to estimate, validate, and interpret regression models efficiently and at scale.

With this refresher in mind, we now extend these ideas to multiple linear regression, where outcomes are predicted using several features simultaneously.

---

## 9.3 Multiple Linear Regression

**Multiple Linear Regression (MLR)** — A modeling technique that estimates the relationship between a single label and two or more features simultaneously. Multiple Linear Regression (MLR) extends simple linear regression by allowing many features to be used together to predict a single outcome. This is essential in real-world data, where outcomes are rarely driven by only one factor.

This course focuses on understanding, applying, and interpreting MLR rather than deriving it mathematically. Students interested in the formal matrix-based derivation can consult the Linear Regression Wikipedia page.

MLR produces one coefficient (β) for each feature and a single intercept term. This generalizes the familiar straight-line equation (_y_ = m*x* + b) by replacing the single slope with multiple feature-specific weights. For example, a model predicting income (_y_) from age (*x*1), education (*x*2), and years of experience (*x*3) takes the following form:

Each β coefficient represents the _conditional effect_ of its feature—that is, how much the predicted label changes when that feature increases by one unit _while all other features are held constant_. This distinction is critical: unlike correlation, which examines variables in isolation, MLR isolates the unique contribution of each feature.

MLR estimates coefficients by finding the set of values that minimizes the total squared prediction error across all observations. Conceptually, this is the same least-squares principle used in simple linear regression, but applied in a higher-dimensional feature space. Because humans cannot visualize beyond three dimensions, this optimization is performed mathematically rather than graphically.

One of the main advantages of MLR is that it prevents misleading conclusions that arise from analyzing features one at a time. For example, suppose you compute bivariate correlations between income and three predictors: age (_r_ = 0.52), education (_r_ = 0.42), and work experience (_r_ = 0.48). Examining these values alone might suggest that age is the most important predictor.

![Three bivariate correlations between income and age, education, and work experience shown as overlapping circles.](../Images/Chapter9_images/regression_correlations.png)

However, these features are themselves correlated with one another. Adding their correlations together (0.52 + 0.48 + 0.42 = 1.42) would imply more than perfect prediction, which is impossible. This occurs because shared information is being double- and triple-counted. Bivariate correlation cannot distinguish between unique and shared effects.

![Venn diagram showing overlapping variance among age, education, and work experience when predicting income.](../Images/Chapter9_images/MLR - Regression.png)

MLR resolves this issue by estimating the _true_ effect of each feature after accounting for overlap with other features. Individual effect sizes are captured by the β coefficients, while the overall explanatory power of the model is summarized by the coefficient of determination, *R*2.

# R2

1
−

SSres
SStot

*R*2 measures the proportion of variability in the label explained by all features combined, relative to predicting the mean alone. Unlike correlation, it accounts for shared information among predictors and reflects the net explanatory power of the entire model.

Together, β coefficients and *R*2 form the foundation for interpreting regression models. In the sections that follow, you will learn how to compute MLR models in Excel and Python, encode categorical features, scale inputs, test assumptions, and evaluate predictions—tasks that are increasingly automated using modern analytics tools and AI-assisted workflows.

---

## 9.4 MLR in Excel

Multiple Linear Regression can feel abstract for first-time modelers, and implementing it directly in Python introduces additional cognitive load due to syntax, libraries, and debugging. For many students, Excel provides a useful conceptual sandbox where model structure, feature estimates, model fit, multicollinearity, and prediction mechanics are easier to see and reason about before introducing code. If you are already comfortable with MLR concepts, you may skip this section. Otherwise, follow along with the videos below using the insurance.csv dataset.

---

## 9.5 MLR in Python

#### MLR in Statsmodels

Now that you have built and interpreted a Multiple Linear Regression model in Excel, let’s replicate that workflow in Python using the _statsmodels_ package (full documentation). The goal here is not to introduce new concepts, but to show how the same model components—features, coefficients, intercepts, and model fit—appear when implemented programmatically.

As before, we divide the dataset into two parts: (1) the label (_y_) and (2) the set of numeric features (_X_) used to predict that label. We will again use the insurance.csv dataset.

```python
import numpy as np
import pandas as pd
import statsmodels.api as sm

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

# Set label and features
y = df['charges']
X = df.select_dtypes(np.number).assign(const=1)
X = X.drop(columns=['charges'])
X.head()
```

_y_ is a Pandas Series because it contains a single column (the label), while _X_ is a DataFrame containing multiple features. We intentionally restrict _X_ to numeric columns because linear regression cannot operate directly on text data. The statement _.assign(const=1)_ adds a column of ones that allows the model to estimate a y-intercept. In Statsmodels, omitting this column would force the regression plane through the origin, which is rarely appropriate.

Finally, we remove the label column from _X_. Including the label as a feature would trivially produce a perfect fit, which is both misleading and invalid.

Next, we fit the Multiple Linear Regression model using Ordinary Least Squares (OLS):

```python
model = sm.OLS(y, X)
results = model.fit()

print(results.summary())

# Output:
#                             OLS Regression Results
# ==============================================================================
# Dep. Variable:                charges   R-squared:                       0.120
# Model:                            OLS   Adj. R-squared:                  0.118
# Method:                 Least Squares   F-statistic:                     60.69
# Date:                Wed, 12 Feb 2025   Prob (F-statistic):           8.80e-37
# Time:                        20:30:40   Log-Likelihood:                -14392.
# No. Observations:                1338   AIC:                         2.879e+04
# Df Residuals:                    1334   BIC:                         2.881e+04
# Df Model:                           3
# Covariance Type:            nonrobust
# ==============================================================================
#                  coef    std err          t      P>|t|      [0.025      0.975]
# ------------------------------------------------------------------------------
# age          239.9945     22.289     10.767      0.000     196.269     283.720
# bmi          332.0834     51.310      6.472      0.000     231.425     432.741
# children     542.8647    258.241      2.102      0.036      36.261    1049.468
# const      -6916.2433   1757.480     -3.935      0.000   -1.04e+04   -3468.518
# ==============================================================================
# Omnibus:                      325.395   Durbin-Watson:                   2.012
# Prob(Omnibus):                  0.000   Jarque-Bera (JB):              603.372
# Skew:                           1.520   Prob(JB):                    9.54e-132
# Kurtosis:                       4.255   Cond. No.                         290.
# ==============================================================================
#
# Notes:
# [1] Standard Errors assume that the covariance matrix of the errors is correctly specified.
```

The regression summary contains a large amount of information. Rather than interpreting everything at once, we will focus on the most important sections and return to the others later in the chapter.

#### Descriptive Information

The upper-left portion of the output reports basic model metadata, including the dependent variable, estimation method (OLS), number of observations, and degrees of freedom for both the model and residuals. These values confirm that the model was constructed as intended.

#### Overall Model Quality (Model Fit)

In the upper-right portion, _R-squared_ indicates the proportion of variance in insurance charges explained by the features in the model. In this case, _R2_ ≈ 0.12 means the model explains about 12% of the variation in charges. Whether this is “good” depends entirely on the context and on how well alternative models perform.

**Adjusted R-squared** — A version of R-squared that accounts for the number of predictors in the model. Adjusted R-squared increases only when a new feature improves the model beyond what would be expected by chance. The similarity between R-squared and adjusted R-squared here indicates that each included feature contributes meaningfully.

_Log-Likelihood (LL)_, _Akaike Information Criterion (AIC)_, and _Bayesian Information Criterion (BIC)_ provide alternative ways to assess model quality while penalizing unnecessary complexity.

- _Log-Likelihood (LL)_: Higher values indicate a better fit.
- _AIC_: Balances goodness of fit with model complexity; lower is better.
- _BIC_: Similar to AIC but penalizes complexity more strongly.

At this stage, we primarily rely on _R2_ to compare models predicting the same outcome. Feature selection strategies using AIC and BIC will be explored later.

Two additional performance measures not shown in the Statsmodels summary are **Mean Absolute Error (MAE)** — The average absolute difference between predicted and actual values. and **Root Mean Squared Error (RMSE)** — The square root of the average squared prediction error.. These metrics are computed using in-sample predictions meaning that we used the fitted model to make predictions for the same rows that were used to fit the model.

```python
df_insample = pd.DataFrame({
    'Actual': df['charges'],
    'Predicted': model.fittedvalues,
    'Residuals': df['charges'] - model.fittedvalues
})

df_insample.head(10)

# Output:
# 	Actual	    Predicted	Residuals (Error)
# 0	16884.92400	6908.777533	  9976.146467
# 1	1725.55230	9160.977061	  -7435.424761
# 2	4449.46200	12390.946918	-7941.484918
# 3	21984.47061	8543.527095	  13440.943515
# 4	3866.85520	10354.147396	-6487.292196
# 5	3756.62160	9071.411158	  -5314.789558
# 6	8240.58960	15771.234831	-7530.645231
# 7	7281.50560	12804.138689	-5522.633089
# 8	6406.41070	12955.328269	-6548.917569
# 9	28923.13692	16064.459249	12858.677671
```

MAE and RMSE quantify average prediction error, with RMSE placing greater emphasis on large errors. RMSE is therefore more sensitive to outliers and non-normal residuals.

It is important to understand a critical limitation of in-sample metrics like MAE and RMSE: they are calculated using predictions on the _same data_ that was used to train the model.

In-sample metrics can be _optimistic_—meaning they may make the model appear better than it actually is. This happens because the model has already "seen" these data points during training. The model's coefficients were specifically chosen to minimize prediction error on this exact dataset, so it's not surprising that the model performs relatively well when evaluated on the same data.

Think of it like a student taking a practice exam and then immediately retaking the same exam: the second score will likely be higher because the student has already seen the questions and answers. Similarly, a model's in-sample performance may be artificially high because it has already "memorized" patterns specific to the training data.

The real question we want to answer is: _How well will this model perform on new, unseen data?_ In-sample metrics cannot answer this question. A model might have excellent in-sample RMSE (low error on training data) but perform poorly on new data if it has **overfitting** — A modeling problem where the model learns patterns specific to the training data that don't generalize to new data, resulting in good training performance but poor test performance.—meaning it has learned patterns that are specific to the training data rather than generalizable relationships.

To assess how well a model will perform on new data, we need to evaluate it on _out-of-sample_ data—data that was _not_ used during model training. This requires splitting the data into separate training and test sets before building the model, then evaluating performance metrics (like MAE and RMSE) on the test set. This process, along with strategies for preventing overfitting, will be covered in detail in a later chapter.

For now, recognize that in-sample metrics like MAE and RMSE are useful for understanding how well the model fits the training data, but they should not be used to make claims about how the model will perform on new data. Always interpret in-sample metrics with caution, understanding that they may be optimistic and that true predictive performance requires evaluation on unseen test data.

---

## 9.6 Feature Estimates

#### Coefficients (Feature Weights)

Once a multiple linear regression model has been fit, it produces a set of **coefficients (β)** — Estimated weights that quantify the independent effect of each feature on the label after controlling for all other features in the model.. These coefficients appear in the lower half of the _OLS().summary()_ output under the _coef_ column.

Each coefficient represents the expected change in the label associated with a one-unit increase in the corresponding feature, holding all other features constant. This “holding constant” condition is critical: coefficients are not simple correlations, but _controlled effects_ that isolate the portion of each feature’s influence that is not shared with other predictors.

At this stage, coefficients should be interpreted as _initial estimates_. They reflect what the model believes about feature effects given the current specification—but they are not yet guaranteed to be reliable. In later sections, you will learn how diagnostic tests determine whether these estimates can be trusted.

![Conceptual representation of Multiple Linear Regression coefficients showing overlapping feature effects and unique contributions to the label.](../Images/Chapter9_images/MLR_coefficients.png)

Once coefficients are estimated, predictions are generated using the general multiple linear regression equation:

_y_ = β1x1 + β2x2 + β3x3 + b

Based on the fitted model, the equation for predicting insurance charges takes the following form:

_y_ = 239.99(age) + 332.08(BMI) + 542.86(children) − 6916.24

This equation can be used to compute predicted insurance charges for observations in the training data or for new customers. However, prediction accuracy and interpretability both depend on whether the model’s assumptions are satisfied—an issue we will address later.

```python
pd.set_option('display.max_columns', None)

df_formula = pd.DataFrame({
    'Prediction (y)': model.fittedvalues,
    '=': '=',
    'age β': round(model.params.iloc[0], 2),
    '*': '*',
    'age X': df['age'],
    '+': '+',
    'bmi β': round(model.params.iloc[1], 2),
    '* ': '*',
    'bmi X': df['bmi'],
    ' +': '+',
    'kids β': round(model.params.iloc[2], 2),
    ' *': '*',
    'kids X': df['children'],
    ' + ': '+',
    'const': round(model.params.iloc[3], 2)
})

df_formula.head(5)
```

The trained model can also produce **out-of-sample predictions** — Predictions for new cases that were not used during model training. using the _.predict()_ method.

```python
prediction = model.predict([32, 21, 2, 1])[0]
print(f"Predicted charges for age=32, bmi=21, children=2: ${round(prediction, 2)}")
```

The _const_ term represents the y-intercept of the regression equation. It is estimated by including a column of ones when defining _X_. Without this column, the model would be forced through the origin—a restriction that is rarely justified in empirical data.

At this point in the chapter, coefficients should be interpreted cautiously. Multicollinearity, non-linearity, autocorrelation, and heteroscedasticity can all distort coefficient estimates and their uncertainty. In Chapter 2, you will revisit these estimates after diagnosing and correcting assumption violations.

#### Standard Error

The _std err_ column reports the **standard error** — The estimated standard deviation of a coefficient’s sampling distribution, reflecting the precision of the coefficient estimate.. Smaller standard errors indicate more stable estimates, while large standard errors often signal multicollinearity or insufficient information.

#### t-Statistics and p-Values

The _t_-statistic tests whether a coefficient is statistically distinguishable from zero and is computed as the coefficient divided by its standard error. Larger absolute t-values indicate stronger evidence that a feature contributes meaningfully to the model.

Raw coefficient magnitudes can be misleading when features are measured on different scales. Statistical significance (via t-statistics and p-values) helps contextualize coefficient importance—but these values are only meaningful if the underlying model assumptions hold.

The _P>|t|_ column reports p-values that quantify the probability of observing a coefficient as extreme as the one estimated if the true effect were zero. Smaller p-values suggest stronger evidence against the null hypothesis, but they should not be interpreted in isolation.

The _[0.025, 0.975]_ interval provides a 95% confidence interval for each coefficient. In Chapter 2, you will learn how diagnostics affect whether these intervals can be trusted.

---

## 9.7 Categorical Variables

As you know, our MLR did not—and could not—use any categorical variables in the dataset. To include categorical variables, we must convert them into **dummy codes** — Binary (0/1) variables that represent category membership for a categorical feature..

At this stage, our goal is simply to represent categorical information correctly in the model; we will not yet worry about whether the resulting coefficients are fully trustworthy.

We can modify our prior code to generate dummy variables in several ways using Pandas. Let’s begin with the simplest technique: manually choosing the columns to dummy code.

```python
import pandas as pd

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

# Manually enter column names to dummy code
df = pd.get_dummies(df, columns=['sex', 'smoker', 'region'])

df.head()
```

![Partial View of DataFrame with Dummy Codes (cut off for readability)](../Images/Chapter9_images/df_insurance_dummy.png)

The first thing I want you to notice is that _.get_dummies()_ may return values as True/False rather than 1/0. This has changed across Pandas versions because boolean values (True/False) can use less memory than integer values (1/0). The implication is that when we perform MLR, some packages (e.g., _statsmodels.api_) require dummy codes to be 0/1, while others (e.g., _sklearn_) will allow boolean True/False. You will see later that when using Statsmodels, we will cast True/False dummy variables to 0/1. But when we use sklearn to perform MLR, we will leave the dummy codes as True/False.

If we want more automated code that doesn’t require manually entering column names, we can dynamically identify and encode all categorical columns instead of listing them:

```python
df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

# Use .select_dtypes to identify all columns that are categorical ('object' dtype)
dummies = df.select_dtypes(['object']).columns  # Creates a list of categorical column names
df = pd.get_dummies(df, columns=dummies)  # Dummy code all categorical variables

df.head()
```

![Partial View of DataFrame with Dummy Codes (cut off for readability)](../Images/Chapter9_images/df_insurance_dummy.png)

The table above shows the transformed dataset with dummy-coded categorical features. It is truncated for readability, but if you examine it in your notebook, you’ll notice additional columns representing the different regions.

Now, let’s test our MLR model with these new dummy-coded features included.

```python
import statsmodels.api as sm

# Set label and features
y = df['charges']
X = df.drop(columns=['charges']).assign(const=1) # .assign(const=1) is a shorthand method of creating a column of all 1s
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int) # Convert True/False to 1/0

# Run the multiple linear regression model
model = sm.OLS(y, X).fit()
print(model.summary())  # View results

# Output:
#                             OLS Regression Results
#  ==============================================================================
#  Dep. Variable:                charges   R-squared:                       0.751
#  Model:                            OLS   Adj. R-squared:                  0.749
#  Method:                 Least Squares   F-statistic:                     500.8
#  Date:                Mon, 11 Sep 2023   Prob (F-statistic):               0.00
#  Time:                        18:55:02   Log-Likelihood:                -13548.
#  No. Observations:                1338   AIC:                         2.711e+04
#  Df Residuals:                    1329   BIC:                         2.716e+04
#  Df Model:                           8
#  Covariance Type:            nonrobust
#  ====================================================================================
#                         coef    std err          t      P>|t|      [0.025      0.975]
#  ------------------------------------------------------------------------------------
#  age                256.8564     11.899     21.587      0.000     233.514     280.199
#  bmi                339.1935     28.599     11.860      0.000     283.088     395.298
#  children           475.5005    137.804      3.451      0.001     205.163     745.838
#  sex_female         -82.5512    269.226     -0.307      0.759    -610.706     445.604
#  sex_male          -213.8656    274.976     -0.778      0.437    -753.299     325.568
#  smoker_no        -1.207e+04    282.338    -42.759      0.000   -1.26e+04   -1.15e+04
#  smoker_yes        1.178e+04    313.530     37.560      0.000    1.12e+04    1.24e+04
#  region_northeast   512.9050    300.348      1.708      0.088     -76.303    1102.113
#  region_northwest   159.9411    301.334      0.531      0.596    -431.201     751.083
#  region_southeast  -522.1170    330.759     -1.579      0.115   -1170.983     126.749
#  region_southwest  -447.1459    310.933     -1.438      0.151   -1057.119     162.827
#  const             -296.4168    430.507     -0.689      0.491   -1140.964     548.130
#  ==============================================================================
#  Omnibus:                      300.366   Durbin-Watson:                   2.088
#  Prob(Omnibus):                  0.000   Jarque-Bera (JB):              718.887
#  Skew:                           1.211   Prob(JB):                    7.86e-157
#  Kurtosis:                       5.651   Cond. No.                     7.13e+17
#  ==============================================================================

#  Notes:
#  [1] Standard Errors assume that the covariance matrix of the errors is correctly specified.
#  [2] The smallest eigenvalue is 6.91e-30. This might indicate that there are
#  strong multicollinearity problems or that the design matrix is singular.
```

Take a look at the results. What do you see? Some aspects have improved, while others have worsened. First, _R2_ has increased significantly from 12% to 75%, which is great. However, the assumption tests at the bottom remain essentially unchanged because we only added more features without transforming any of them. In fact, the condition number (_Cond. No._) has increased dramatically to _7.13e+17_, signaling severe multicollinearity. Additionally, the notes at the bottom of the output warn that the design matrix may be singular, which is another symptom of redundancy in the feature set.

This occurs because we included redundant dummy variables. For example, _smoker_no_ and _smoker_yes_ provide the same information—if _smoker_yes_ is 1, then _smoker_no_ must be 0, and vice versa. Including both creates perfect multicollinearity. To resolve this, we remove one dummy-coded category (a reference group) for each categorical feature. In Pandas, we can do this by setting _drop_first=True_ when generating dummy variables.

```python
df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

# Generate dummy variables for all categorical columns while dropping the first category
df = pd.get_dummies(df, columns=df.select_dtypes(['object']).columns, drop_first=True)

df.head()
```

![DataFrame with Dummy Codes After Dropping First](../Images/Chapter9_images/df_insurance_dummy_reduced.png)

Now, let’s rerun our model after removing the redundant dummy-coded categories.

```python
# Set label and features
y = df['charges']
X = df.drop(columns=['charges']).assign(const=1)
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

# Generate model results
model = sm.OLS(y, X).fit()
print(model.summary())

# Output:
#                             OLS Regression Results
#  ==============================================================================
#  Dep. Variable:                charges   R-squared:                       0.751
#  Model:                            OLS   Adj. R-squared:                  0.749
#  Method:                 Least Squares   F-statistic:                     500.8
#  Date:                Wed, 12 Feb 2025   Prob (F-statistic):               0.00
#  Time:                        17:41:02   Log-Likelihood:                -13548.
#  No. Observations:                1338   AIC:                         2.711e+04
#  Df Residuals:                    1329   BIC:                         2.716e+04
#  Df Model:                           8
#  Covariance Type:            nonrobust
#  ====================================================================================
#                         coef    std err          t      P>|t|      [0.025      0.975]
#  ------------------------------------------------------------------------------------
#  age                256.8564     11.899     21.587      0.000     233.514     280.199
#  bmi                339.1935     28.599     11.860      0.000     283.088     395.298
#  children           475.5005    137.804      3.451      0.001     205.163     745.838
#  sex_female         -82.5512    269.226     -0.307      0.759    -610.706     445.604
#  sex_male          -213.8656    274.976     -0.778      0.437    -753.299     325.568
#  smoker_no        -1.207e+04    282.338    -42.759      0.000   -1.26e+04   -1.15e+04
#  smoker_yes        1.178e+04    313.530     37.560      0.000    1.12e+04    1.24e+04
#  region_northeast   512.9050    300.348      1.708      0.088     -76.303    1102.113
#  region_northwest   159.9411    301.334      0.531      0.596    -431.201     751.083
#  region_southeast  -522.1170    330.759     -1.579      0.115   -1170.983     126.749
#  region_southwest  -447.1459    310.933     -1.438      0.151   -1057.119     162.827
#  const             -296.4168    430.507     -0.689      0.491   -1140.964     548.130
#  ==============================================================================
#  Omnibus:                      300.366   Durbin-Watson:                   2.088
#  Prob(Omnibus):                  0.000   Jarque-Bera (JB):              718.887
#  Skew:                           1.211   Prob(JB):                    7.86e-157
#  Kurtosis:                       5.651   Cond. No.                     7.13e+17
#  ==============================================================================
#
#  Notes:
#  [1] Standard Errors assume that the covariance matrix of the errors is correctly specified.
#  [2] The smallest eigenvalue is 6.91e-30. This might indicate that there are
#  strong multicollinearity problems or that the design matrix is singular.
```

What changed? We now have fewer features in the model because we removed one category per categorical feature (the reference category). The _R2_ value remains the same, but the condition number (_Cond. No._) drops dramatically (from about _7.13e+17_ to roughly _311_ in this example), which indicates the redundancy problem has been reduced. Interpreting dummy-coded coefficients also becomes more meaningful because each coefficient is now measured relative to its reference group.

Finally, let’s check whether our _MAE_ and _RMSE_ values have improved.

```python
# Calculate Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE)
print(f"MAE:\t${abs(model.fittedvalues - y).mean():.2f}")
print(f"RMSE:\t${((model.fittedvalues - y)**2).mean() ** (1/2):.2f}")

# Output
# MAE:	$4170.89
# RMSE:	$6041.68
```

This model predicts much better than the earlier numeric-only model. Our _MAE_ decreases from 9000+ to about 4170, and our _RMSE_ drops to around 6041, indicating a substantial improvement in fit.

In the next chapter, we will use diagnostics like VIF and residual analysis to further refine which categorical features we should trust and which should be removed.

---

## 9.8 Feature Scaling

Feature scaling adjusts the numeric range of features so they can be meaningfully compared within a regression model. While ordinary least squares (OLS) multiple linear regression can be estimated without scaling, scaling plays an important role when interpreting coefficients and comparing the relative strength of predictors.

In this chapter, feature scaling is introduced as a _descriptive and interpretive tool_, not as a corrective step. Scaling does not fix violated regression assumptions such as non-linearity, heteroscedasticity, or multicollinearity. Those issues are addressed explicitly in the next chapter using regression diagnostics.

Here, scaling helps answer a limited but important question: when predictors are measured in different units, how can coefficient magnitudes be compared in a principled way?

#### Terminology

**Feature scaling** — A transformation that adjusts the numeric range of feature values so they are comparable across predictors. Scaling changes units of measurement but preserves the underlying relationships between variables.

In practice, two related terms are commonly used:

- **Normalization** — A general term for rescaling features to a common numeric range. The most common example is min–max normalization, which rescales values to fall between 0 and 1.
- **Standardization** — A specific form of scaling that transforms values into z-scores. Standardized features have a mean of zero and a standard deviation of one.

Although these terms are sometimes used interchangeably in casual discussion, they are mathematically distinct and serve different interpretive purposes.

It is critical to distinguish between _numeric predictors_ and _dummy-coded categorical predictors_ when scaling features. Only continuous numeric variables should be scaled.

Dummy-coded variables (0/1 indicators) should _not_ be scaled. A value of 0 or 1 already represents a complete and meaningful unit change: membership versus non-membership in a category. Scaling dummy variables would distort their interpretation without providing any analytical benefit.

Scaling a 0/1 variable would replace clear category membership with fractional values that have no real-world meaning. In regression models, dummy coefficients are interpreted relative to a reference group, and preserving the 0/1 structure ensures that interpretation remains valid and intuitive.

Accordingly, when scaling is applied in this chapter, it is applied only to continuous numeric predictors. Dummy-coded categorical variables are left unchanged.

#### Scaling in Python

Python’s scikit-learn library provides several common scaling utilities. In this section, we focus on two widely used approaches: _StandardScaler_ and _MinMaxScaler_. Both preserve the underlying relationships in the data while changing the units on which numeric predictors are measured.

To illustrate the effects of scaling, we return to the insurance dataset. Categorical variables are dummy-coded, and only continuous numeric predictors are scaled. Dummy variables are intentionally excluded from scaling.

```python
from sklearn import preprocessing
import pandas as pd
import seaborn as sns

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

# Dummy-code categorical variables
df = pd.get_dummies(df, columns=df.select_dtypes(['object']).columns, drop_first=True)

# Identify numeric predictors only
numeric_cols = ["age", "bmi", "children"]

sns.jointplot(df, x="age", y="bmi");
```

![Scatterplot of BMI and Age from Original Data](../Images/Chapter9_images/scale_none_age_bmi.png)

_Standardization_ converts numeric predictors into z-scores by subtracting the mean and dividing by the standard deviation. After standardization, coefficients represent the expected change in the label associated with a one–standard deviation increase in a predictor, holding all other variables constant.

```python
df_zscore = df.copy()
df_zscore[numeric_cols] = preprocessing.StandardScaler().fit_transform(df_zscore[numeric_cols])
df_zscore.head()
```

Notice above that we didn't scale the label charges. In causal regression modeling, feature predictors may be scaled for interpretability, but the label should remain in its original units. Scaling the label is reserved for prediction-focused workflows. Otherwise, as you'll notice if you scale the label, you won't get feature coefficients in a meaningful scale.

_Min–max normalization_ rescales numeric predictors to a fixed range between 0 and 1. Coefficients then represent the expected change in the label associated with moving from the minimum to the maximum observed value of a predictor.

```python
df_minmax = df.copy()
df_minmax[numeric_cols] = preprocessing.MinMaxScaler().fit_transform(df_minmax[numeric_cols])
df_minmax.head()
```

![Insurance Data Min-Max Normalized](../Images/Chapter9_images/scale_minmax_all_data.png)

#### Effect on Regression Interpretation

Scaling does not change model fit statistics such as R2, F-statistics, or residual patterns. These quantities depend on the underlying relationships in the data, not on the units of measurement.

What scaling changes is coefficient magnitude and interpretability. After scaling numeric predictors, coefficients can be compared meaningfully across predictors, while their signs and statistical significance remain unchanged.

Notice if we run our MLR again with the MinMax scaled numeric features, we can meaningfully compare them to each other even though they were previously on different scales (because now they're on the same scale).

```python
import statsmodels.api as sm

y = df_minmax.charges
X = df_minmax.drop(columns=['charges']).assign(const=1)

# Convert boolean columns to integers (0s and 1s)
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

model = sm.OLS(y, X).fit()
print(model.summary())

# Output:
#                             OLS Regression Results
# ==============================================================================
# Dep. Variable:                charges   R-squared:                       0.751
# Model:                            OLS   Adj. R-squared:                  0.749
# Method:                 Least Squares   F-statistic:                     500.8
# Date:                Tue, 06 Jan 2026   Prob (F-statistic):               0.00
# Time:                        20:14:42   Log-Likelihood:                -13548.
# No. Observations:                1338   AIC:                         2.711e+04
# Df Residuals:                    1329   BIC:                         2.716e+04
# Df Model:                           8
# Covariance Type:            nonrobust
# ====================================================================================
#                       coef    std err          t      P>|t|      [0.025      0.975]
# ------------------------------------------------------------------------------------
# age               1.182e+04    547.347     21.587      0.000    1.07e+04    1.29e+04
# bmi               1.261e+04   1063.042     11.860      0.000    1.05e+04    1.47e+04
# children          2377.5027    689.020      3.451      0.001    1025.816    3729.189
# sex_male          -131.3144    332.945     -0.394      0.693    -784.470     521.842
# smoker_yes        2.385e+04    413.153     57.723      0.000     2.3e+04    2.47e+04
# region_northwest  -352.9639    476.276     -0.741      0.459   -1287.298     581.370
# region_southeast -1035.0220    478.692     -2.162      0.031   -1974.097     -95.947
# region_southwest  -960.0510    477.933     -2.009      0.045   -1897.636     -22.466
# const            -1901.5967    586.973     -3.240      0.001   -3053.091    -750.103
# ==============================================================================
# Omnibus:                      300.366   Durbin-Watson:                   2.088
# Prob(Omnibus):                  0.000   Jarque-Bera (JB):              718.887
# Skew:                           1.211   Prob(JB):                    7.86e-157
# Kurtosis:                       5.651   Cond. No.                         9.59
# ==============================================================================
#
# Notes:
# [1] Standard Errors assume that the covariance matrix of the errors is correctly specified.
```

After scaling the numeric predictors, the coefficients for _bmi_ and _children_ are now expressed on a comparable scale. This allows us to compare their relative effect sizes more meaningfully. In this scaled model, _bmi_ exhibits a larger coefficient magnitude than _children_, suggesting a stronger association with insurance charges, holding all other variables constant. In the unscaled model, the larger coefficient for _children_ reflected differences in measurement units rather than a stronger underlying relationship.

More generally, scaling alters measurement units but does not change the underlying relationships in the data. For this reason, scaling should be viewed as an interpretive aid rather than a corrective technique. Coefficients examined here remain provisional and should be revisited after diagnostic assumptions are evaluated in the next chapter.

In the next chapter, feature estimates are revisited after evaluating regression assumptions. At that stage, scaled coefficients become more trustworthy inputs for interpretation and feature selection.

---

## 9.9 Summary

In this chapter, you were introduced to multiple linear regression (MLR) as a foundational modeling technique for understanding relationships between features and a continuous outcome. The emphasis throughout was on building, estimating, and interpreting regression models in a controlled, explanatory setting.

#### What MLR Provides

MLR allows us to estimate the independent association between each feature and the label while controlling for the presence of other features in the model. This makes MLR especially valuable for _causal reasoning and decision support_, where understanding direction, magnitude, and relative importance of predictors matters.

Using Python and the Statsmodels OLS implementation, you learned how to define features and labels, fit a regression model, and interpret the resulting output, including coefficients, standard errors, t-statistics, and p-values.

#### Feature Estimates as a Starting Point

Regression coefficients provide an initial estimate of how changes in each feature are associated with changes in the outcome, holding all other features constant. These estimates form the basis for interpretation, comparison, and reasoning about potential drivers of outcomes.

At this stage, however, feature estimates should be treated as _provisional_. While they are mathematically correct given the fitted model, their reliability depends on assumptions that have not yet been fully evaluated or corrected.

#### Preparing Features for Modeling

Because regression models require numeric inputs, categorical variables were converted into dummy-coded features. You also saw why one category must be removed for each categorical feature to avoid redundancy and perfect multicollinearity.

You also explored feature scaling, including standardization and min–max normalization. While MLR does not require scaling to function, scaling improves coefficient comparability and prepares the data for algorithms that are sensitive to feature magnitude.

#### A Deliberate Pause Before Trust

Although you examined model fit statistics and feature estimates, this chapter intentionally stopped short of validating regression assumptions or refining the model. This pause is deliberate.

Before coefficients can be trusted for strong claims or downstream decisions, the model must be evaluated for normality, multicollinearity, autocorrelation, linearity, and homoscedasticity. These diagnostics—and the adjustments they motivate—are addressed in the next chapter.

#### Where This Leads

By the end of this chapter, you should be comfortable building and interpreting an initial regression model, understanding what the output tells you, and recognizing its limitations.

In the next chapter, you will revisit regression models with a more critical lens, applying diagnostics, transformations, and refinements that allow feature estimates to move from _suggestive_ to _defensible_, and ultimately preparing the model for predictive use.

---

## 9.10 Case Studies

This section includes three practice assessments using new datasets so you can rehearse the same multiple linear regression workflow with different feature mixes and real-world context.

For each dataset, your goal is to build an MLR model, interpret fit statistics and coefficients, and answer a set of targeted analysis questions.

This practice uses the **Diamonds** dataset that ships with the Seaborn Python package. Seaborn makes the dataset available through a built-in loader function, so you can download it directly in your notebook without any external files.

**Dataset attribution:** The Diamonds dataset is distributed with the Seaborn data repository and can be loaded with _seaborn.load_dataset("diamonds")_. If you want the underlying CSV source, Seaborn hosts it in its public GitHub repository under _seaborn-data_. To be clear, you can get this dataset using this code:

```python
mport seaborn as sns

# Download/load the dataset from Seaborn
df = sns.load_dataset("diamonds")
```

In this chapter, you are practicing **MLR for causal (explanatory) modeling**. That means you will:

- Load the dataset using _seaborn.load_dataset("diamonds")_.
- Inspect the dataset: rows/columns, data types, and summary statistics.
- Fit an MLR model predicting _price_ using numeric predictors (_carat_, _depth_, _table_, _x_, _y_, _z_) and categorical predictors (_cut_, _color_, _clarity_).
- Record model fit metrics (_R²_ and _Adjusted R²_).
- Identify the most statistically significant predictors using _t_ and _P>|t|_.
- (Optional) Compute standardized coefficients for numeric predictors and compare magnitudes.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Diamonds dataset?
1. What is the mean value of _price_ in the dataset?
1. What are the _R²_ and _Adjusted R²_ values for your fitted MLR model? Report both values to 4 decimal places.
1. Which single predictor term (feature or dummy-coded category) has the smallest _P>|t|_ value in your model output?
1. Among the numeric predictors (_carat_, _depth_, _table_, _x_, _y_, _z_), which one has the largest absolute _t_-value? Provide the feature name and its t-value (rounded to 2 decimals).
1. Pick one categorical variable (_cut_, _color_, or _clarity_). Which category level appears to increase predicted _price_ the most relative to the reference group (based on coefficient sign and magnitude)? Provide the dummy term name and coefficient value.
1. (Optional) After standardizing numeric variables, which numeric predictor has the largest standardized coefficient magnitude? Provide the feature name and the standardized coefficient value.

### Diamonds Practice Answers

These answers were computed by fitting an OLS multiple linear regression model predicting _price_ using numeric predictors (_carat_, _depth_, _table_, _x_, _y_, _z_) plus dummy-coded categorical predictors for _cut_, _color_, and _clarity_ (with the first category as the reference group for each categorical variable).

1. The Diamonds dataset contains _53940_ rows and _10_ columns.
1. The mean value of _price_ is _3932.7997_.
1. For the fitted MLR model, _R² = 0.9198_ and _Adjusted R² = 0.9197_ (both reported to 4 decimals).
1. The single predictor term with the smallest _P>|t|_ value is _carat_ (its p-value is effectively 0.0000 in floating-point terms and is shown as 0.000 in typical summary output due to rounding).
1. Among numeric predictors, _carat_ has the largest absolute _t_-value: _t = 231.49_ (rounded to 2 decimals).
1. Using _cut_ as the categorical variable, the level that increases predicted _price_ the most relative to the reference group is _cut_Ideal_ with coefficient _832.91_.
1. (Optional) After standardizing numeric variables, the numeric predictor with the largest standardized coefficient magnitude is _carat_ with standardized coefficient _5335.88_.

This practice uses the **Red Wine Quality** dataset (physicochemical measurements plus a quality rating). Unlike the Diamonds dataset, this dataset contains only numeric predictors, so you will practice building and interpreting an MLR model without dummy coding.

**Dataset attribution:** This dataset is commonly distributed as the _winequality-red.csv_ file from the UCI Machine Learning Repository (Wine Quality Data Set), originally published by Cortez et al. in “Modeling wine preferences by data mining from physicochemical properties” (Decision Support Systems, 2009). In this course, you may be given the CSV directly (as _winequality-red.csv_), or you can download it from UCI and load it into your notebook.

To load the dataset, use one of the following approaches (choose one):

- **Option A (recommended if the CSV is provided):** Upload _winequality-red.csv_ to your notebook environment, then load it with _pd.read_csv()_.
- **Option B (download from UCI):** Download the CSV from the UCI repository, then load it with _pd.read_csv()_.

In this chapter, you are practicing **MLR for causal (explanatory) modeling**. That means you will:

- Load the dataset and inspect it: rows/columns, data types, and summary statistics.
- Fit an OLS MLR model predicting _quality_ using all other columns as numeric predictors.
- Record model fit metrics (_R²_ and _Adjusted R²_).
- Identify the most statistically significant predictors using _t_ and _P>|t|_.
- (Optional) Compute standardized coefficients for numeric predictors and compare magnitudes.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Red Wine Quality dataset?
1. What is the mean value of _quality_ in the dataset?
1. What are the _R²_ and _Adjusted R²_ values for your fitted MLR model? Report both values to 4 decimal places.
1. Which single predictor term has the smallest _P>|t|_ value in your model output?
1. Which predictor has the largest absolute _t_-value? Provide the predictor name and its t-value (rounded to 2 decimals).
1. Which predictor has the largest positive coefficient in the fitted model? Provide the predictor name and coefficient value (rounded to 4 decimals).
1. (Optional) After standardizing numeric predictors (z-scores) and refitting the model, which predictor has the largest standardized coefficient magnitude? Provide the predictor name and coefficient value (rounded to 4 decimals).

### Red Wine Quality Practice Answers

These answers were computed by fitting an OLS multiple linear regression model predicting _quality_ using all remaining columns in _winequality-red.csv_ as numeric predictors, with an intercept term (_const_).

1. The Red Wine Quality dataset contains _1599_ rows and _12_ columns.
1. The mean value of _quality_ is _5.6360_.
1. For the fitted MLR model, _R² = 0.3606_ and _Adjusted R² = 0.3561_ (both reported to 4 decimals).
1. The single predictor term with the smallest _P>|t|_ value is _alcohol_ (its p-value is effectively 0.0000 in floating-point terms and is shown as 0.000 in typical summary output due to rounding).
1. The predictor with the largest absolute _t_-value is _alcohol_ with _t = 10.43_ (rounded to 2 decimals).
1. The predictor with the largest positive coefficient is _sulphates_ with coefficient _0.9163_ (rounded to 4 decimals).
1. (Optional) After standardizing numeric predictors (z-scores) and refitting the model (without scaling the label), the predictor with the largest standardized coefficient magnitude is _alcohol_ with standardized coefficient _0.2942_ (rounded to 4 decimals).

This practice uses the **Bike Sharing** daily dataset (the _day.csv_ file). You will fit an OLS multiple linear regression model to explain variation in total daily rentals (_cnt_) using both numeric predictors (weather conditions) and categorical predictors (season/month/weekday/weather situation).

**Dataset attribution:** This dataset is distributed as part of the Bike Sharing Dataset hosted by the UCI Machine Learning Repository (Fanaee-T and Gama). It includes daily rental counts and weather/context variables derived from the Capital Bikeshare system in Washington, D.C. You will use the _day.csv_ file provided with your course materials.

In this chapter, you are practicing **MLR for causal (explanatory) modeling**. That means you will fit the model on the full dataset (no train/test split) and interpret model fit and feature evidence using _R²_, _Adjusted R²_, _t_-values, and _P>|t|_.

**Important modeling note:** Do not include _casual_ or _registered_ as predictors because they directly sum to _cnt_ and would leak the answer into the model.

**Tasks**

- Inspect the dataset: rows/columns, data types, and summary statistics for _cnt_.
- Fit an OLS MLR model predicting _cnt_ using numeric predictors (_temp_, _atemp_, _hum_, _windspeed_) and categorical predictors (_season_, _mnth_, _weekday_, _weathersit_), plus binary indicators (_yr_, _holiday_, _workingday_).
- Dummy-code the categorical predictors using _drop_first=True_, then fit the model with Statsmodels _OLS_.
- Record model fit metrics (_R²_ and _Adjusted R²_).
- Identify the most statistically significant predictor term using _P>|t|_.
- Compute standardized coefficients for the numeric predictors (_yr_, _holiday_, _workingday_, _temp_, _atemp_, _hum_, _windspeed_) while leaving dummy codes unscaled, then compare effect sizes.

If you want a code scaffold that matches the chapter style (no formula interface), start here:

```python
import pandas as pd
import statsmodels.api as sm

df = pd.read_csv("day.csv")

# Label
y = df["cnt"]

# Predictors (exclude leakage variables such as casual and registered)
X = df[["season","yr","mnth","holiday","weekday","workingday","weathersit","temp","atemp","hum","windspeed"]]

# Dummy-code categorical predictors (reference group is the first category for each)
X = pd.get_dummies(X, columns=["season","mnth","weekday","weathersit"], drop_first=True)

# Statsmodels expects numeric (0/1) dummies, not True/False
bool_cols = X.select_dtypes("bool").columns
X[bool_cols] = X[bool_cols].astype(int)

# Add intercept
X = X.assign(const=1)

model = sm.OLS(y, X).fit()
print(model.summary())
```

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Bike Sharing daily dataset (_day.csv_)?
1. What is the mean value of _cnt_ in the dataset?
1. What are the _R²_ and _Adjusted R²_ values for your fitted MLR model? Report both values to 4 decimal places.
1. Which single predictor term (feature or dummy-coded category) has the smallest _P>|t|_ value in your model output?
1. Among the numeric predictors (_yr_, _holiday_, _workingday_, _temp_, _atemp_, _hum_, _windspeed_), which one has the largest absolute _t_-value? Provide the feature name and its t-value (rounded to 2 decimals).
1. For the categorical variable _weathersit_, which category level decreases predicted _cnt_ the most relative to the reference group? Provide the dummy term name and coefficient value.
1. (Standardized coefficients) After standardizing the numeric predictors (but not the dummy codes), which numeric predictor has the largest standardized coefficient magnitude? Provide the feature name and the standardized coefficient value.
1. (Standardized coefficients) Which numeric predictor has the smallest standardized coefficient magnitude (i.e., the weakest effect size on the standardized scale)? Provide the feature name and the standardized coefficient value.
1. Based on your standardized coefficients, name one “best” feature for making informal predictions about daily rentals (large magnitude) and one “worst” feature (small magnitude). Briefly justify your choices in one sentence.

### Bike Sharing Practice Answers

These answers were computed by fitting an OLS multiple linear regression model predicting _cnt_ using numeric predictors (_yr_, _holiday_, _workingday_, _temp_, _atemp_, _hum_, _windspeed_) plus dummy-coded categorical predictors for _season_, _mnth_, _weekday_, and _weathersit_ (with the first category as the reference group for each categorical variable), and excluding leakage variables (_casual_ and _registered_).

1. The Bike Sharing daily dataset contains _731_ rows and _16_ columns.
1. The mean value of _cnt_ is _4504.3488_.
1. For the fitted MLR model, _R² = 0.8381_ and _Adjusted R² = 0.8312_ (both reported to 4 decimals).
1. The single predictor term with the smallest _P>|t|_ value is _yr_ (its p-value is effectively 0.0000 in floating-point terms and is shown as 0.000 in typical summary output due to rounding).
1. Among numeric predictors, _yr_ has the largest absolute _t_-value: _t = 34.69_ (rounded to 2 decimals).
1. For _weathersit_, the level that decreases predicted _cnt_ the most relative to the reference group is _weathersit_3_ with coefficient _-2409.68_.
1. (Standardized) The numeric predictor with the largest standardized coefficient magnitude is _yr_ with standardized coefficient _1738.95_.
1. (Standardized) The numeric predictor with the smallest standardized coefficient magnitude is _workingday_ with standardized coefficient _17.09_.
1. A reasonable “best” feature is _yr_ (very large standardized coefficient magnitude), while a reasonable “worst” feature is _workingday_ (very small standardized coefficient magnitude), meaning it contributes little to informal predictions once the other variables are controlled.

#### More Practice

There are two additional "assignment style" practice assessments below. The first is a repeat of the same dataset you've been using throughout the chapter and will serve as a reminder of everything you have covered. The second uses a new dataset to continue practicing the same concepts.

You may want to test yourself to see how well you understand the concepts. The quiz below walks through the same dataset you just used in these examples above.

### 9.10 Practice: Regression: Insurance Charges

- Label

_charges_: the total cost of medical insurance charges for each customer for the prior year

- Features

_age_: the age of the customer in years
_sex_: the sex/gender of the customer
_bmi_: the customer's measured body mass index (BMI) score, which is based on their weight-to-height ratio
_children_: the number of children this customer has (NOTE: their children's charges are not included in the *charges*label)
_smoker_: whether or not the customer is a smoker
_region_: the region of North America where the customer lives

- Perform the steps included in each of the questions below and answer the associated questions.

- Submit your .ipynb file with all data cleaning and regression models where specified below.

### 9.10 Practice: Regression: Disney Movies

- Labels

_total_gross_: the actual gross revenue of the movie
_inflation_adjusted_gross_: the gross revenue converted to account for inflation

- Features

_movie_title_: the title of the film
_release_date_: the first date it appeared in theaters
_genre_: the type of file
_mpaa_rating_: G, PG, PG-13, R, Not Rated, or null/empty

- Perform the steps included in each of the questions below and answer the associated questions.

- Submit your .ipynb file with all data cleaning and regression models where specified below.

---

## 9.11 Assignment

Complete the assignment(s) below (if any):

### 9.11 MLR Concepts

- Load and explore a real-world dataset
- Clean and prepare data for modeling
- Build a multiple linear regression model using Python
- Interpret model results (coefficients, R², statistical significance)

- **SalePrice** – Sale price (target variable)
- **MSSubClass** – Building class
- **MSZoning** – Zoning classification
- **LotFrontage** – Street frontage (ft)
- **LotArea** – Lot size (sq ft)
- **Street** – Road type
- **Alley** – Alley access
- **LotShape** – Property shape
- **LandContour** – Flatness
- **Utilities** – Utilities available
- **LotConfig** – Lot configuration
- **LandSlope** – Slope
- **Neighborhood** – Location in Ames
- **Condition1 / Condition2** – Proximity to roads/railroad
- **BldgType** – Dwelling type
- **HouseStyle** – House style
- **OverallQual** – Overall quality (1–10)
- **OverallCond** – Overall condition (1–10)
- **YearBuilt** – Construction year
- **YearRemodAdd** – Remodel year
- **RoofStyle** – Roof type
- **RoofMatl** – Roof material
- **Exterior1st / Exterior2nd** – Exterior covering
- **MasVnrType** – Masonry veneer type
- **MasVnrArea** – Masonry area (sq ft)
- **ExterQual** – Exterior quality
- **ExterCond** – Exterior condition
- **Foundation** – Foundation type
- **BsmtQual** – Basement quality
- **BsmtCond** – Basement condition
- **BsmtExposure** – Basement exposure
- **BsmtFinType1 / BsmtFinType2** – Basement finish type
- **BsmtFinSF1 / BsmtFinSF2** – Finished basement area
- **BsmtUnfSF** – Unfinished basement area
- **TotalBsmtSF** – Total basement area
- **Heating** – Heating type
- **HeatingQC** – Heating quality
- **CentralAir** – Central air (Y/N)
- **Electrical** – Electrical system
- **1stFlrSF** – First floor area
- **2ndFlrSF** – Second floor area
- **LowQualFinSF** – Low-quality finished area
- **TotalSF** – Total finished area
- **BsmtFullBath / BsmtHalfBath** – Basement bathrooms
- **FullBath / HalfBath** – Above-grade bathrooms
- **BedroomAbvGr** – Bedrooms above grade
- **KitchenAbvGr** – Kitchens above grade
- **KitchenQual** – Kitchen quality
- **TotRmsAbvGrd** – Total rooms above grade
- **Functional** – Home functionality
- **Fireplaces** – Number of fireplaces
- **FireplaceQu** – Fireplace quality
- **GarageType** – Garage location
- **GarageYrBlt** – Garage build year
- **GarageFinish** – Garage finish
- **GarageCars** – Garage capacity
- **GarageArea** – Garage size
- **GarageQual / GarageCond** – Garage quality & condition
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
- **MiscVal** – Value of misc. feature
- **MoSold** – Month sold
- **YrSold** – Year sold
- **SaleType** – Sale type
- **SaleCondition** – Sale condition

- Download the dataset and notebook (.ipynb) file below.
- Use the Jupyter Notebook file below to complete this assignment
- Maintain the "# Question [n]:" comments in each code cell for the autograder
- Upload that file with all code cells executed and output showing where instructed below
- Respond to each of the short answser and multiple choice questions below

---
