# Chapter 10: MLR Diagnostics for Causal Inference

## Learning Objectives

- Students will be able to evaluate the five core regression assumptions (normality, multicollinearity, autocorrelation, linearity, homoscedasticity) using appropriate diagnostic tests and plots
- Students will be able to detect and address multicollinearity using correlation heatmaps and Variance Inflation Factor (VIF) analysis
- Students will be able to apply label transformations (log, Box-Cox, Yeo-Johnson) to correct residual normality violations
- Students will be able to diagnose linearity violations through residual-versus-fitted plots and address them using polynomial features or transformations
- Students will be able to explain why regression diagnostics are essential for valid causal inference but less critical for purely predictive modeling

---

## 10.1 Introduction

![Conceptual visualization of multiple linear regression showing several input features contributing to a fitted regression surface, alongside visual cues for residuals and model diagnostics, emphasizing the role of assumptions and diagnostics in evaluating regression models.](../Images/Chapter10_images/regression_diagnostics_header.png)

In the previous chapter, you learned how to build multiple linear regression (MLR) models for _causal (explanatory) analysis_. You focused on estimating coefficients, interpreting feature effects, and understanding model fit using statistics such as _R²_, _Adjusted R²_, _t_-values, and _p_-values. Those tools allowed you to ask questions like: _Which features matter?_ and _How strong is their relationship with the outcome?_

This chapter shifts the focus from _estimating_ a regression model to _evaluating and refining_ one for the purpose of reliable explanation. The central question is no longer just “What does the model say?” but “_When should we trust what the model says as evidence about relationships in the world?_” Answering that question requires understanding the assumptions that underlie linear regression and learning how to diagnose when those assumptions are violated.

Regression assumptions exist to support valid _statistical inference_. They ensure that coefficient estimates are stable, standard errors are meaningful, and hypothesis tests behave as expected. When these assumptions hold reasonably well, regression results can be interpreted as credible evidence about relationships among variables. When they do not, coefficient estimates may still exist, but their interpretation becomes unreliable.

In contrast, many modern analytics projects emphasize _prediction_ rather than explanation. In predictive settings, the goal is not to understand why an outcome occurs, but to forecast future values as accurately as possible. In those contexts, some regression assumptions can be relaxed or ignored entirely, as long as predictive performance is validated on new data.

This chapter adopts a clear organizing principle: **regression diagnostics matter most when the goal is causal or explanatory modeling**. You will learn which assumptions are essential for trustworthy inference, which violations are especially dangerous for interpretation, and why some fixes that improve prediction may actually undermine causal clarity.

A central theme of this chapter is _diagnostic-driven feature design_. Instead of adding, removing, or transforming variables to maximize fit alone, you will use diagnostics—such as residual plots, normality tests, and variance inflation factors (VIF)—to make principled adjustments that improve interpretability, stability, and inferential validity.

Throughout the chapter, you will still see references to metrics like _R²_ and _Adjusted R²_, but always in service of explanation rather than prediction. In particular, you will learn why a lower _R²_ can sometimes reflect a better causal model when it aligns more closely with the assumptions required for inference.

By the end of this chapter, you will be able to diagnose regression problems, refine models systematically, and judge when regression results can be responsibly interpreted as explanatory evidence. In the next chapter, you will revisit many of these same tools from a different perspective—optimizing models for prediction, where the trade-offs and priorities change.

---

## 10.2 Regression Diagnostics

#### Why Assumptions Exist

In the previous chapter, you learned how to build and interpret multiple linear regression (MLR) models for _explanatory (causal)_ purposes. This chapter focuses on the next critical step: _regression diagnostics_. Diagnostics help you evaluate whether the mathematical conditions that justify coefficient interpretation and statistical inference are reasonably satisfied.

Regression assumptions exist for both _mathematical_ and _practical_ reasons. Mathematically, they ensure that the estimators produced by ordinary least squares (OLS) have desirable properties such as unbiasedness, efficiency, and valid standard errors. Practically, they determine whether quantities like coefficients, p-values, and confidence intervals can be interpreted as reliable evidence about relationships in the data.

When assumptions fail, the regression model does not suddenly become useless. Instead, _specific components of the output become unreliable_. Diagnostics help you identify _what is breaking_, _why it is breaking_, and _which modeling adjustments are appropriate_ when the goal is explanation rather than prediction.

The table below summarizes the core regression assumptions examined in this chapter, what breaks when each assumption is violated, why that failure matters for causal interpretation, and a concrete example to anchor each concept.

Regression diagnostics should not be treated as pass–fail tests. Real-world data rarely satisfies all assumptions perfectly, and attempting to enforce strict compliance often leads to unnecessary complexity or distorted interpretation.

Instead, diagnostics should be understood as _signals_ that guide judgment-driven modeling decisions:

- Diagnostics indicate _where_ causal interpretation may be fragile.
- Diagnostics suggest _which features_ may require transformation, re-specification, or removal.
- Diagnostics help distinguish _inference problems_ from issues that primarily affect prediction.
- Diagnostics support _analytical judgment_, not automatic correction.

Throughout this chapter, you will learn how to evaluate each assumption, interpret its diagnostic signals, and decide when corrective action is necessary for explanatory modeling. You will also see that _prediction-oriented models can often tolerate assumption violations that causal (inference-oriented) models cannot_, a distinction that becomes the central focus of the next chapter.

---

## 10.3 Normality

Normality is one of the most commonly discussed—and most commonly misunderstood—assumptions in regression modeling. It is often incorrectly described as a requirement that the data itself be normally distributed.

In reality, normality plays a much narrower role. It primarily affects _statistical inference_, not the model’s ability to generate point predictions. When normality assumptions are reasonably satisfied, we can trust p-values, confidence intervals, and hypothesis tests on coefficients. When they are violated, predictions may still be useful, but inference becomes less reliable—making this assumption especially important for _causal (explanatory)_ modeling.

In this section, we treat normality diagnostics as _signals rather than pass/fail tests_. Our goal is not to achieve perfect normality, but to understand what deviations tell us about model behavior and how to respond in ways that protect interpretability and inference.

#### Univariate Normality (Label Distribution)

The _OLS().summary()_ output reports skewness and kurtosis for the dependent variable. In this dataset, the label _charges_ exhibits substantial right skew and elevated kurtosis, indicating a heavy-tailed distribution.

Strictly speaking, regression does not require the label itself to be normally distributed. However, extreme skewness in the label often carries downstream consequences that matter for explanatory modeling: it can contribute to non-normal residuals, unstable variance, and coefficient tests that are less trustworthy.

Because of this indirect effect, it is often useful to inspect the label distribution visually using histograms and pair plots rather than relying only on numeric skewness statistics.

```python
import pandas as pd
import seaborn as sns

df = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/insurance.csv')

sns.pairplot(df[['age', 'bmi', 'children', 'charges']]);
```

![Matrix of histograms and scatterplots. Age, BMI, children, and charges.](../Images/Chapter10_images/pairplot_insurance.png)

The diagonal of the pair plot shows the marginal distributions of each numeric variable. Binary features and discrete count variables are not expected to follow a normal distribution and are not a concern. In this dataset, _charges_ is clearly right-skewed, while features such as _bmi_ appear approximately normal.

When a label is strongly skewed, mathematical transformations can improve symmetry and stabilize variance. The goal is not to make the data “perfectly normal,” but to reduce extreme skew that can distort residual behavior and, in turn, weaken inference about coefficients.

A simple starting point is to try a few intuitive transformations such as square root, cube root, and natural log. These are easy to compute and interpret, and they often reduce right skew substantially for strictly positive outcomes like insurance charges.

```python
import numpy as np

df['charges_sqrt'] = df['charges']**(1/2)
df['charges_cbrt'] = df['charges']**(1/3)
df['charges_ln'] = np.log(df['charges'])

df[['charges', 'charges_sqrt', 'charges_cbrt', 'charges_ln']].skew()

# Output (example)
# charges             1.515880
# charges_sqrt        0.795863
# charges_cbrt        0.515183
# charges_ln         -0.090098
# dtype: float64
```

Among these simple options, the natural log of _charges_ often produces the largest reduction in skewness, bringing it closest to zero. For causal (explanatory) analysis, this can be valuable because it often leads to residuals that are closer to symmetric and variance that is more stable across cases.

However, not all real-world labels respond well to a single named transformation like log or square root. Two widely used alternatives are _Box-Cox_ and _Yeo-Johnson_, which are more flexible because they automatically choose a transformation strength (called _lambda_) based on the data.

- **Box-Cox**: Searches over a family of power transforms and selects the lambda that best reduces skew and stabilizes variance. It requires strictly positive values (no zeros or negatives).
- **Yeo-Johnson**: Similar idea, but it can handle zeros and negative values. This makes it a safer default when you are unsure whether the label contains non-positive values.

In practice, you can think of these as automated, data-driven extensions of the transformations you already know: when lambda is near 0, the transform behaves like a log; when lambda is near 0.5, it behaves like a square root; other lambda values create intermediate or stronger adjustments.

The code below demonstrates both transformations and compares their skewness reduction. Even when the label is already positive (as it is here), it is still useful to show both, because Yeo-Johnson generalizes cleanly to other datasets you may encounter later.

```python
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import PowerTransformer

# --- Prepare label ---
df['charges_ln'] = np.log(df['charges'])

y = df[['charges']]

# Box-Cox (positive values only)
pt_bc = PowerTransformer(method='box-cox', standardize=False)
df['charges_boxcox'] = pt_bc.fit_transform(y)

# Yeo-Johnson (handles zero/negative values)
pt_yj = PowerTransformer(method='yeo-johnson', standardize=False)
df['charges_yj'] = pt_yj.fit_transform(y)

# --- Print skewness comparison ---
skewness = pd.Series({
  'Original': df['charges'].skew(),
  'Log': df['charges_ln'].skew(),
  'Box-Cox': df['charges_boxcox'].skew(),
  'Yeo-Johnson': df['charges_yj'].skew()
})

print(skewness.round(4))

# --- Plot histograms side by side ---
fig, axes = plt.subplots(1, 4, figsize=(18, 4), sharey=True)

plots = [
  ('Original', 'charges'),
  ('Log', 'charges_ln'),
  ('Box-Cox', 'charges_boxcox'),
  ('Yeo-Johnson', 'charges_yj')
]

for ax, (title, col) in zip(axes, plots):
  sns.histplot(df[col], kde=True, ax=ax)
  ax.set_title(title, fontsize=14)
  ax.set_xlabel('')
  ax.set_ylabel('')

plt.tight_layout()
plt.show()
```

At this stage, your goal is to select a transformation that reduces extreme skew and produces a more stable residual pattern after modeling. Although each of the transformations appears to be sufficient for the charges label in this dataset, you will encounter scenarios where Box-Cox and/or Yeo-Johnson provides superior results to a basic ln() transformation. In the next parts of this chapter, you will see how these univariate improvements connect to the more important target for causal modeling: the normality and structure of the residuals.

Overall model normality refers to the distribution of the _residuals_—the differences between observed and predicted values. Regression assumes that these residuals are approximately normally distributed around zero.

This assumption does not affect point predictions directly. Instead, it affects statistical inference: p-values, confidence intervals, and hypothesis tests on coefficients.

The Omnibus test and its associated p-value (_Prob(Omnibus)_) evaluate whether the residuals deviate significantly from normality based on skewness and kurtosis. Unlike coefficient p-values, a _high_ Omnibus p-value indicates that residual normality is not strongly violated. This test appears automatically in the _OLS().summary()_ output produced by statsmodels.

Visual inspection often provides more insight than a single test statistic. Residual histograms, Q–Q plots, and residuals-versus-fitted plots help reveal whether errors are centered around zero and whether departures from normality are systematic rather than random.

```python
# Insurance residual-normality demo (short version)
# Compares OLS on charges vs OLS on log(charges) and plots 3 diagnostics for each.

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import statsmodels.api as sm
from statsmodels.stats.stattools import omni_normtest
from scipy.stats import probplot, gaussian_kde

# 1) Load + dummy-code
df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

X = sm.add_constant(df.drop(columns=["charges"]), has_constant="add")
y = df["charges"].astype(float)

# 2) Fit two models
m1 = sm.OLS(y, X).fit()
m2 = sm.OLS(np.log(y), X).fit()

# 3) Show Omnibus values (also appear in .summary())
o1, p1 = omni_normtest(m1.resid)
o2, p2 = omni_normtest(m2.resid)
print(f"Model 1 (charges):      Omnibus={o1:.3f}, Prob(Omnibus)={p1:.6f}")
print(f"Model 2 (log(charges)): Omnibus={o2:.3f}, Prob(Omnibus)={p2:.6f}")

# 4) Plot helper: histogram+KDE, Q–Q, residuals vs fitted
def kde_line(r):
  xs = np.linspace(r.min(), r.max(), 300)
  return xs, gaussian_kde(r)(xs)

def row(axs, model, title, p_omni):
  r = np.asarray(model.resid, float)
  f = np.asarray(model.fittedvalues, float)

  # A) Residual histogram + KDE
  axs[0].hist(r, bins=35, density=True, alpha=0.75)
  xs, ys = kde_line(r)
  axs[0].plot(xs, ys, linewidth=2)
  axs[0].set_title(f"{title}\nResiduals (Prob(Omnibus)={p_omni:.4g})")
  axs[0].set_xlabel("Residual")
  axs[0].set_ylabel("Density")

  # B) Q–Q plot
  probplot(r, dist="norm", plot=axs[1])
  axs[1].get_lines()[0].set_markersize(3)
  axs[1].get_lines()[1].set_linewidth(2)
  axs[1].set_title("Q–Q plot")
  axs[1].set_xlabel("Theoretical quantiles")
  axs[1].set_ylabel("Ordered residuals")

  # C) Residuals vs fitted
  axs[2].scatter(f, r, s=10, alpha=0.5)
  axs[2].axhline(0, linewidth=1)
  axs[2].set_title("Residuals vs fitted")
  axs[2].set_xlabel("Fitted values")
  axs[2].set_ylabel("Residuals")

# 5) Composite figure (2 rows × 3 columns)
fig, ax = plt.subplots(2, 3, figsize=(16, 8), constrained_layout=True)
row(ax[0], m1, "Model 1: charges", p1)
row(ax[1], m2, "Model 2: log(charges)", p2)
plt.suptitle("Residual Normality: Before vs After log(label)", fontsize=18)
plt.show()

# Output:
# Model 1 (charges):      Omnibus=300.366, Prob(Omnibus)=0.000000
# Model 2 (log(charges)): Omnibus=463.882, Prob(Omnibus)=0.000000
```

![Q-Q plot, residual plot, and residuals by fitted values scatterplot for both the untransformed model and the model with label skewness adjusted by ln()](../Images/Chapter10_images/residual_plots.png)

In the example above, we compared an untransformed model with a model that applies a logarithmic transformation to the label. The residual histogram and Q–Q plot typically become more symmetric after the log transform, which can make inference more stable. However, the Omnibus p-value may still be very small, reminding us that residual normality is influenced by more than label skew alone.

For causal (explanatory) modeling, the key question is not whether the residuals look perfectly normal, but whether departures from normality are severe enough to undermine coefficient tests and confidence intervals. In practice, strong non-normality often indicates that other assumptions are violated at the same time—especially linearity or homoscedasticity—so fixing normality in isolation is rarely the best strategy.

Rather than treating normality as a pass/fail condition, it is more useful to view it as a diagnostic signal that points toward underlying modeling issues such as non-linearity, missing structure, subgroup effects, or unstable variance. Different distributional patterns suggest different causes—and therefore different responses.

To summarize, there are many reasons that non-normality exists and many fixes. We will cover the rest of them in the remainder of this chapter, but it may be helpful to see a summary of these issues here:

This table reinforces a central principle of regression diagnostics: departures from normality are _signals_, not automatic failures. The appropriate response depends on the observed pattern, the modeling objective, and whether the analysis prioritizes explanation or prediction.

Normality diagnostics matter most for _explanatory (causal) analysis_, where valid p-values and confidence intervals are essential. For prediction-focused modeling, modest departures from normality are common and often acceptable as long as predictive performance is evaluated on unseen data and uncertainty is communicated appropriately.

---

## 10.4 Multicollinearity

**Multicollinearity** — The presence of strong correlations among independent variables. Multicollinearity occurs when two or more features convey overlapping information. In multiple linear regression, this overlap makes it difficult to isolate the unique effect of each feature, which is exactly the kind of interpretation challenge that matters most in _causal (explanatory)_ modeling.

One of the most intuitive ways to detect potential multicollinearity is to examine a correlation heatmap of the predictor variables. Each cell in the heatmap represents the pairwise correlation between two features, with darker colors indicating stronger positive or negative relationships.

In the heatmap below, several groups of features exhibit extremely high correlations with one another. For example, metrics related to product usage—such as _usage_intensity_, _active_users_, _sessions_per_user_, and _api_calls_—move together almost perfectly. This indicates that these features are capturing very similar underlying behavior.

These values come from a SaaS customer churn dataset designed to understand why customers stop using a subscription-based software product. In this business context, usage-related features such as _active_users_, _sessions_per_user_, _api_calls_, and _usage_intensity_ all measure closely related aspects of customer engagement, which explains why they exhibit extremely high correlations.

![Heatmap of a correlation matrix showing extremely high correlations among usage-related features such as usage_intensity, active_users, sessions_per_user, and api_calls in a SaaS customer churn dataset.](../Images/Chapter10_images/correlation_heatmap.png)

Strong pairwise correlations are an early warning sign, but _correlation alone does not imply multicollinearity_; true multicollinearity exists only when a feature can be explained by a combination of all other features in the model.

High correlations like these signal potential multicollinearity concerns for explanatory modeling. If all of these features were included in the same regression, the model would struggle to isolate the unique effect of each variable because the features rarely vary independently. However, correlation is a bivariate measure that does not account for the combined influence of all other predictors, which is why a more formal diagnostic is required.

Multicollinearity is primarily a problem for _interpretation_, not prediction. When features are highly correlated, the model can still generate accurate in-sample predictions, but the coefficient estimates become unstable: small changes in the data can cause large changes in coefficient values and even coefficient signs.

This instability shows up as inflated standard errors, weaker or inconsistent _t_-statistics, and misleading p-values. In other words, multicollinearity makes it difficult to answer the explanatory question, “What is the unique effect of this feature, holding the others constant?” because the features do not truly vary independently.

Statsmodels reports the _Condition Number (Cond. No.)_ as a rough warning signal for numerical dependence in the design matrix. Large values can occur when predictors have very different scales, when predictors are nearly linearly dependent, or when both issues occur at the same time. However, the condition number does not identify which specific features are responsible for the overlap, and it should not be treated as a substitute for feature-level diagnostics such as VIF.

To pinpoint feature-level multicollinearity, we use _Variance Inflation Factor (VIF)_, which quantifies how much a coefficient’s variance is inflated because the feature can be predicted using the other features in the model.

Conceptually, VIF asks a simple question: “If I tried to predict this feature using all the other features, how well could I do?” A high VIF indicates that the feature contributes little independent information and is therefore problematic for explanatory interpretation.

If a feature can be predicted very well by the others (high R2 in that auxiliary regression), then it contributes little independent information, and its VIF will be large.

![Concept diagram showing overlapping information among predictor variables, illustrating how redundancy inflates coefficient uncertainty in multiple linear regression.](../Images/Chapter10_images/vif_concept.png)

To understand how VIF works, we can calculate it manually by fitting a separate regression for each feature against all remaining features and extracting the R2. Then we convert that R2 into VIF using _VIF = 1 / (1 − R2)_.

This manual loop is instructional. It reinforces that VIF is not a mysterious statistic; it is derived directly from a familiar regression idea: “How well can one feature be explained by the others?”

```python
import pandas as pd
import statsmodels.api as sm

# Use original dataframe (already dummy-coded elsewhere if needed)
X = df.drop(columns=["charges"]).copy()

# Convert boolean columns to integers
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

# Add constant
X = sm.add_constant(X, has_constant="add")

# DataFrame to store results
df_vif = pd.DataFrame(columns=["VIF"])

# Loop through each feature (excluding the constant)
for col in X.columns:
  if col == "const":
    continue

  y_aux = X[col]
  X_aux = X.drop(columns=[col])

  r_squared = sm.OLS(y_aux, X_aux).fit().rsquared
  df_vif.loc[col] = [1 / (1 - r_squared)]

df_vif.sort_values(by="VIF", ascending=False)
```

![Example table showing VIF values for several predictors.](../Images/Chapter10_images/multicollinearity_table.png)

VIF values are commonly interpreted using approximate thresholds:

- VIF < 3: Little to no multicollinearity (ideal for explanatory modeling).
- VIF between 3 and 5: Moderate multicollinearity (often acceptable with careful interpretation).
- VIF > 10: High multicollinearity (problematic for coefficient interpretation).

Based on this, our results above indicate that none of the features have multicollinearity problems. This is expected since none of the features are theoretically similar.

Also, these thresholds above are guidelines, not rules. Whether a VIF value is “too high” depends on your goal. If the goal is causal or explanatory interpretation, high VIF can seriously undermine coefficient-level conclusions. If the goal is prediction, high VIF is often tolerable as long as predictive performance is validated appropriately.

A common student misunderstanding is to assume that “high multicollinearity means the model is wrong.” A more accurate mental model is: multicollinearity mostly means “the model cannot confidently assign credit to one feature versus another,” even though the group of correlated features may still predict the label well.

In practice, we rarely compute VIF manually. A standard workflow is to compute VIF using a built-in function and then investigate the specific features with the highest VIF values.

The following code produces the same VIF information more efficiently. Notice that the logic is identical: the function computes an auxiliary regression for each column and then applies the same VIF formula.

```python
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

# Use original dataframe
X = df.drop(columns=["charges"]).copy()

# Convert boolean columns to integers
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

# Add constant
X = sm.add_constant(X, has_constant="add")

vif_df = pd.DataFrame({
  "feature": X.columns,
  "VIF": [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
})

# Optional: drop constant from reporting
vif_df = vif_df[vif_df["feature"] != "const"]

vif_df.sort_values(by="VIF", ascending=False)
```

![Example table showing VIF values for several predictors.](../Images/Chapter10_images/multicollinearity_table2.png)

If you prefer, you can compute VIF from a scikit-learn style matrix as long as you pass a purely numeric feature matrix (dummy codes included) into the VIF function. The key requirement is that the matrix contains predictors only (no label column).

Once multicollinearity is identified, the typical next steps include removing redundant features, combining correlated features into a single construct, or choosing a reference coding approach for categorical variables to avoid perfect redundancy. In causal (explanatory) work, the goal is not to maximize the number of predictors, but to ensure that each coefficient corresponds to a distinct and interpretable concept.

Multicollinearity affects how confidently we can interpret coefficients, but it does not imply time dependence or sequencing in the data. That concern is addressed by the next assumption: autocorrelation.

Finally, the table below summarizes various patterns of multicollinearity, where you might see them, what it often means, and what you should do about it:

---

## 10.5 Autocorrelation

**Autocorrelation** — Correlation between observations caused by time ordering or sequence dependence. Autocorrelation occurs when residuals from one observation are correlated with residuals from nearby observations, typically because the data is ordered in time. This matters for causal (explanatory) regression because autocorrelation can make _standard errors_ misleading, which undermines confidence intervals and hypothesis tests.

This assumption applies primarily to _time-series_ or sequential data. In cross-sectional datasets—where each row represents an independent entity—autocorrelation is usually not a concern. For the causal inference focus of this chapter, the key takeaway is simple: you must check autocorrelation when your observations have a meaningful order (usually time), because that order can invalidate standard inference.

It is important to distinguish autocorrelation from multicollinearity. Autocorrelation concerns relationships _between observations_ (rows), not relationships between features (columns). Multicollinearity threatens your ability to interpret coefficients; autocorrelation threatens the validity of your standard errors and tests.

For example, using _age in 2018_ and _age in 2020_ as separate features creates multicollinearity, not autocorrelation. Autocorrelation arises when residuals are linked across time (for example, today’s error is related to yesterday’s error).

The _Durbin-Watson (DW) statistic_ is the most common test for first-order autocorrelation in regression residuals. It ranges from 0 to 4:

- DW ≈ 2: No autocorrelation (ideal for inference).
- DW < 2: Positive autocorrelation.
- DW > 2: Negative autocorrelation.

Positive autocorrelation means that errors tend to repeat in the same direction over time, while negative autocorrelation indicates alternating error patterns. Both violate the OLS assumption that residuals are independent across observations.

When autocorrelation is present, coefficient estimates can remain unbiased under common conditions, but standard errors are incorrect. This creates false confidence: p-values may look “significant” and confidence intervals may look “tight” even when inference is not valid. Because this chapter emphasizes causal (explanatory) interpretation, autocorrelation is a high-priority diagnostic whenever the data is time-ordered.

#### Illustrating Autocorrelation with a Cross-Sectional Dataset

The insurance dataset is cross-sectional, so we do not expect autocorrelation. However, it is still useful to practice the diagnostic workflow. The key idea is that autocorrelation is about whether residuals are related across an observation order. For time-series data, that order is time. For cross-sectional data, the row order is usually arbitrary, so a good diagnostic should show no systematic pattern.

We will fit an OLS model, report the Durbin-Watson statistic, and then visualize whether residuals show any run-like pattern when plotted in row order. Finally, we will compute the lag-1 correlation of residuals as an intuitive check. In a causal workflow, this step is a quick way to confirm that independence across observations is a reasonable assumption before you proceed to functional-form diagnostics such as linearity.

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.stats.stattools import durbin_watson

# 1) Load + dummy-code (cross-sectional)
df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

# 2) Fit OLS
y = df["charges"].astype(float)
X = sm.add_constant(df.drop(columns=["charges"]), has_constant="add")
m = sm.OLS(y, X).fit()

# 3) Durbin-Watson (also appears in m.summary())
dw = durbin_watson(m.resid)
print(f"Durbin-Watson: {dw:.3f}")

# 4) Residuals in observation order (should look patternless for cross-sectional data)
r = np.asarray(m.resid, float)

plt.figure(figsize=(7, 3.8))
plt.plot(r, linewidth=1)
plt.axhline(0, linewidth=1)
plt.xlabel("Row index (observation order)")
plt.ylabel("Residual")
plt.title("Residuals in row order (insurance is cross-sectional)")
plt.tight_layout()
plt.show()

# 5) Simple lag-1 correlation as an intuitive check
lag1_corr = np.corrcoef(r[1:], r[:-1])[0, 1]
print(f"Lag-1 residual correlation: {lag1_corr:.4f}")

# Output (example format):
# Durbin-Watson: 2.088
# Lag-1 residual correlation: -0.0456
```

In the output printed above, the Durbin-Watson statistic is approximately 2, which is the “no autocorrelation” benchmark. In the figure, the residuals also do not show long runs of mostly-positive or mostly-negative values. Taken together, those two signals support the conclusion that autocorrelation is not a concern for this cross-sectional dataset.

The lag-1 residual correlation provides an intuitive complement to Durbin–Watson. In the printed output above, the lag-1 correlation is close to zero, meaning each residual is not meaningfully related to the residual immediately before it in row order. For causal (explanatory) regression, this is what you want to see: if errors were serially dependent, your standard errors and p-values could be overly optimistic.

Because the insurance dataset is not time-ordered, the row index is not a meaningful sequence. For time-series data, you would plot residuals against time (or observation order by time) and interpret Durbin-Watson in that context.

Autocorrelation is most critical in forecasting problems, sensor data, financial time series, and any setting where observations are ordered and influence one another. In those settings, autocorrelation is a direct threat to inference if you attempt to use standard OLS standard errors.

In those cases, ordinary least squares may be inappropriate, and specialized approaches such as generalized least squares, Newey–West (HAC) robust standard errors, or time-series models are often required.

Having confirmed that observation independence is reasonable for this dataset, we can next evaluate whether the relationship between features and the label is appropriately modeled as linear in the model space.

---

## 10.6 Linearity

The linearity assumption states that the relationship between each feature and the label can be reasonably approximated by a straight line, holding other variables constant. For causal (explanatory) modeling, this assumption is especially important because a mis-specified functional form can bias coefficient estimates and lead to misleading conclusions about effect sizes.

This assumption applies to the _functional form_ of the relationship—not to the raw data values themselves. In other words, the question is not whether the raw scatterplot looks like a straight line, but whether the model structure correctly represents how the expected outcome changes with each predictor.

Linearity does not require that features themselves be normally distributed, nor that the relationship be visually straight in raw scatterplots. Instead, it requires that the expected value of the label changes linearly with each feature _in the model space_ after any transformations you choose to apply.

A practical way to assess linearity is to look for systematic structure in errors. When linearity is violated, residuals often show curved or wave-like patterns when plotted against fitted values or against a specific feature. In causal modeling, these patterns are a warning sign that the coefficient estimates may be describing the wrong relationship.

![Examples of Linear and Non-Linear Correlations](../Images/Chapter10_images/perfect_corrs.png)

The left plot shows a perfect linear relationship, which is ideal for multiple linear regression. The middle plot shows a curved relationship, where a straight line would systematically misrepresent the pattern. The right plot demonstrates how polynomial terms (such as x² or x³) can restore linearity in the model space.

When a feature violates the linearity assumption, the solution is often not to abandon regression, but to revise the model’s functional form. Common approaches include logarithmic, square root, exponential, or polynomial transformations.

These transformations change the _scale_ of the feature so that the relationship becomes approximately linear in the transformed space. For causal inference, the goal is not simply a better fit, but a functional form that makes coefficient interpretation more defensible.

Linearity is most critical when the goal is relationship _interpretation_. If coefficients are used to explain cause-and-effect relationships, violations of linearity can lead to misleading conclusions because the model is estimating the wrong functional form.

For prediction tasks, mild nonlinearity can be less damaging, and many machine learning models capture nonlinear patterns automatically. In contrast, multiple linear regression requires the analyst to specify the functional form explicitly, which is why linearity diagnostics play such a central role in an inference-focused workflow.

From a deployment perspective, linearity matters because transformations applied during training must be applied identically during inference. Any transformation used to correct nonlinearity becomes part of the production pipeline. Even though this chapter emphasizes causal interpretation, this reminder helps reinforce that diagnostic fixes have implementation consequences.

In this section, we will use the insurance dataset to (1) visualize potential nonlinear patterns, (2) fit a baseline model, (3) add nonlinear terms (polynomials and logs), and (4) compare whether residual patterns become more random. The emphasis throughout is diagnostics-driven refinement for more trustworthy coefficient interpretation.

Before adding nonlinear terms to a full multiple regression model, it helps to look at simple bivariate relationships. If a feature has an obviously curved relationship with _charges_, a straight-line term may systematically miss that pattern, which often shows up later as curved structure in residual plots.

In the insurance dataset, _age_ often shows accelerating changes in _charges_ at older ages, which makes a quadratic term (_age²_) a reasonable first attempt. Likewise, _bmi_ often relates to _charges_ in a way that becomes steeper at higher BMI values, which makes a logarithmic transform (_log(bmi)_) a reasonable first attempt.

The code below produces two scatterplots (Age vs Charges, BMI vs Charges). Each plot overlays a linear trendline and a curved alternative, and annotates each trendline with its _R²_ and the key term’s _p-value_, so you can see whether the curved form fits better. In an inference-focused workflow, this is a preliminary justification step before you confirm improvements through residual diagnostics.

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

# Load insurance (update path if needed)
df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

y = df["charges"].astype(float)

def fit_lin(x, y):
  X = sm.add_constant(x, has_constant="add")
  return sm.OLS(y, X).fit()

def fit_quad(x, y):
  X = pd.DataFrame({"x": x, "x2": x**2})
  X = sm.add_constant(X, has_constant="add")
  return sm.OLS(y, X).fit()

def fit_logx(x, y):
  x_ln = np.log(x)
  X = sm.add_constant(x_ln, has_constant="add")
  return sm.OLS(y, X).fit()

def line_from_model(x_grid, model, kind):
  if kind == "lin":
    Xg = sm.add_constant(x_grid, has_constant="add")
    return model.predict(Xg)
  if kind == "quad":
    Xg = pd.DataFrame({"x": x_grid, "x2": x_grid**2})
    Xg = sm.add_constant(Xg, has_constant="add")
    return model.predict(Xg)
  if kind == "logx":
    Xg = sm.add_constant(np.log(x_grid), has_constant="add")
    return model.predict(Xg)

# 1) Age vs charges (linear vs quadratic)
x_age = df["age"].astype(float)
m_age_lin = fit_lin(x_age, y)
m_age_quad = fit_quad(x_age, y)

age_grid = np.linspace(x_age.min(), x_age.max(), 250)
y_age_lin = line_from_model(age_grid, m_age_lin, "lin")
y_age_quad = line_from_model(age_grid, m_age_quad, "quad")

# 2) BMI vs charges (linear vs log(BMI))
x_bmi = df["bmi"].astype(float)
m_bmi_lin = fit_lin(x_bmi, y)
m_bmi_log = fit_logx(x_bmi, y)

bmi_grid = np.linspace(max(1e-6, x_bmi.min()), x_bmi.max(), 250)
y_bmi_lin = line_from_model(bmi_grid, m_bmi_lin, "lin")
y_bmi_log = line_from_model(bmi_grid, m_bmi_log, "logx")

fig, ax = plt.subplots(1, 2, figsize=(13, 4.5), constrained_layout=True)

ax[0].scatter(x_age, y, s=10, alpha=0.35)
ax[0].plot(age_grid, y_age_lin, linewidth=2, label=f"Linear: R2={m_age_lin.rsquared:.3f}, p(age)={m_age_lin.pvalues.iloc[1]:.2g}")
ax[0].plot(age_grid, y_age_quad, linewidth=2, label=f"Quadratic: R2={m_age_quad.rsquared:.3f}, p(age^2)={m_age_quad.pvalues['x2']:.2g}")
ax[0].set_title("Age vs Charges: Linear vs Age²")
ax[0].set_xlabel("age")
ax[0].set_ylabel("charges")
ax[0].legend(frameon=False, fontsize=9)

ax[1].scatter(x_bmi, y, s=10, alpha=0.35)
ax[1].plot(bmi_grid, y_bmi_lin, linewidth=2, label=f"Linear: R2={m_bmi_lin.rsquared:.3f}, p(bmi)={m_bmi_lin.pvalues.iloc[1]:.2g}")
ax[1].plot(bmi_grid, y_bmi_log, linewidth=2, label=f"log(BMI): R2={m_bmi_log.rsquared:.3f}, p(log(bmi))={m_bmi_log.pvalues.iloc[1]:.2g}")
ax[1].set_title("BMI vs Charges: Linear vs log(BMI)")
ax[1].set_xlabel("bmi")
ax[1].set_ylabel("charges")
ax[1].legend(frameon=False, fontsize=9)

plt.show()
```

In the Age vs Charges panel, compare the two annotated _R²_ values shown in the legend. If the quadratic model’s _R²_ is meaningfully higher than the linear model’s _R²_ and the _p-value_ for _age²_ is small, that is evidence that a curved relationship is present and that a straight-line age term alone is missing structure. For causal interpretation, this matters because the “effect of age” is not constant across the age range when the relationship is curved.

In the BMI vs Charges panel, compare the linear trendline to the _log(BMI)_ trendline. If the _log(BMI)_ model shows higher _R²_ and a small _p-value_ for the log term, that supports trying a log transform in the multivariate model. Even if the improvement is modest, this bivariate view provides a concrete rationale for the transformation before you evaluate residual diagnostics in the full regression.

When you add a squared term (such as _age²_) to a model that already includes the linear term (such as _age_), both terms work _together_ to capture the non-linear relationship. This is different from replacing the linear term with a squared term—you keep both because they serve different purposes.

The linear term (e.g., _age_) captures the base effect or starting slope of the relationship. The squared term (e.g., _age²_) captures how that effect changes as the feature value increases—it models the curvature. Together, they allow the model to represent relationships where the effect of a feature depends on its current value.

For example, if a model includes both _age_ and _age²_, the effect of increasing age by one year is not constant. Instead, the marginal effect depends on the current age value. At younger ages, the effect might be smaller, while at older ages, the effect might be larger (or vice versa, depending on the coefficient signs). This is why both terms are needed: the linear term provides the baseline, and the squared term adjusts that baseline based on the feature's value.

Importantly, you cannot interpret the linear and squared coefficients separately. They must be interpreted together because they jointly determine how the feature influences the outcome. The coefficient for _age_ alone does not tell you the effect of age—you need both coefficients to understand the relationship at any given age value.

This also explains why polynomial terms and their base features will have high variance inflation factors (VIF) with each other—they are naturally correlated. However, this high VIF is _expected and acceptable_ for polynomial terms because both are necessary to capture the non-linear relationship. Removing one would eliminate the ability to model the curvature, which defeats the purpose of adding the polynomial term in the first place.

For causal (explanatory) modeling, understanding that marginal effects depend on current values is crucial for accurate interpretation. When you report that "increasing age by one year increases charges by X dollars," that statement is only valid at a specific age value. The effect will be different at age 30 versus age 60, which is exactly what polynomial terms allow the model to capture.

We begin by fitting a baseline OLS model and then plotting residuals versus fitted values. For causal (explanatory) modeling, the purpose of this plot is not “to get the best-looking scatter,” but to check whether the model is systematically mis-specifying the functional form. When residuals show clear curvature, waves, or structured clustering, the model may be assigning biased or misleading effect estimates to one or more predictors.

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

y = df["charges"].astype(float)
X = sm.add_constant(df.drop(columns=["charges"]), has_constant="add")
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

m_base = sm.OLS(y, X).fit()

fitted = np.asarray(m_base.fittedvalues, float)
resid = np.asarray(m_base.resid, float)

plt.figure(figsize=(6.5, 4.5))
plt.scatter(fitted, resid, s=10, alpha=0.5)
plt.axhline(0, linewidth=1)
plt.xlabel("Fitted values")
plt.ylabel("Residuals")
plt.title("Baseline model: residuals vs fitted")
plt.tight_layout()
plt.show()

# Optional:
# print(m_base.rsquared, m_base.rsquared_adj)
```

![Residuals vs fitted values for a baseline OLS model on the insurance dataset, used to visually assess linearity.](../Images/Chapter10_images/linearity_resid_vs_fitted_baseline.png)

In the baseline residuals versus fitted plot above, the horizontal line at zero represents “perfect” predictions (no error). Each point shows one observation’s error: points above zero are underpredictions (actual charges are higher than predicted), and points below zero are overpredictions.

For the linearity assumption, the key question is whether residuals look like random noise around zero across the fitted-value range. In this dataset, the residuals form visible clusters and non-random structure rather than a single, pattern-free cloud. Some of that structure is driven by strong subgroup separation (especially smokers versus non-smokers), but curvature within clusters can also indicate that one or more numeric relationships are not well captured by a straight-line functional form.

In regression output, _fitted values_ (sometimes called predicted values) are the model’s estimated values of the label for each observation, based on the learned coefficients and the observed feature values. Each fitted value represents what the model expects the outcome to be for that row, given its inputs.

Next, we add a small number of targeted nonlinear terms. The goal is not to add complexity everywhere, but to adjust specific relationships when diagnostics suggest curvature. Two common approaches are (1) polynomial terms (such as _age²_) and (2) log transforms (such as _log(bmi)_). In a causal workflow, these terms are justified when they reduce systematic residual structure and make the coefficient interpretation closer to the true relationship.

A useful rule of thumb is to start simple: add one nonlinear term at a time, refit, and then check whether residual patterns become more random. If residual structure remains, that suggests the model is still missing key functional form details.

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

# Baseline model
y = df["charges"].astype(float)
X = sm.add_constant(df.drop(columns=["charges"]), has_constant="add")
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)
m_base = sm.OLS(y, X).fit()

# Add targeted nonlinear terms
df_nl = df.copy()
df_nl["age_sq"] = df_nl["age"] ** 2
df_nl["bmi_ln"] = np.log(df_nl["bmi"])

y2 = df_nl["charges"].astype(float)
X2 = sm.add_constant(df_nl.drop(columns=["charges"]), has_constant="add")
X2[X2.select_dtypes(bool).columns] = X2.select_dtypes(bool).astype(int)
m_nl = sm.OLS(y2, X2).fit()

print(f"Baseline:  R2={m_base.rsquared:.4f}, Adj R2={m_base.rsquared_adj:.4f}")
print(f"Nonlinear: R2={m_nl.rsquared:.4f}, Adj R2={m_nl.rsquared_adj:.4f}")

# Residuals vs fitted for the nonlinear model
fitted2 = np.asarray(m_nl.fittedvalues, float)
resid2 = np.asarray(m_nl.resid, float)

plt.figure(figsize=(6.5, 4.5))
plt.scatter(fitted2, resid2, s=10, alpha=0.5)
plt.axhline(0, linewidth=1)
plt.xlabel("Fitted values")
plt.ylabel("Residuals")
plt.title("Nonlinear terms added: residuals vs fitted")
plt.tight_layout()
plt.show()
```

![Residuals vs fitted values after adding nonlinear terms (e.g., age squared and log BMI) to improve linearity in the insurance OLS model.](../Images/Chapter10_images/linearity_resid_vs_fitted_nonlinear.png)

Compare the nonlinear residual plot to the baseline plot. The purpose of adding _age_sq_ and _bmi_ln_ is to reduce curvature within the numeric relationships so that fewer systematic patterns remain in the residuals.

In this dataset, residuals may still form strong bands and clusters because smoker status creates a large shift in typical charges. However, within the main clusters, the nonlinear model often reduces bowed structure that appears when age and BMI effects are forced into straight-line terms. In other words, the squared and log terms explain part of the curvature that previously showed up as systematic error.

Notice that _R²_ typically increases when you add nonlinear terms because the model becomes more flexible. For causal modeling, the more important question is whether the added complexity is justified by improved diagnostic behavior (residual patterns become more random) and by clearer, more defensible coefficient interpretation.

The distinct groupings in the residual plot suggest that the relationship between the predictors and _charges_ may differ across subgroups (especially smokers versus non-smokers). A common way to represent this in a linear regression is with an _interaction term_, which allows the slope of one feature to change depending on the value of another feature.

Conceptually, adding an interaction term tells the model: “The effect of this feature may not be the same for every subgroup.” In an inference-focused workflow, interactions matter because a single pooled coefficient can mask subgroup-specific effects, which can lead to incorrect conclusions about how a variable influences the outcome.

```python
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

# Assumes df is already loaded and dummy-coded (including smoker_yes)
df_int = df.copy()
smoker_col = "smoker_yes"

# Nonlinear terms
df_int["age_sq"] = df_int["age"] ** 2
df_int["bmi_ln"] = np.log(df_int["bmi"])

# Interaction terms (allow slopes to differ by smoker status)
df_int["age_x_smoker"] = df_int["age_sq"] * df_int[smoker_col]
df_int["bmi_x_smoker"] = df_int["bmi_ln"] * df_int[smoker_col]

y3 = df_int["charges"].astype(float)
X3 = sm.add_constant(df_int.drop(columns=["charges"]), has_constant="add")
X3[X3.select_dtypes(bool).columns] = X3.select_dtypes(bool).astype(int)
m_int = sm.OLS(y3, X3).fit()

# Compare fit summaries (optional)
# print(f"Baseline:   R2={m_base.rsquared:.4f}, Adj R2={m_base.rsquared_adj:.4f}")
# print(f"Nonlinear:  R2={m_nl.rsquared:.4f}, Adj R2={m_nl.rsquared_adj:.4f}")
# print(f"+Interact:  R2={m_int.rsquared:.4f}, Adj R2={m_int.rsquared_adj:.4f}")

plt.figure(figsize=(6.5, 4.5))
plt.scatter(m_int.fittedvalues, m_int.resid, s=10, alpha=0.5)
plt.axhline(0, linewidth=1)
plt.xlabel("Fitted values")
plt.ylabel("Residuals")
plt.title("Interaction terms added: residuals vs fitted")
plt.tight_layout()
plt.show()
```

![Residuals vs fitted values after adding interaction terms to allow subgroup-specific slopes in the insurance OLS model.](../Images/Chapter10_images/linearity_resid_vs_fitted_interaction.png)

After adding interaction terms, residual plots often show fewer distinct bands and less within-group curvature. The key diagnostic improvement is that subgroup-specific structure that previously appeared as systematic error is now represented directly in the model. Even if residuals are not perfectly pattern-free, a reduction in visible structure suggests that the functional form is closer to correct for explanatory interpretation.

When you add an interaction term (such as _age_sq × smoker_yes_), a common question arises: should you keep the original terms (_age_sq_ and _smoker_yes_) in the model, or can you remove them if they have high VIF with the interaction term?

The general rule, known as the _hierarchical principle_, is to _keep the original terms_ (the main effects) when you include an interaction. This principle states that if you include an interaction term, you should also include all lower-order terms (the main effects) that make up that interaction.

There are several important reasons for this:

1. Interpretation: The main effect of _age_sq_ represents the effect of age (squared) when _smoker_yes = 0_ (the reference group, non-smokers). The interaction term shows how that effect changes when _smoker_yes = 1_ (smokers). Without the main effect, you cannot properly interpret what the baseline effect is, making the interaction coefficient meaningless.
1. Model completeness: The interaction captures how the effect of one variable depends on another, but the main effects provide the baseline effects. Together, they form a complete representation of the relationship. Removing main effects creates an incomplete model that may misrepresent the true relationships.
1. Statistical practice: This follows standard practice in regression modeling. Most statistical textbooks and applied research maintain main effects when interactions are present, unless there is a strong theoretical reason to do otherwise.

What about multicollinearity? Interaction terms will naturally have high variance inflation factors (VIF) with their component terms—this is _expected and acceptable_, similar to how polynomial terms have high VIF with their base features. The high correlation between an interaction term and its components does not mean you should remove the main effects. Instead, you should accept this as a necessary consequence of modeling how effects vary across subgroups.

There are rare exceptions where you might consider removing a main effect:

1. The main effect is truly zero (not just non-significant) and you have strong theoretical justification that the variable has no direct effect, only an effect through the interaction.
1. You are intentionally replacing (rather than supplementing) the functional form, and the transformed/interaction version fully captures what the original term represented.

However, these exceptions are controversial and should be used sparingly. For most causal (explanatory) modeling purposes, the hierarchical principle should be followed: keep the main effects when you include interactions. This ensures that your model can be properly interpreted and that coefficient estimates reflect meaningful relationships rather than artifacts of model specification.

When checking VIF after adding interaction terms, you may see high VIF values (often above 10) between interaction terms and their component features. This is expected and generally acceptable for interaction terms, just as it is for polynomial terms. Focus your VIF concerns on other sources of multicollinearity (such as redundant features that don't serve a specific modeling purpose) rather than removing main effects to reduce VIF.

When nonlinear and interaction terms reduce visible curvature, banding, or clustering in residual plots, it suggests that the model’s functional form better reflects the underlying relationships in the data. In practical terms, the model is explaining structure that earlier versions treated as noise. Increases in _R²_ and _Adjusted R²_ often accompany this improvement, but in this chapter the emphasis is on diagnostics: a better-looking residual plot supports more trustworthy coefficient interpretation.

However, nonlinear and interaction terms can also create new issues, especially multicollinearity (for example, _age_, _age_sq_, and _age_x_smoker_ will naturally be related). For this reason, linearity-driven feature engineering should be followed by multicollinearity checks before you finalize an inference-oriented model.

In a causal (explanatory) workflow, functional form is part of the identification story. If the true relationship is nonlinear or differs by subgroup, a purely linear specification can misattribute effects and produce coefficients that do not represent the intended “holding all else constant” interpretation. Linearity diagnostics help you revise the model so that coefficient-based explanations are more defensible.

A practical workflow is to treat linearity diagnostics as a feature engineering guide: start with simple transformations for obvious curvature, add interactions when subgroup-specific slopes are plausible, and then re-check multicollinearity and residual behavior after each change. For causal modeling, these steps help ensure that coefficients reflect the relationships you intend to interpret.

Even when relationships are closer to linear in the model space, prediction errors may still behave unevenly across different fitted values. That leads to the next assumption: homoscedasticity.

---

## 10.7 Heteroscedasticity

Homoscedasticity is the assumption that the variance of the residuals remains constant across all values of the independent variables. In a well-behaved regression model, prediction errors should be spread evenly rather than growing or shrinking systematically as predicted values increase.

When this assumption is violated, the model exhibits _heteroscedasticity_, meaning that the size of the errors depends on the level of the prediction. In practice, this often appears as residuals that fan outward, compress inward, or change shape across the range of fitted values.

Heteroscedasticity does not automatically mean the model is unusable. Instead, it signals that the model’s uncertainty is uneven: some parts of the prediction range are much more reliable than others. For causal (explanatory) modeling, that uneven uncertainty matters because it can distort the precision we assign to coefficient estimates.

In the examples above, residual variance changes as the value of the independent variable increases. In the left panel, errors widen for larger x-values; in the right panel, errors narrow. In both cases, the average relationship may still be approximately correct, but uncertainty is not constant.

Homoscedasticity is especially important for _causal and explanatory modeling_. When variance is unequal, standard errors from ordinary least squares are no longer reliable, which affects confidence intervals, hypothesis tests, and p-values. The practical consequence is that a variable may appear statistically significant (or not significant) for the wrong reason: the model is mis-estimating uncertainty.

For _prediction-focused_ projects, heteroscedasticity is often less damaging to point forecasts. The model may still predict well on average, even if error variance changes across cases. However, heteroscedasticity still matters when you need calibrated prediction intervals, risk estimates, or consistent accuracy across low- and high-risk segments.

In the rest of this section, you will learn how to detect heteroscedasticity using both residual diagnostics and a formal statistical test. Then you will learn why some responses primarily improve _inference_ (making standard errors more trustworthy), while others change how the model is fit (which can be useful, but must be justified carefully for causal interpretation).

#### Detecting Heteroscedasticity

Before attempting to respond to heteroscedasticity, we must first determine whether it is present. This is typically done using a combination of visual diagnostics and formal statistical tests.

We will again use the insurance dataset. In many insurance settings, variance naturally grows with risk level: high-cost cases tend to be more variable than low-cost cases. That business reality makes this dataset a useful illustration of heteroscedasticity and why it matters for coefficient-level inference.

A residuals-versus-fitted-values plot is the most common visual diagnostic for heteroscedasticity. If the spread of residuals increases or decreases systematically as fitted values grow, the constant-variance assumption is violated.

Look again at the residual plot from the prior section. Notice that there is a highly dense region of points along with fanned-out, lower-density regions as fitted values increase. This pattern indicates heteroscedasticity because residual variance is not stable throughout the prediction range.

![Residuals vs fitted values after adding interaction terms to improve functional form in the insurance OLS model.](../Images/Chapter10_images/linearity_resid_vs_fitted_interaction.png)

This indicates that prediction errors tend to grow as expected charges increase, even if the average relationship is captured reasonably well. For causal modeling, the concern is not primarily the fan shape itself, but what it implies: uncertainty is being misestimated, which can make standard errors and p-values misleading.

While visual inspection is informative, formal statistical tests provide additional confirmation. The _Breusch–Pagan test_ evaluates whether residual variance depends on the fitted values or the independent variables.

```python
from statsmodels.stats.diagnostic import het_breuschpagan

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)

y = df["charges"].astype(float)
X = sm.add_constant(df.drop(columns=["charges"]), has_constant="add")
X[X.select_dtypes(bool).columns] = X.select_dtypes(bool).astype(int)

# Fit baseline OLS model
model = sm.OLS(y, X).fit()

# Extract residuals
residuals = model.resid

# Breusch–Pagan test
bp_test = het_breuschpagan(residuals, X)

bp_results = pd.Series(
  bp_test,
  index=["Lagrange Multiplier", "p-value", "f-value", "f p-value"]
)

bp_results
```

A small p-value in the Breusch–Pagan test indicates evidence of heteroscedasticity. In the insurance dataset, this test typically confirms what we observed visually: residual variance is not constant.

Once heteroscedasticity has been identified, the next step is deciding how—or whether—to respond. Different responses target different consequences of heteroscedasticity, and the best choice depends on whether your goal is causal inference or prediction.

#### Responding to Heteroscedasticity

Once heteroscedasticity has been detected, the next step is deciding how to respond. Importantly, not all responses change the model in the same way. Some approaches adjust only how uncertainty is estimated, while others change how the model is fit.

In this chapter’s causal (inference) framing, the primary objective is to produce _defensible standard errors and hypothesis tests_. For that reason, we distinguish between approaches that keep the OLS coefficient estimates but correct inference, and approaches that change the coefficient estimates by reweighting the data.

We focus on three commonly used responses:

- Using heteroscedasticity-robust standard errors (HC3)
- Transforming the label (introduced earlier)
- Using weighted least squares (WLS)

We demonstrate HC3 and WLS here. Label transformations were covered earlier and will be combined with other adjustments later in the chapter.

HC3 does _not_ change the fitted values of the model. Instead, it adjusts the estimated standard errors to account for non-constant variance. This makes hypothesis tests and confidence intervals more reliable without changing predictions or the underlying OLS coefficient estimates.

HC3 is often the preferred response when the goal is _causal or explanatory analysis_ and the model specification is otherwise appropriate. In other words, if you believe your functional form is reasonable and you want inference that is more robust to unequal variance, HC3 typically improves the trustworthiness of standard errors without changing the meaning of your coefficient estimates.

```python
# OLS with HC3 robust standard errors
model_hc3 = model.get_robustcov_results(cov_type="HC3")

# Compare summaries
print(model.summary())
print(model_hc3.summary())
```

Notice that the coefficient estimates remain identical, but the standard errors and p-values change. This reflects more realistic uncertainty when residual variance is uneven. In other words, we did not “fix the residual plot,” but we did improve how confidently we can interpret coefficient evidence.

Weighted least squares addresses heteroscedasticity by changing how the model is fit. Observations that are expected to have larger error variance receive less weight, while more stable observations receive more influence.

Unlike HC3, WLS changes coefficient estimates and fitted values. This can be appropriate when you have a credible variance model and your goal is to estimate effects more efficiently under unequal variance. However, in a causal (inference) workflow, WLS requires an additional assumption: you must correctly specify (or approximate well) the weighting structure. If the weights are poorly chosen, WLS can distort coefficient estimates and create misleading inference.

In practice, weights are often derived from a first-pass OLS model, but this is best viewed as a teaching demonstration rather than a universal recommendation. The reliability of WLS depends heavily on whether the weight formula matches the true error-variance pattern.

```python
# Estimate variance as a function of fitted values (simple demo weighting)
weights = 1 / (fitted ** 2)

# Fit WLS model
model_wls = sm.WLS(y, X, weights=weights).fit()

print(model_wls.summary())
```

This simple weighting scheme downweights high-cost predictions, where variance is often larger. More sophisticated variance models are possible, but this example illustrates the core idea: WLS prioritizes regions of the data where the model can learn more stable patterns.

![Composite comparison of residual plots before and after applying heteroscedasticity corrections.](../Images/Chapter10_images/heteroscedasticity_comparison.png)

The figure above compares residual patterns across several models. Relative to the baseline OLS model, the WLS model may produce residuals with more stable variance across fitted values, indicating improved homoscedasticity. However, for causal inference, the key tradeoff is that WLS changes the coefficient estimates and relies on the correctness of the weighting scheme. For that reason, this chapter emphasizes HC3 and transformations as the default tools for improving inference reliability, while treating WLS as an optional technique when a defensible variance model is available.

To summarize how these approaches differ—and when each is appropriate—we now present a concise comparison table.

#### Interpreting Results and Choosing a Response

After you run the code above and insert the composite figure, focus on what changes across models. In the original OLS residual plot, the vertical spread of residuals tends to widen as fitted values increase. This fan-shaped pattern is a classic sign of heteroscedasticity: the model’s errors are not equally variable across the prediction range.

When you refit the model using WLS, the goal is to reduce that fan shape. In an improved WLS residual plot, you should see a more uniform band of residuals around zero across low and high fitted values. This does not mean every point is “perfect,” but it does mean the model’s error variance is more stable.

HC3 behaves differently. Because HC3 changes only the standard errors, the residual plot does not change at all. The purpose of HC3 is not to “fix the picture,” but to make coefficient inference more trustworthy when heteroscedasticity is present.

Heteroscedasticity is a larger threat to _inference_ than to prediction. If your goal is to interpret coefficients causally, a common default is OLS with robust standard errors (such as HC3) and/or label transformations, because these approaches improve inference reliability without requiring you to correctly model the variance function. WLS can be useful, but it is most defensible when the weighting scheme is grounded in a credible variance model rather than chosen ad hoc.

The main lesson is not that every model must be “perfect.” The lesson is that diagnostic plots reveal _where_ your model is less reliable, and different responses target different consequences of heteroscedasticity.

In later sections, we will combine multiple targeted adjustments into a single refined model and compare it to the original untransformed model. For now, the goal is to recognize heteroscedasticity and understand the simplest, most common responses in a causal inference workflow.

---

## 10.8 Diagnostic-Adjusted Model for Causal Inference

In this chapter, we examined the major assumptions underlying multiple linear regression and demonstrated how diagnostic signals can guide thoughtful model improvements. Rather than treating assumption violations as binary failures, we used them as indicators of where the model could be refined to support more reliable statistical inference.

The goal of this summary section is _causal or explanatory modeling_. That means our primary concern is not maximizing predictive accuracy, but ensuring that coefficients, standard errors, confidence intervals, and hypothesis tests are as trustworthy as possible given the data.

Some of the modeling decisions made here—such as transforming the label, removing redundant predictors, or prioritizing diagnostic stability over raw fit—may reduce familiar fit metrics like _R²_. That tradeoff is intentional. In the next chapter, we revisit these same issues from a _prediction-focused_ perspective, where different choices are often preferable.

The diagnostic adjustments in this chapter address the following assumptions, each of which plays a direct role in the validity of regression-based inference:

- **Normality**: Extreme skewness in the label can distort residual behavior and invalidate standard errors; power transformations such as Box–Cox or Yeo–Johnson improve symmetry and stabilize inference.
- **Multicollinearity**: Strong overlap among predictors inflates standard errors and destabilizes coefficients, directly threatening interpretability and causal claims.
- **Autocorrelation**: Residual dependence violates independence assumptions and undermines inference in time-ordered data, but is not a concern for cross-sectional datasets like insurance.
- **Linearity**: The expected value of the label must be linear in the model space; nonlinear relationships require transformations, polynomial terms, or interactions to avoid functional form misspecification.
- **Homoscedasticity**: Unequal residual variance biases standard errors and hypothesis tests; robust standard errors and weighted least squares restore inferential reliability.

We now combine these insights to construct a single, well-specified model for the insurance dataset that prioritizes diagnostic validity and causal interpretability.

We begin by applying transformations that directly support causal inference by improving residual behavior, functional form, and interpretability. These adjustments are motivated by diagnostics rather than by maximizing predictive accuracy.

First, we transform the label using a _Box–Cox_ transformation. The insurance charges variable is heavily right-skewed, which leads to non-normal residuals and unstable variance. Because valid hypothesis tests and confidence intervals rely on approximately normal residuals, stabilizing the label distribution is an important step for explanatory modeling.

Next, we address nonlinear relationships by engineering transformed features for _age_ and _BMI_. Prior diagnostics showed curvature in these relationships, indicating that a straight-line functional form was misspecified. Adding nonlinear terms allows the model to better represent how changes in these predictors relate to the expected value of the label.

Finally, we introduce interaction terms with _smoker status_. Exploratory plots and residual patterns indicated that the effect of age and BMI differs substantially between smokers and non-smokers. Interaction terms allow slopes to vary across groups, which is essential for correctly attributing effects in causal analysis.

```python
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.preprocessing import PowerTransformer

# Load and dummy-code
df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)
df[df.select_dtypes(bool).columns] = df[df.select_dtypes(bool).columns].astype(int)

# --------------------------------------------------
# Label transformation (supports inference, not fit)
# --------------------------------------------------
pt = PowerTransformer(method="box-cox", standardize=False)
df["charges_bc"] = pt.fit_transform(df[["charges"]])

# --------------------------------------------------
# Nonlinear terms
# --------------------------------------------------
df["age_sq"] = df["age"] ** 2
df["bmi_ln"] = np.log(df["bmi"])

# --------------------------------------------------
# Interaction terms with smoker status
# --------------------------------------------------
df["age_x_smoker"] = df["age_sq"] * df["smoker_yes"]
df["bmi_x_smoker"] = df["bmi_ln"] * df["smoker_yes"]
```

An important design choice concerns how interaction terms are defined. When diagnostics indicate that a nonlinear transformation is the appropriate functional form, it is typically more consistent to interact the _transformed feature_ with the subgroup indicator (for example, _age_sq × smoker_yes_ rather than _age × smoker_yes_). This allows the nonlinear relationship itself to differ by group.

Interacting the untransformed feature instead would answer a different question—namely, whether smoker status modifies the _linear_ slope. Because the diagnostics in this dataset point to nonlinear baseline relationships, we construct interactions using the transformed versions of _age_ and _BMI_. This choice prioritizes correct functional form and stable inference over simplicity.

In causal modeling, misspecified functional forms and unmodeled group differences can lead to biased or misleading coefficient estimates. Transformations and interaction terms help ensure that estimated effects reflect meaningful relationships rather than artifacts of curvature or aggregation across heterogeneous subgroups.

After creating nonlinear terms and interaction effects, we reassess multicollinearity. This step is especially important for causal inference because multicollinearity inflates standard errors, weakens hypothesis tests, and can make coefficient estimates unstable even when overall model fit appears strong.

It is important to evaluate multicollinearity _after_ feature engineering is complete. Polynomial terms, transformations, and interactions often introduce new correlations that were not present in the original feature set.

We use the _Variance Inflation Factor (VIF)_ to quantify how strongly each predictor can be explained by the remaining predictors. A VIF of 1 indicates no linear overlap, while larger values indicate increasing redundancy.

The intercept (constant) term frequently exhibits an extremely large VIF and is not interpreted as a multicollinearity problem. For that reason, we remove the constant row from the VIF table before drawing conclusions.

```python
from statsmodels.stats.outliers_influence import variance_inflation_factor
import pandas as pd
import statsmodels.api as sm

# Design matrix including all engineered features
X_vif = df.drop(columns=["charges", "charges_bc"])

# Ensure boolean columns are numeric
X_vif[X_vif.select_dtypes(bool).columns] = X_vif.select_dtypes(bool).astype(int)

# Add constant for matrix completeness
X_vif = sm.add_constant(X_vif, has_constant="add")

vif_df = pd.DataFrame({
  "feature": X_vif.columns,
  "VIF": [variance_inflation_factor(X_vif.values, i) for i in range(X_vif.shape[1])]
})

# Remove intercept before interpretation
vif_df = vif_df[vif_df["feature"] != "const"].sort_values("VIF", ascending=False)
vif_df
```

![VIF table showing multicollinearity diagnostics after adding transformed and interaction features.](../Images/Chapter10_images/VIF_table1.png)

As a reminder, earlier in the chapter we used conservative VIF guidance (for example, treating values above approximately 3 as an early warning). After feature engineering, it is normal to see higher VIF values because transformed terms (like _age_sq_) and interaction terms (like _age_x_smoker_) are intentionally constructed from existing variables.

The results show the expected pattern: engineered features are often correlated with their corresponding main effects. For example, _age_ tends to be correlated with _age_sq_, and _bmi_ tends to be correlated with _bmi_ln_. Likewise, an interaction term such as _age_x_smoker_ is mechanically related to both _age_ and _smoker_yes_ because it is built from them.

In this book, we do _not_ remove main-effect terms (such as _age_, _bmi_, or _smoker_yes_) simply because we added nonlinear terms and interactions. As explained earlier, keeping the main effects is important for correct specification and interpretation: interactions represent _differences in slopes between groups_, and nonlinear terms represent _curvature around a baseline relationship_. Dropping the underlying main effects can make the model harder to interpret and can unintentionally change what the interaction or nonlinear term means.

A high VIF does not automatically mean a predictor must be removed. After you add squared terms, logs, and interactions, some multicollinearity is expected because the engineered features share information with the variables used to construct them. The goal of VIF analysis is to identify when multicollinearity becomes severe enough to threaten numerical stability or make inference unusable. If the VIF table shows that only the engineered-feature families exhibit elevated VIF (for example, a main effect and its squared term), that is typically a modeling tradeoff rather than a modeling mistake.

In our results, no predictors besides the engineered-feature families trigger a meaningful multicollinearity concern under the VIF rule used in this chapter. Because we are intentionally keeping main effects alongside nonlinear and interaction terms (as instructed earlier), we will not eliminate any predictors at this stage. Instead, we proceed with the full engineered specification and interpret coefficients with appropriate caution: multicollinearity can increase uncertainty (wider confidence intervals), but it does not invalidate the model if the specification matches the theory and the diagnostics do not indicate numerical instability.

With functional form corrected and the label stabilized, we fit a final model designed explicitly for _causal interpretation_. The goal is not to maximize predictive accuracy, but to produce coefficients, standard errors, and confidence intervals that are statistically reliable and interpretable.

In Step 2, we used VIF as a diagnostic but, as discussed earlier in the chapter, we do _not_ drop main effects simply because we added nonlinear and interaction terms. We keep main effects alongside their engineered counterparts so the meaning of curvature and interactions remains well-defined (for example, interactions represent differences in slopes between groups, conditional on the baseline relationship).

Because heteroscedasticity affects inference rather than coefficient bias, we estimate the model using _ordinary least squares_ and report _HC3-robust standard errors_. This preserves meaningful _R²_ values on the transformed outcome scale while correcting standard errors for unequal variance.

```python
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.preprocessing import PowerTransformer, StandardScaler

df = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/data/insurance.csv")
df = pd.get_dummies(df, columns=df.select_dtypes(["object"]).columns, drop_first=True)
df[df.select_dtypes(bool).columns] = df[df.select_dtypes(bool).columns].astype(int)

pt = PowerTransformer(method="box-cox", standardize=False)
df["charges_bc"] = pt.fit_transform(df[["charges"]])

# Centering (helps numeric stability for squared terms and interactions)
age_c = df["age"] - df["age"].mean()
bmi_ln = np.log(df["bmi"])
bmi_ln_c = bmi_ln - bmi_ln.mean()

# Engineered terms
df["age_c_sq"] = age_c ** 2
df["bmi_ln_c"] = bmi_ln_c
df["age_c_sq_x_smoker"] = df["age_c_sq"] * df["smoker_yes"]
df["bmi_ln_c_x_smoker"] = df["bmi_ln_c"] * df["smoker_yes"]

# Final predictors (keep main effects + engineered terms)
features = [
  "children",
  "sex_male",
  "region_northwest",
  "region_southeast",
  "region_southwest",
  "age",                # main effect kept
  "bmi",                # main effect kept
  "smoker_yes",         # main effect kept
  "age_c_sq",
  "bmi_ln_c",
  "age_c_sq_x_smoker",
  "bmi_ln_c_x_smoker"
]

X = df[features].copy()

# Scale numeric predictors for numerical stability (not required for OLS, but helps conditioning)
numeric_cols = [
  "children",
  "age",
  "bmi",
  "age_c_sq",
  "bmi_ln_c",
  "age_c_sq_x_smoker",
  "bmi_ln_c_x_smoker"
]
X[numeric_cols] = StandardScaler().fit_transform(X[numeric_cols])

y = df["charges_bc"].astype(float)
X = sm.add_constant(X, has_constant="add")

m_final = sm.OLS(y, X).fit(cov_type="HC3")
print(m_final.summary())
```

Centering and scaling the predictors can substantially reduce the _condition number_, improving numerical stability without changing the underlying substantive meaning of the model. This does not “fix” multicollinearity by itself, but it helps ensure coefficient estimates are not overly sensitive to floating-point precision when squared terms and interactions are present.

Because we kept the main effects (_age_, _bmi_, and _smoker_yes_) alongside nonlinear and interaction terms, each engineered term has a clear interpretation: the squared and log terms represent curvature around the baseline effect, and the interaction terms represent how that curvature differs for smokers versus non-smokers (holding the main effects constant).

The _R²_ from this model should not be compared directly to earlier regressions on raw dollar charges. Because the label is measured in _Box–Cox(charges)_, _R²_ now describes variance explained on a transformed scale. For causal analysis, this shift is intentional: improving residual behavior and inference validity is more important than maximizing variance explained on the original outcome scale.

Although we introduced _weighted least squares (WLS)_ earlier as a response to heteroscedasticity, we do not use WLS in this final model. WLS requires stronger assumptions about the form of the error variance and depends on a correctly specified weighting scheme. Instead, we use _ordinary least squares with HC3-robust standard errors_, which corrects inference for heteroscedasticity while preserving unbiased coefficients and avoiding additional modeling assumptions. For causal and explanatory modeling, this approach prioritizes defensible hypothesis tests, stable standard errors, and transparent assumptions over efficiency gains that rely on unverifiable variance models.

This model supports _statistical explanation_ under the assumptions of linear regression. It does not, by itself, establish true causal effects in the experimental sense. Causal claims still depend on study design, omitted-variable considerations, and domain knowledge.

For this reason, we rely on _OLS with HC3-robust standard errors_ rather than weighted least squares in the final specification. Robust standard errors correct inference for heteroscedasticity without requiring strong assumptions about the exact form of the error variance, making them a more defensible default for causal and explanatory analysis when variance structure cannot be confidently specified.

This final model illustrates the central lesson of the chapter: regression diagnostics are not obstacles, but guides. When the goal is explanation rather than prediction, improving assumptions—even at the expense of familiar fit metrics—leads to more meaningful and defensible conclusions.

Now that we have constructed a more defensible and trustworthy regression model—one that better satisfies the assumptions required for statistical explanation—we can use its results to inform real business decisions. Because this model prioritizes interpretability and reliable inference, its coefficients highlight which factors are most strongly associated with insurance charges, holding other variables constant.

In this final diagnostic-adjusted model, the most influential features are those that (1) have large and statistically stable coefficients, (2) remain significant after correcting for multicollinearity and heteroscedasticity, and (3) participate in meaningful interaction effects. In particular, nonlinear age effects, BMI (on a log scale), and interactions with smoker status emerge as central drivers of cost differences.

These decisions should be framed as _evidence-informed_ rather than strictly causal in the experimental sense. The value of the model lies in clarifying patterns, tradeoffs, and risk drivers that are consistent with both the data and the modeling assumptions.

- Pricing strategy refinement: The large nonlinear effects of _age_ and _BMI_, combined with strong _smoker interaction terms_, suggest that uniform pricing rules are insufficient. Premium structures can be refined to reflect how risk accelerates for smokers as age and BMI increase.
- Targeted wellness programs: Because _BMI_ and _smoking status_ jointly amplify costs, wellness incentives aimed at smoking cessation and weight management are likely to yield the greatest marginal cost reductions.
- Customer segmentation: Interaction effects indicate that customers should be segmented by _combined risk profiles_ (for example, older smokers with high BMI), not by single characteristics in isolation.
- Policy design evaluation: Region indicators that remain statistically significant after diagnostics suggest persistent cost differences that may justify region-specific plan features or provider negotiations.
- Regulatory and actuarial justification: Because the model addresses normality, functional form, multicollinearity, and heteroscedasticity, its coefficient estimates provide stronger support for explaining pricing logic to regulators and stakeholders.

In short, diagnostics do not merely improve statistical cleanliness—they clarify which features deserve managerial attention. By identifying the strongest and most reliable cost drivers, the model supports decisions that are both analytically defensible and operationally actionable.

Suppose the coefficient on _bmi_ln × smoker_ is large and statistically significant. In plain business terms, this means that increases in BMI are associated with much higher expected insurance charges _for smokers than for non-smokers_, even after controlling for age, sex, region, and other factors.

A manager should not interpret this as “BMI causes higher costs,” but rather as evidence that _smoking status fundamentally changes how health risk scales with body mass_. This insight supports differentiated pricing, targeted interventions, or preventive programs aimed specifically at high-BMI smokers.

Because this conclusion is drawn from a diagnostics-adjusted model, the confidence intervals and hypothesis tests supporting it are more trustworthy than they would be in a model that ignored assumption violations.

In the next chapter, we revisit these same issues from a _prediction-focused_ perspective, where tradeoffs are evaluated differently and model performance is judged by out-of-sample accuracy rather than inferential stability.

---

## 10.9 Case Studies

This practice extends the Diamonds dataset from Chapter 9 to focus on **regression diagnostics and causal inference**. You will evaluate whether the assumptions required for valid statistical inference are satisfied and apply corrective modeling steps where appropriate.

**Dataset attribution:** The Diamonds dataset is distributed with the Seaborn data repository and can be loaded with _seaborn.load_dataset("diamonds")_.

```python
import seaborn as sns

df = sns.load_dataset("diamonds")
```

In this chapter, you are practicing **MLR for causal (explanatory) modeling**. That means your goal is not prediction accuracy, but producing a model with stable coefficients, valid standard errors, and defensible hypothesis tests.

- Fit the same baseline MLR model used in Chapter 9 predicting _price_.
- Evaluate regression assumptions using formal diagnostics and visual tools.
- Apply transformations or structural changes where assumptions are violated.
- Refit a diagnostic-adjusted causal model.
- Compare coefficient stability and inference quality before and after adjustment.

**Analytical questions (answers should be specific)**

1. What is the Durbin–Watson statistic for the baseline model, and what does it imply about autocorrelation?
1. Based on a residual histogram or Q–Q plot, is the normality assumption reasonably satisfied? Briefly justify.
1. Which numeric predictor has the highest Variance Inflation Factor (VIF)? Report its approximate value.
1. Does the residual vs. fitted plot suggest nonlinearity? If so, describe the pattern.
1. Does the scale–location (or residual spread) plot indicate heteroscedasticity? Explain.
1. Which diagnostic issue is most severe in this model: non-normality, multicollinearity, nonlinearity, or heteroscedasticity?
1. What transformation or modeling change did you apply to address this issue (e.g., log transform, polynomial term, interaction term, feature removal)?
1. After adjustment, how did the condition number change (increase, decrease, or remain similar)?
1. Which predictor’s statistical significance changed the most after diagnostic adjustment?
1. In one paragraph, explain why the adjusted model is more appropriate for causal interpretation than the original model.

### Diamonds Diagnostics Practice Answers

These answers were computed using an OLS multiple linear regression predicting _price_ with numeric predictors (_carat_, _depth_, _table_, _x_, _y_, _z_) and categorical predictors (_cut_, _color_, _clarity_), followed by diagnostic evaluation and model adjustment.

1. The Durbin–Watson statistic is approximately _2.01_, indicating no meaningful autocorrelation.
1. The residual distribution shows moderate right skew, indicating that strict normality is violated.
1. The predictor with the highest VIF is _x_, with VIF ≈ _45_, indicating severe multicollinearity.
1. The residual vs. fitted plot shows curvature, suggesting nonlinearity between predictors and price.
1. Residual variance increases with fitted values, indicating heteroscedasticity.
1. Multicollinearity is the most severe diagnostic issue.
1. Size variables (_x_, _y_, _z_) were removed and _carat_ retained as the primary size proxy.
1. The condition number decreased substantially after removing collinear size variables.
1. The coefficient for _depth_ changed sign and lost statistical significance.
1. The adjusted model produces more stable coefficients, lower multicollinearity, more reliable standard errors, and defensible hypothesis tests, making it suitable for causal interpretation.

This practice extends the Red Wine Quality regression you built earlier by applying the regression diagnostics from this chapter. Your goal is to evaluate whether the assumptions needed for trustworthy coefficient inference are reasonably satisfied, and then apply at least one diagnostic-adjusted remedy.

**Dataset attribution:** This dataset is commonly distributed as _winequality-red.csv_ from the UCI Machine Learning Repository (Wine Quality Data Set), originally published by Cortez et al. in “Modeling wine preferences by data mining from physicochemical properties” (Decision Support Systems, 2009).

The red wine quality dataset is available in the prior chapter if you need it.

**What you will do in this practice**

- Fit a baseline OLS multiple linear regression model predicting _quality_ using all other columns as numeric predictors (include an intercept).
- Run diagnostic checks for: normality, multicollinearity, autocorrelation, linearity, and heteroscedasticity.
- Create at least one diagnostic-adjusted model for causal inference (for example, HC3 robust standard errors and/or a multicollinearity remedy), then compare what changes.

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Red Wine Quality dataset?
1. Fit the baseline OLS model. What are _R²_ and _Adjusted R²_ (report both to 4 decimals)?
1. Normality: run a residual normality test (for example, Jarque–Bera). Report the test’s p-value and state whether the residuals appear normal at α = 0.05.
1. Autocorrelation: compute the Durbin–Watson statistic for the residuals. Report the value and interpret whether it suggests serious autocorrelation.
1. Multicollinearity: compute VIF for each predictor. Which predictor has the largest VIF, and what is that VIF value (rounded to 2 decimals)?
1. Heteroscedasticity: run the Breusch–Pagan test. Report the p-value and interpret the result at α = 0.05.
1. Linearity: examine a residuals-versus-fitted plot. In one or two sentences, describe whether you see evidence of systematic curvature or pattern.
1. Diagnostic-adjusted model: refit the model using HC3 robust standard errors. Do the coefficient estimates change? What changes, and why?
1. Multicollinearity remedy: standardize (z-score) all predictors and refit the same OLS model. What happens to the _condition number_ after scaling, and why?
1. Synthesis: based on your diagnostics, which assumption appears to be the most problematic for causal inference in this dataset, and what is your recommended response?

### Wine Practice Answers

These answers were computed by fitting an OLS multiple linear regression model predicting _quality_ from all remaining numeric columns in _winequality-red.csv_ (with an intercept), then running standard regression diagnostics (normality, multicollinearity, autocorrelation, heteroscedasticity) and demonstrating common inference-focused adjustments.

1. The Red Wine Quality dataset contains _1599_ rows and _12_ columns.
1. The mean value of _quality_ is _5.6360_.
1. For the baseline OLS model (all predictors), _R² = 0.3606_ and _Adjusted R² = 0.3561_ (reported to 4 decimals).
1. (Normality) The Jarque–Bera test on residuals produces a very small p-value (_p = 1.2696e-09_), indicating evidence that residuals are not normally distributed.
1. (Multicollinearity) The largest VIF in the baseline model is for _fixed acidity_ with _VIF = 7.7673_, indicating notable multicollinearity among some chemistry measures.
1. (Autocorrelation) The Durbin–Watson statistic is _1.7570_. Because these rows are not a time series, this is not typically treated as a critical threat to inference in this dataset.
1. (Heteroscedasticity) The Breusch–Pagan test provides strong evidence of non-constant variance (LM test p-value _p = 1.3264e-13_).
1. (Most significant predictor) The single predictor term with the smallest _P>|t|_ value is _alcohol_ (p-value is effectively 0 in typical printed summaries; in floating-point terms it is _1.1230e-24_).
1. (HC3 adjustment) When switching from non-robust OLS standard errors to _HC3_, coefficient estimates remain the same, but uncertainty estimates change. For example, the standard error for _alcohol_ increases from _0.0265_ to _0.0293_, and its p-value changes from _1.1230e-24_ to _1.2510e-20_ (still highly significant).
1. (Diagnostic-adjusted model example) One reasonable multicollinearity response is to remove _fixed acidity_ (the largest VIF term) and refit. After removing it, the maximum VIF drops to _5.4572_ (for _density_), while model fit remains essentially unchanged (_R² = 0.3604_, _Adjusted R² = 0.3562_).
1. (Numerical stability note) The baseline design matrix has a very large condition number (_113203.10_), which is driven largely by mixed feature scales. If predictors are standardized (z-scores) for numerical stability, the condition number drops substantially (to about _19.54_) while representing the same underlying model in standardized units.

This practice uses the **Bike Sharing** daily dataset (the _day.csv_ file). You will extend your Chapter 9 multiple linear regression work by applying the Chapter 10 diagnostic workflow (normality, multicollinearity, autocorrelation, linearity, and heteroscedasticity) and then fitting a diagnostic-adjusted model intended for **causal (explanatory) interpretation**.

**Dataset attribution:** This dataset is distributed as part of the Bike Sharing Dataset hosted by the UCI Machine Learning Repository (Fanaee-T and Gama). It includes daily rental counts and weather/context variables derived from the Capital Bikeshare system in Washington, D.C. You will use the _day.csv_ file provided with your course materials.

The bike sharing daily dataset is available in the prior chapter if you need it.

**Important modeling note:** Do not include _casual_ or _registered_ as predictors because they directly sum to _cnt_ and would leak the answer into the model.

**Tasks**

- Inspect the dataset: rows/columns, data types, and summary statistics for _cnt_.
- Fit the baseline OLS MLR model predicting _cnt_ using predictors: _season_, _yr_, _mnth_, _holiday_, _weekday_, _workingday_, _weathersit_, _temp_, _atemp_, _hum_, _windspeed_.
- Dummy-code the categorical predictors (_season_, _mnth_, _weekday_, _weathersit_) using _drop_first=True_, then fit the model with Statsmodels _OLS_.
- Run diagnostic tests and visuals: residual normality (histogram + Q–Q plot), multicollinearity (VIF), autocorrelation (Durbin–Watson), linearity (residuals vs fitted), and heteroscedasticity (Breusch–Pagan).
- Fit a diagnostic-adjusted model suitable for causal interpretation (for example: remove or combine highly collinear predictors, apply a transformation such as _log(cnt)_ if justified, and/or use heteroscedasticity-robust standard errors).
- Compare baseline vs adjusted model results and explain what changed (fit metrics, key coefficients, and diagnostic evidence).

**Analytical questions (answers should be specific)**

1. How many rows and columns are in the Bike Sharing daily dataset (_day.csv_)?
1. What is the mean value of _cnt_ in the dataset?
1. For the baseline model, what are _R²_ and _Adjusted R²_? Report both to 4 decimals.
1. Which single predictor term (feature or dummy-coded category) has the smallest _P>|t|_ value in the baseline model?
1. Normality: Based on the Q–Q plot and a normality test (for example, Jarque–Bera), do the residuals appear approximately normal? Answer yes/no and cite one piece of evidence from your output.
1. Multicollinearity: Compute VIF for all numeric predictors and dummy-coded terms. Which predictor has the largest VIF, and what is its VIF value (rounded to 2 decimals)?
1. Autocorrelation: What is the Durbin–Watson statistic for the baseline model (rounded to 3 decimals)? Based on this value, is autocorrelation a concern in this dataset?
1. Linearity: Inspect a residuals-vs-fitted plot. Does the plot suggest nonlinearity (systematic curve) or is it roughly random scatter? Briefly describe what you see.
1. Heteroscedasticity: Run the Breusch–Pagan test. Report the p-value (rounded to 4 decimals). Do you reject homoscedasticity at α = 0.05?
1. Diagnostic adjustment: Describe one concrete adjustment you made (or would make) to improve causal interpretability (for example, dropping one of two collinear predictors, transforming _cnt_, or using HC3 robust standard errors). Explain _why_ the adjustment is justified based on diagnostic evidence.
1. After your adjustment, report the adjusted model’s _R²_ and _Adjusted R²_ (4 decimals) and one diagnostic statistic that improved (for example, lower max VIF, higher p-value on Breusch–Pagan, or more stable residual pattern).
1. Short reflection (2–4 sentences): In causal regression, why might you accept a slightly worse fit if diagnostics and assumptions improve?

### Bike Sharing Diagnostics Answers

These answers were computed by fitting a baseline OLS multiple linear regression model predicting _cnt_ using predictors _season_, _yr_, _mnth_, _holiday_, _weekday_, _workingday_, _weathersit_, _temp_, _atemp_, _hum_, and _windspeed_, with categorical variables dummy-coded using _drop_first=True_ and an intercept term (_const_), then applying Chapter 10 diagnostics (Jarque–Bera, VIF, Durbin–Watson, residual plots, Breusch–Pagan) and a diagnostic-adjusted model.

1. The Bike Sharing daily dataset contains _731_ rows and _16_ columns.
1. The mean value of _cnt_ is _4504.3488_.
1. Baseline model fit: _R² = 0.8381_ and _Adjusted R² = 0.8312_ (reported to 4 decimals).
1. In the baseline model, the predictor term with the smallest _P>|t|_ is _yr_ (p-value shown as approximately 0.000 due to rounding).
1. Normality: _No_. The Jarque–Bera test rejects normality (p-value is effectively 0.0000), and the Q–Q plot shows systematic departures from the 45° line in the tails.
1. Multicollinearity: the largest VIF is _infinite_ for _weekday_4_ (and several related weekday dummy terms also show infinite VIF), indicating perfect multicollinearity in the baseline design matrix; report _VIF = inf_.
1. Autocorrelation: the Durbin–Watson statistic is _0.421_ (rounded to 3 decimals). Because this value is far below 2.0, _positive autocorrelation is a concern_ in this daily time-ordered dataset.
1. Linearity: the residuals-vs-fitted plot is _not purely random scatter_; it shows structure (a mild curve) and changing spread, suggesting the linearity/constant-variance assumptions are imperfect for the baseline specification.
1. Heteroscedasticity: the Breusch–Pagan test p-value is _0.0000_ (rounded to 4 decimals). At α = 0.05, _reject_ homoscedasticity.
1. Diagnostic adjustment (example): remove _workingday_ and _atemp_ from the baseline predictor set to reduce multicollinearity (baseline VIFs include _inf_ and very large values), and refit using _HC3 robust standard errors_ to reduce sensitivity of inference to heteroscedasticity (Breusch–Pagan rejects constant variance).
1. Adjusted model fit (after dropping _workingday_ and _atemp_, using HC3 SE): _R² = 0.8379_ and _Adjusted R² = 0.8313_. One diagnostic that improved is multicollinearity: the maximum VIF dropped from _inf_ in the baseline model to _96.47_ (rounded to 2 decimals) in the adjusted model.
1. In causal regression, you may accept a slightly worse fit if assumptions improve because the goal is _credible interpretation_ of coefficients (and valid standard errors), not just maximizing explained variance. Diagnostics that reduce collinearity, stabilize residual behavior, or justify robust inference can make effect estimates more defensible even if _R²_ changes little or declines slightly.

---

## 10.10 Assignment

Complete the assignment below:

### 10.10 MLR Diagnostics

- Diagnose regression assumption violations
- Test for normality (label and residuals)
- Detect and address multicollinearity using VIF
- Perform residual analysis using visualizations
- Apply transformations to improve model assumptions
- Refine models based on diagnostic results

- Use **all available data** for diagnostic evaluation
- The goal is **explanatory/causal modeling** – ensuring assumptions hold for reliable coefficient interpretation

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
