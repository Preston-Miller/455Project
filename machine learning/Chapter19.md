# Chapter 19: Forecast Modeling

## Learning Objectives

- Students will be able to build forecasting models using Excel trendlines, moving averages, and the ETS-based Forecast Sheet feature
- Students will be able to assess and ensure time-series stationarity using the Augmented Dickey-Fuller test and differencing techniques
- Students will be able to detect and interpret autocorrelation in time-series data using lag analysis
- Students will be able to implement moving average models in Python and evaluate forecast accuracy using MAE and RMSE

---

## 19.1 Introduction

![Introduction](../Images/Chapter19_images/forecasting.png)

You may recall that Multiple Linear Regression (MLR) relies on several key assumptions, one of which is the absence of autocorrelation. Autocorrelation occurs when observations are not independent of one another, meaning the value in one row depends on values from previous rows. This assumption is routinely violated in time-ordered data, which creates a problem for traditional Ordinary Least Squares (OLS) regression. Forecasting, by definition, depends on patterns across time, so we must explicitly account for this dependency rather than ignore it.

The core distinction between standard predictive modeling and forecasting is how the data are structured. In forecasting problems, each row represents a consecutive point in time, such as a year, month, day, hour, or even a second. These temporal dependencies are not a nuisance to be removed; they are the signal we want to model. Forecasting methods are therefore designed to relax the independence assumption and directly leverage relationships between past and future values.

Images in this section are either used with creative license (like the one above) or were created using DALL·E from OpenAI.

---

## 19.2 Excel Forecasting

Microsoft Excel remains a widely used tool for exploratory analysis and lightweight forecasting, especially in business settings where speed and accessibility matter. Excel allows analysts to quickly generate forecasts for outcomes such as sales, inventory demand, or consumer activity using historical data. At a basic level, forecasting in Excel can begin by visualizing time-ordered data and applying trendlines such as linear, exponential, or moving average models.

Let’s begin with a simple example using the TV Sales.csv dataset from Kaggle. After creating a time series chart of weekly sales, you can add a linear trendline to establish a baseline forecast. Excel reports the coefficient of determination (R²), which measures how much variation in the outcome is explained by the model and ranges from 0 to 1.

An R² value of 0 indicates that the model explains none of the variability in the outcome, while an R² value of 1 indicates a perfect fit.

In this example, an R² value of approximately 0.70 suggests that the linear trend explains about 70 percent of the variation in weekly sales. While this represents a reasonably strong fit, it is important to remember that a high R² does not guarantee strong forecasting performance, particularly when temporal patterns such as seasonality or autocorrelation are present.

![TV Sales Linear Trend](../Images/Chapter19_images/Sales Linear Trend Forecast.png)

Another common Excel-based approach is a simple moving average forecast. In this example, a two-period moving average closely follows the historical sales pattern, creating a smoothed version of the time series.

![TV Sales Two Period Moving Average](../Images/Chapter19_images/Sales Moving Average_2p.png)

Despite its simplicity, a two-period moving average has several important limitations that should be considered before relying on it for forecasting.

1. High sensitivity to outliers, since each new observation has a large influence on the forecast.
1. Limited responsiveness to structural changes, as the model smooths variation rather than adapting to shifts.
1. Minimal use of historical information, which prevents the model from capturing long-term trends.
1. Inability to model seasonality or recurring cyclical patterns.
1. A constant forecast value until new data are added.
1. Equal weighting of recent periods, even when older observations may be more representative.
1. An implicit assumption of stationarity that often does not hold in real-world business data.

Increasing the moving average window from two periods to four further smooths the series, but in this example it reduces forecast accuracy by lagging behind changes in sales.

![TV Sales Four Period Moving Average](../Images/Chapter19_images/Sales Moving Average_4p.png)

Excel also provides a more advanced option through the Forecast Sheet feature, which automatically generates forecasts using built-in time-series modeling techniques.

The Forecast Sheet feature is especially useful for data with seasonal patterns, where fluctuations repeat at regular intervals such as months, quarters, or years.

Behind the scenes, Excel’s Forecast Sheet relies on an exponential triple smoothing method known as ETS, which models error, trend, and seasonality components of a time series.

The error component captures random variation not explained by trend or seasonality.

The trend component represents long-term directional movement in the data.

The seasonality component models recurring patterns that repeat at fixed intervals.

When Excel detects no seasonality, the smoothing parameters associated with seasonality are set to zero, indicating that a simpler model was selected.

Forecast accuracy should always be evaluated using appropriate error metrics. Common measures include mean absolute error (MAE) and root mean squared error (RMSE), both of which express forecast error in the same units as the original data and allow for meaningful comparison across models.

While Excel forecasting tools are useful for rapid analysis and communication, more flexible and statistically rigorous forecasting methods such as ARIMA and SARIMA models are better suited for complex or high-stakes applications. These methods, along with information criteria such as AIC and BIC for model selection, will be introduced in subsequent Python-based sections.

Forecasting Videos

---

## 19.3 Python Forecasting

In this section, we will use two publicly available datasets from the Kaggle Datasets website to introduce forecasting workflows in Python. The first dataset, TV Sales Forecasting, is intentionally simple and already partially aggregated. It contains one row per product model per date, with a count indicating how many units of that model were sold on that day. In practice, this type of dataset is often derived from transaction-level data, where individual customer receipts are aggregated by product and date. The second dataset, Retail Analysis with Walmart Sales Data, is more realistic and complex. It includes weekly sales along with additional economic and contextual variables, which we will later use with more advanced forecasting models.

We will begin with the Walmart sales data:

```python
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)  # Use this if you want to suppress scientific notation

# TV sales data
df = pd.read_csv('walmart_sales.csv')
df.sort_values(by=['Date'], inplace=True)
print(df.dtypes)
df.head()

# Output:
# Store             int64
# Date             object
# Weekly_Sales    float64
# Holiday_Flag      int64
# Temperature     float64
# Fuel_Price      float64
# CPI             float64
# Unemployment    float64
# dtype: object
```

![5 records. Columns for Store, Date, Weekly_Sales, Holiday_Flag, Temperature, Fuel_Price, CPI, and Unemployment.](../Images/Chapter19_images/df_walmart.png)

After importing the data, we performed two important preparation steps. First, we sorted the records by date to ensure the observations are in chronological order, which is essential for any time-series analysis. Second, we examined the data types and observed that the Date column was stored as a generic object. Converting this column to a true datetime type gives us access to specialized time-series functionality in Pandas, including resampling, grouping, and lag-based operations.

```python
df.Date = pd.to_datetime(df.Date, format="%d-%m-%Y")
df.dtypes

# Output:
# Store                    int64
# Date            datetime64[ns]
# Weekly_Sales           float64
# Holiday_Flag             int64
# Temperature            float64
# Fuel_Price             float64
# CPI                    float64
# Unemployment           float64
# dtype: object
```

With the Date column properly cast as a datetime, we can now aggregate the data so that each row represents a single time period. Because this dataset contains observations for many stores on the same dates, we first aggregate across stores. We then further aggregate from daily observations to monthly observations, which simplifies the forecasting problem and reduces noise. By using a datetime index and the Pandas Grouper utility, we can dynamically group records by month and apply appropriate aggregation functions to each variable.

```python
# Ensure Date is datetime
df['Date'] = pd.to_datetime(df['Date'])

# Set Date as the DatetimeIndex
df = df.set_index('Date')

# Aggregate to monthly frequency (Month End)
df_agg = df.groupby(pd.Grouper(freq='ME')).agg({
  'Weekly_Sales': 'sum',
  'Holiday_Flag': 'mean',
  'Temperature': 'mean',
  'Fuel_Price': 'mean',
  'CPI': 'mean',
  'Unemployment': 'mean'
})

df_agg.tail()
```

Before fitting any forecasting model, it is critical to visualize the time series. Visualization allows us to quickly assess trends, structural changes, volatility, and potential seasonality. In this case, we will begin with a bar chart to highlight changes in monthly sales over time.

```python
import seaborn as sns
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
fig.set_size_inches(20, 6)
sns.barplot(x=df_agg.index, y="Weekly_Sales", data=df_agg, ax=ax)
plt.xticks(rotation=90)
plt.show()
```

This visualization reveals that the series does not follow a simple linear trend. There is a pronounced early spike followed by periods of fluctuation and decline. Recognizing these patterns early helps guide model selection and alerts us that more sophisticated approaches may be required. In later sections, we will apply multiple forecasting algorithms to this same dataset and compare their performance.

At this point, it is worth pausing to explain why classical regression techniques, such as ordinary least squares, are generally inappropriate for time-series forecasting. Traditional regression assumes that observations are independent of one another, meaning the value in one row does not depend on values in previous rows. Time-series data violates this assumption by design: past values directly influence future values. This temporal dependence introduces autocorrelation, which can bias coefficient estimates, understate uncertainty, and lead to overly confident predictions. Forecasting models address this limitation by explicitly modeling time dependence through lags, trends, and seasonality, which we will begin exploring in the next sections.

If you prefer a continuous representation of the time series, Pandas provides a convenient built-in plotting method that quickly generates a line chart:

```python
df_agg.Weekly_Sales.plot(figsize=(15,5));
```

![Line plot showing an early spike followed by moderate volatility and a downward trend near the end of the series.](../Images/Chapter19_images/walmart_plot_2.png)

---

## 19.4 Stationarity and Lags

Now that the data are structured appropriately for forecasting, the next step is to evaluate whether the series is stationary and, if not, how to transform it. **Stationarity** — The assumption that the statistical properties of a time series, such as its mean and variance, remain constant over time. is a foundational concept in time-series analysis because many forecasting models assume that the underlying data-generating process does not change across time. Some algorithms require strict stationarity, while others are designed to work with weaker or transformed forms of stationarity.

To formally assess stationarity, we commonly use the Augmented Dickey–Fuller test. This test evaluates whether a unit root is present in the series, which would indicate non-stationarity. The test produces a p-value, and values below 0.05 suggest that the null hypothesis of a unit root can be rejected, providing evidence that the series is sufficiently stationary for models that rely on this assumption. The statsmodels library provides a convenient implementation of this test.

```python
from statsmodels.tsa.stattools import adfuller

stationarity = adfuller(df_agg['Weekly_Sales'], autolag='AIC')
print("P-value: ", stationarity[1])

# This p-value indicates that we do not have stationality.
# Therefore, we need an algorithm more advanced than a basic
# autoregression formula.

# Output:
# P-value:  7.044293727364576e-08
```

Although the Augmented Dickey–Fuller test yields a p-value well below 0.05, suggesting statistical stationarity, this result should be interpreted cautiously in applied forecasting contexts. Real-world time series—especially aggregated business data like sales—often exhibit trends, structural changes, or seasonal effects that are not fully captured by a single statistical test. For instructional purposes, we will proceed as if stationarity is weak and intentionally explore models that assume or enforce stationarity. This approach allows us to clearly demonstrate how differencing, lag structure, and seasonal components improve predictive performance as we progress through increasingly sophisticated forecasting algorithms. With this context in mind, the next step is to examine lag relationships in the data.

Students often assume that a single statistical test definitively determines whether a time series is suitable for a given model. In practice, stationarity exists on a spectrum, and applied forecasting frequently proceeds even when tests suggest stationarity or non-stationarity ambiguously. In this chapter, we intentionally continue with models that assume or enforce stationarity so students can observe how differencing, lag structure, and seasonal terms affect forecast quality across increasingly sophisticated algorithms.

For example, hourly temperature data often exhibit strong autocorrelation at a lag of 24 periods, reflecting daily cycles, and at a lag of 365 periods, reflecting yearly seasonality. Identifying such lag structures allows models to explicitly capture repeating patterns. Importantly, reliable lag detection requires sufficient data coverage across the relevant cycles; without enough observations, seasonal dependencies cannot be estimated accurately.

### Fixing Non-Stationary Data

When a series is non-stationary, one of the most common and effective transformations is differencing. Differencing removes trends by subtracting previous values from current values, thereby stabilizing the mean of the series. To illustrate this process clearly, we will temporarily switch from the Walmart dataset to the TV sales data, which exhibits strong non-stationarity.

```python
df_tv = pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/tv_sales.csv')
df_tv.sort_values(by=['Date'], inplace=True)
df_tv.head()
```

![Fixing Non-Stationary Data](../Images/Chapter19_images/tv_sales_df.png)

This dataset contains only three variables: date, product model, and the count of televisions sold. After formatting the date column and aggregating to monthly values, we again evaluate stationarity using the Augmented Dickey–Fuller test.

```python
df_tv['Date'] = pd.to_datetime(df_tv['Date'], format="%d-%b-%y")

# Use a DatetimeIndex (not PeriodIndex) for grouping/resampling
df_tv = df_tv.set_index('Date')

# Monthly aggregation (Month End)
df_tv = df_tv.groupby(pd.Grouper(freq="ME")).agg({'Count': 'count'})

stationarity = adfuller(df_tv['Count'], autolag='AIC')
print("P-value: ", stationarity[1])

# Output
# P-value: 0.9317731864300911
```

This result confirms strong non-stationarity. We now create first-order and second-order differences to evaluate which transformation best stabilizes the series.

```python
df_tv['Count_diff_1'] = df_tv['Count'].diff(periods=1)
df_tv['Count_diff_2'] = df_tv['Count'].diff(periods=2)
df_tv.head()
```

![Fixing Non-Stationary Data](../Images/Chapter19_images/df_tv_diff.png)

Differencing subtracts values from earlier periods to remove systematic trends. First-order differencing removes linear trends, while second-order differencing can address more complex curvature. Visualizing the transformed series helps confirm whether the variance and mean have stabilized.

```python
df_tv.plot(figsize=(15,5));
```

![Fixing Non-Stationary Data](../Images/Chapter19_images/tv_diff_plot.png)

We can confirm the effectiveness of differencing by re-running the Augmented Dickey–Fuller test on the transformed series.

```python
stationarity = adfuller(df_tv['Count_diff_1'].dropna(), autolag='AIC')
print("P-value of first-order diff:\t", stationarity[1])

stationarity = adfuller(df_tv['Count_diff_2'].dropna(), autolag='AIC')
print("P-value of second-order diff:\t", stationarity[1])

# Output:
# P-value of first-order diff:	 8.970106449403325e-09
# P-value of second-order diff:	 0.09344020998849384
```

The first-order difference achieves strong stationarity and will be used as the response variable for subsequent forecasting models. We now return to the Walmart data to identify appropriate lag structures.

### Autocorrelation and Partial Autocorrelation Plots

Once stationarity has been established, autocorrelation and partial autocorrelation plots help determine which model family and lag structure are most appropriate. Autocorrelation plots reveal how strongly observations are related to past values, while partial autocorrelation isolates the direct effect of each lag.

```python
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
acf = plot_acf(df_agg['Weekly_Sales'], lags=16)
```

![Autocorrelation and Partial Autocorrelation Plots](../Images/Chapter19_images/walmart_autocorrelation.png)

Autocorrelation values indicate how each period relates to earlier periods. Values extending beyond the confidence bounds suggest statistically meaningful dependence. However, autocorrelation includes indirect effects transmitted through intermediate lags.

```python
pacf = plot_pacf(df_agg['Weekly_Sales'], lags=16, method='ywm')
```

![Autocorrelation and Partial Autocorrelation Plots](../Images/Chapter19_images/walmart_pautocorrelation.png)

Partial autocorrelation removes indirect effects, making it easier to identify the number of autoregressive terms needed. In this case, neither plot shows strong lag dependence, suggesting that only minimal lag structure is required.

Based on these diagnostics, an autoregressive model with minimal lag order is a reasonable starting point. We will now evaluate this assumption by fitting several forecasting models and comparing their performance.

---

## 19.5 Moving Average (MA)

A moving average (MA) model is a classical forecasting approach that predicts future values using past forecast errors (also called residual “shocks”). This is different from the “moving average” you may have built in Excel, where you simply average recent observations; an MA model averages recent error terms to capture short-run randomness in a time series.

To see how an MA model works, consider a simple example. Suppose the true mean of a series is 100. At time t−1, the observed value is 110, so the error (actual minus mean) is +10. An MA(1) model predicts the next value using the mean plus a weighted version of the previous error. If the MA coefficient is 0.5, the next forecast is 100 + (0.5 × 10) = 105. If no new shock occurs, future forecasts return toward the mean because past errors no longer contribute.

MA models are designed for single-variable (univariate) time-series forecasting, meaning we forecast a single outcome (y) using its own history rather than separate predictor variables. In this chapter, we will focus on univariate forecasting first and later expand to models that can incorporate additional variables.

In Python, we can fit an MA model using statsmodels through the ARIMA framework. An MA model of order 1 is specified as ARIMA(0, 0, 1), meaning no autoregressive terms, no differencing, and one moving-average term.

```python
from statsmodels.tsa.arima.model import ARIMA

# Fit an MA(1) model using the ARIMA interface: order=(p, d, q) = (0, 0, 1)
model = ARIMA(df_agg.Weekly_Sales, order=(0, 0, 1))
model = model.fit()
print(model.summary())

# Output
#                                SARIMAX Results
# ==============================================================================
# Dep. Variable:           Weekly_Sales   No. Observations:                   33
# Model:                 ARIMA(0, 0, 1)   Log Likelihood                -610.789
# Date:                Mon, 22 Dec 2025   AIC                           1227.577
# Time:                        20:41:14   BIC                           1232.067
# Sample:                    02-28-2010   HQIC                          1229.088
#                          - 10-31-2012
# Covariance Type:                  opg
# ==============================================================================
#                  coef    std err          z      P>|z|      [0.025      0.975]
# ------------------------------------------------------------------------------
# const       2.042e+08      2e+06    102.200      0.000       2e+08    2.08e+08
# ma.L1         -0.6463      0.235     -2.755      0.006      -1.106      -0.187
# sigma2      7.755e+14      0.017   4.55e+16      0.000    7.75e+14    7.75e+14
# ===================================================================================
# Ljung-Box (L1) (Q):                   0.16   Jarque-Bera (JB):                23.83
# Prob(Q):                              0.69   Prob(JB):                         0.00
# Heteroskedasticity (H):               1.10   Skew:                             1.57
# Prob(H) (two-sided):                  0.88   Kurtosis:                         5.73
# ===================================================================================
#
# Warnings:
# [1] Covariance matrix calculated using the outer product of gradients (complex-step).
# [2] Covariance matrix is singular or near-singular, with condition number 1.39e+44. Standard errors may be unstable.
```

The summary output is similar in style to the regression summaries you saw earlier, but the fit metrics are different. Instead of R2, you will typically compare time-series models using information criteria such as Akaike information criterion (AIC), Bayesian information criterion (BIC; also called Schwarz information criterion), and Hannan–Quinn information criterion (HQIC). These are error-penalized fit measures, so lower values generally indicate a better balance between fit and simplicity. We will use these metrics throughout the chapter to compare models consistently.

Now that we have a trained model, we can generate forecasts for future periods.

```python
# Generate multi-step forecasts
forecast_ma = model.predict(start=len(df_agg), end=len(df_agg) + 6)
print(forecast_ma)

# Output:
# 2012-11-30   217763943.537
# 2012-12-31   204158151.125
# 2013-01-31   204158151.125
# 2013-02-28   204158151.125
# 2013-03-31   204158151.125
# 2013-04-30   204158151.125
# 2013-05-31   204158151.125
# Freq: ME, Name: predicted_mean, dtype: float64
```

With an MA(1) model, the forecast can adjust one step ahead using the most recent error term, but beyond that it quickly converges toward a stable level (often close to the long-run mean of the process). In this example, the moving-average coefficient is statistically significant, indicating that short-run shocks do influence the next period. However, because MA terms do not propagate forward indefinitely, later forecasts still flatten. In practice, MA models are most useful for very short-horizon forecasting and as building blocks for more flexible models such as ARMA and ARIMA.

Finally, we will store the model’s fit metrics in a DataFrame so we can compare algorithms side-by-side as we move through the chapter.

```python
df_fit = pd.DataFrame(columns=['AIC', 'BIC', 'Model'])
df_fit.set_index('Model', inplace=True)

df_fit.loc["Moving Average"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit
```

![Model is Moving Average, AIC is 1227.580, BIC is 1232.070.](../Images/Chapter19_images/df_fit_1.png)

---

## 19.6 Autoregressive (AR)

Autoregressive (AR) models forecast future values as a linear function of past observations from the same time series, conceptually similar to how multiple linear regression models a label as a function of predictor variables. In an AR(p) model, the parameter p specifies how many prior time steps (lags) are included in the forecast. For example, AR(1) uses only the immediately preceding observation. AR models are most effective for univariate time series that are stationary or approximately stationary; however, as shown in this chapter, they can still provide useful baseline forecasts even when strict stationarity assumptions are only partially met.

To see how an AR model works, consider a simple example. Suppose the last observed value of a series is 120 and the long-run mean is 100. In an AR(1) model with a coefficient of 0.6, the next forecast is computed as 100 + 0.6 × (120 − 100) = 112. Unlike a moving average model, the forecast depends directly on the previous observed value rather than past errors. If future observations remain above the mean, the model continues to propagate that momentum forward.

```python
from statsmodels.tsa.ar_model import AutoReg

# fit model; trend can be 'n' none, 'c' constant only, 't' trend only, or 'ct' constant and trend
model = AutoReg(df_agg.Weekly_Sales, lags=[1], trend='ct', seasonal=False)
model = model.fit()

# make predictions
forecast_autoreg = model.predict(len(df_agg), len(df_agg) + 6)
print(forecast_autoreg, '\n')

# fit statistics to DataFrame
df_fit.loc["AutoReg"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output
# 2013-01   166971773.126
# 2013-02   164720134.700
# 2013-03   163328811.856
# 2013-04   161931476.229
# 2013-05   160534182.626
# 2013-06   159136888.729
# 2013-07   157739594.834
# Freq: M, dtype: float64

#     	                 AIC	    BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
```

In this example, we selected a lag of 1 based on the signal observed at lag 1 in the partial autocorrelation plot. We also included both a constant and a trend term to capture the underlying level and direction of the series. As anticipated from the autocorrelation diagnostics, the AR model outperforms the MA model, as indicated by lower AIC and BIC values. This improvement suggests that recent past values contain meaningful predictive information. Next, we will combine autoregressive and moving-average components into a single model.

---

## 19.7 Autoregressive Moving Average (ARMA)

The autoregressive moving average (ARMA) algorithm models the next period (forecast) as a linear function of both prior observations and prior forecast errors. By combining autoregressive (AR) and moving average (MA) components, ARMA can capture momentum from past values as well as short-run shocks from unexpected changes. ARMA models are best suited for univariate time series that are stationary or approximately stationary and do not exhibit strong trend or seasonality.

To see how ARMA works, suppose the long-run mean of a series is 100, the most recent observed value is 120, and the most recent forecast error is −10. In an ARMA(1,1) model with an AR coefficient of 0.6 and an MA coefficient of 0.4, the next forecast is computed as 100 + 0.6 × (120 − 100) + 0.4 × (−10) = 108. This shows how ARMA blends persistence from past values with correction for recent forecast errors.

```python
model = ARIMA(df_agg.Weekly_Sales, order=(1, 0, 1))
model = model.fit()

# make prediction for the last period and the next n
forecast_arma = model.predict(len(df_agg), len(df_agg) + 6)
print(forecast_arma, '\n')

df_fit.loc["ARMA"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   212308935.330
# 2012-12-31   205922527.612
# 2013-01-31   204540080.557
# 2013-02-28   204240826.275
# 2013-03-31   204176047.573
# 2013-04-30   204162025.117
# 2013-05-31   204158989.716
# Freq: ME, Name: predicted_mean, dtype: float64

#                       AIC     BIC
# Model
# Moving Average   1227.580  1232.070
# AutoReg          1197.020  1202.880
# ARMA             1229.010  1234.990
```

In this case, adding an MA term increases AIC and BIC slightly relative to the AR model, suggesting that the additional complexity may not be justified for this dataset. However, ARMA models often outperform pure AR or MA models when both persistence and short-term shocks are present. Next, we will improve these models further by explicitly handling non-stationarity through differencing.

---

## 19.8 Autoregressive Integrated Moving Average (ARIMA)

The autoregressive integrated moving average (ARIMA) algorithm models forecasted values as a linear function of prior values and prior forecast errors after the series has been differenced to improve stationarity. Like ARMA, ARIMA combines an autoregressive (AR) component and a moving average (MA) component, but it adds an “integration” step (the I) that differences the series d times to reduce trend-like non-stationarity. In statsmodels, you specify these three settings with the _order_ parameter: (p, d, q), where p is the number of AR lags, d is the differencing order, and q is the number of MA error lags; the model below uses (1, 1, 1).

ARIMA is best used for univariate time series that show trend (non-stationarity in level) but do not require an explicit seasonal component.

```python
model = ARIMA(df_agg.Weekly_Sales, order=(1, 1, 1))
model = model.fit()

# make prediction for the last period and the next n
forecast_arima = model.predict(len(df_agg), len(df_agg) + 6)
print(forecast_arima, '\n')

df_fit.loc["ARIMA"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   182813651.613
# 2012-12-31   183458513.163
# 2013-01-31   183189883.524
# 2013-02-28   183301786.432
# 2013-03-31   183255171.096
# 2013-04-30   183274589.622
# 2013-05-31   183266500.457
# Freq: ME, Name: predicted_mean, dtype: float64

#   	                AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
# ARMA	        1229.010	1234.990
# ARIMA	        1220.140	1223.070
```

Notice that differencing improves fit here (lower AIC and BIC) relative to the non-differenced ARMA model, which is consistent with the idea that the original series contains trend-like non-stationarity. In these results, ARIMA also improves on the MA-only and ARMA models, but it still does not outperform the simpler AR model, suggesting that the MA term may add complexity without much benefit for this particular dataset. Next, we will extend ARIMA to explicitly model repeating seasonal patterns, which is often essential for real sales data.

---

## 19.9 Seasonal Autoregressive Integrated Moving Average (SARIMA)

Conceptually, a SARIMA model extends ARIMA by adding a second set of autoregressive, differencing, and moving average terms that operate over a seasonal cycle. In mathematical terms, the forecast at time t depends not only on recent values and recent forecast errors, but also on values and errors from the same season in prior cycles (for example, the same month last year). This allows the model to capture repeating seasonal patterns while still accounting for trend and short-term dynamics.

The seasonal autoregressive integrated moving average (SARIMA) method models the forecast as a linear function of (1) differenced observations, (2) prior forecast errors, (3) differenced seasonal observations, and (4) seasonal forecast errors. In practice, this means specifying two sets of parameters: _order_=(p, d, q) for the non-seasonal component and _seasonal_order_=(P, D, Q, s) for the seasonal component, where s is the length of the seasonal cycle (for example, 12 for yearly seasonality in monthly data).

SARIMA is best used for univariate time series that exhibit both trend and repeating seasonal patterns.

```python
# SARIMA example
from statsmodels.tsa.statespace.sarimax import SARIMAX

model = SARIMAX(df_agg.Weekly_Sales, order=(1, 1, 1), seasonal_order=(0, 1, 1, 12))
model = model.fit(disp=False)

# make prediction for the last period and the next n
forecast_sarima = model.predict(len(df_agg), len(df_agg) + 6)
print(forecast_sarima, '\n')

df_fit.loc["SARIMA"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   189005952.801
# 2012-12-31   270177321.116
# 2013-01-31   154422271.320
# 2013-02-28   187269575.389
# 2013-03-31   213936983.247
# 2013-04-30   194007136.640
# 2013-05-31   182929042.969
# Freq: ME, Name: predicted_mean, dtype: float64
#
# /usr/local/lib/python3.12/dist-packages/statsmodels/tsa/statespace/sarimax.py:866: UserWarning: Too few observations to estimate starting parameters for seasonal ARMA. All parameters except for variances will be set to zeros.
#  warn('Too few observations to estimate starting parameters%s.'
#                     AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
# ARMA	        1229.010	1234.990
# ARIMA	        1220.140	1223.070
# SARIMA	         761.100	 764.090
```

Adding seasonal terms dramatically improves model fit here (much lower AIC and BIC), which is consistent with the idea that retail sales often follow repeating annual cycles. However, notice the warning about having too few observations to estimate starting parameters for the seasonal ARMA portion of the model; with short time series, seasonal parameters can be harder to estimate reliably, so you should treat the exact seasonal settings as more tentative and validate them using out-of-sample forecasting when possible. In this example, the seasonal parameters were selected by experimenting with small values and choosing the combination that minimized AIC and BIC, but with experience you can often narrow these choices by examining autocorrelation and partial autocorrelation patterns at seasonal lags.

Notice that we were given a warning that there are too few observations to estimate starting parameters for seasonal ARMA. With short monthly series (e.g., only ~36 observations), seasonal ARMA terms can be unstable; a seasonal difference-only SARIMA often works better unless you have multiple years of data. One way to adjust for this limitation is to use a simpler seasonal structure as in this example below:

```python
from statsmodels.tsa.statespace.sarimax import SARIMAX

model = SARIMAX(
  df_agg['Weekly_Sales'],
  order=(1, 1, 0),
  seasonal_order=(0, 1, 0, 12),  # simpler seasonal structure
  enforce_stationarity=False,
  enforce_invertibility=False
)
model = model.fit(disp=False)

forecast_sarima = model.predict(start=len(df_agg), end=len(df_agg) + 6)
print(forecast_sarima, '\n')

df_fit.loc["SARIMA"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   179653475.532
# 2012-12-31   281760111.369
# 2013-01-31   144062957.431
# 2013-02-28   181400645.440
# 2013-03-31   210003363.583
# 2013-04-30   175713142.891
# 2013-05-31   169207775.009
# Freq: ME, Name: predicted_mean, dtype: float64

#                       AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
# ARMA	            1229.010	1234.990
# ARIMA	            1220.140	1223.070
# SARIMA	         716.770	 718.660
```

The models we have examined so far differ primarily in how they use past values, past errors, differencing, and seasonal structure to generate forecasts. The summary table below consolidates these differences to help you compare when each model is most appropriate, what information it relies on, and how its parameters should be interpreted in practice.

At this point, we have modeled trend and seasonality using only the history of Weekly*Sales itself. However, this dataset also contains related variables such as Holiday_Flag, Temperature, Fuel_Price, CPI, and Unemployment. These \_covariates* may carry additional predictive signal, and incorporating them requires a further extension of SARIMA, which we will explore next.

---

## 19.10 Seasonal Autoregressive Integrated Moving Average with Exogenous Regressors (SARIMAX)

Conceptually, SARIMAX extends SARIMA by allowing the forecast at time t to depend not only on past values, past errors, and seasonal patterns, but also on additional time-aligned input variables. In simple terms, SARIMAX answers the question: “How would the forecast change if we also knew something else about the environment at each time period?” These additional inputs shift the level of the forecast rather than creating their own autoregressive or moving-average structure.

The seasonal autoregressive integrated moving average with exogenous regressors (SARIMAX) is an extension of the SARIMA model that includes the effect of _exogenous_ variables—external inputs that vary over time and may help explain changes in the target series. The primary series being forecasted (Weekly*Sales) is called the \_endogenous* variable. In the Walmart dataset, exogenous variables include Holiday_Flag, Temperature, Fuel_Price, CPI, and Unemployment. These variables may influence purchasing behavior, but they are not modeled as AR or MA processes themselves.

SARIMAX is flexible enough to represent AR, MA, ARMA, ARIMA, and SARIMA models with the addition of covariates, making it appropriate for univariate time series with trend or seasonality when relevant external drivers are available.

```python
df_exog = df_agg[['Holiday_Flag', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']]

model = SARIMAX(df_agg.Weekly_Sales, exog=df_exog, order=(1, 1, 0), seasonal_order=(0, 1, 1, 12))
model = model.fit()

# make prediction for the last period and the next n
exog_future = [
                [0.200, 46.606, 3.439, 174.510, 7.550],
                [0.000, 43.605, 3.626, 174.790, 7.508],
                [0.000, 59.256, 3.678, 175.141, 7.441],
                [0.000, 60.927, 3.955, 175.499, 7.437],
                [0.000, 68.469, 3.823, 175.916, 7.264],
                [0.000, 66.201, 3.592, 175.274, 7.401],
                [0.200, 79.190, 3.585, 175.733, 7.274]
              ]
forecast_sarimax = model.predict(len(df_agg), len(df_agg) + 6, exog=exog_future)
print(forecast_sarimax, '\n')

df_fit.loc["SARIMAX"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# /usr/local/lib/python3.12/dist-packages/statsmodels/tsa/statespace/sarimax.py:866: UserWarning: Too few observations to estimate starting parameters for seasonal ARMA. All parameters except for variances will be set to zeros.
#  warn('Too few observations to estimate starting parameters%s.'
# 2012-11-30   314131523.564
# 2012-12-31   542699079.704
# 2013-01-31   252343079.214
# 2013-02-28   536322931.319
# 2013-03-31   295739205.668
# 2013-04-30   268177822.646
# 2013-05-31    48745652.948
# Freq: ME, Name: predicted_mean, dtype: float64
#
#                     AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
# ARMA	        1229.010	1234.990
# ARIMA	        1220.140	1223.070
# SARIMA	         716.770	 718.660
# SARIMAX	         775.380	 783.350
```

Again, if you want to eliminate that warning, use a simpler seasonal structure:

```python
df_exog = df_agg[['Holiday_Flag', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']]

model = SARIMAX(df_agg.Weekly_Sales, exog=df_exog, order=(1, 1, 0), seasonal_order=(0, 1, 1, 12))
model = model.fit()

# make prediction for the last period and the next n
exog_future = [
                [0.200, 46.606, 3.439, 174.510, 7.550],
                [0.000, 43.605, 3.626, 174.790, 7.508],
                [0.000, 59.256, 3.678, 175.141, 7.441],
                [0.000, 60.927, 3.955, 175.499, 7.437],
                [0.000, 68.469, 3.823, 175.916, 7.264],
                [0.000, 66.201, 3.592, 175.274, 7.401],
                [0.200, 79.190, 3.585, 175.733, 7.274]
              ]
forecast_sarimax = model.predict(len(df_agg), len(df_agg) + 6, exog=exog_future)
print(forecast_sarimax, '\n')

df_fit.loc["SARIMAX"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   179653475.532
# 2012-12-31   281760111.369
# 2013-01-31   144062957.431
# 2013-02-28   181400645.440
# 2013-03-31   210003363.583
# 2013-04-30   175713142.891
# 2013-05-31   169207775.009
# Freq: ME, Name: predicted_mean, dtype: float64
#
#                     AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
# AutoReg	        1197.020	1202.880
# ARMA	        1229.010	1234.990
# ARIMA	        1220.140	1223.070
# SARIMA	         716.770	 718.660
# SARIMAX	         775.380	 783.350
```

Including all covariates slightly increased the error metrics, which does not necessarily indicate a worse model. Additional variables can reduce overfitting by forcing the model to explain variation more realistically. To better understand the contribution of individual covariates, we can fit reduced SARIMAX models using one exogenous variable at a time.

```python
# Try this model with each of the exogenous covariates: 'Holiday_Flag', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment'
df_exog = df_agg[['Temperature']]

model = SARIMAX(df_agg.Weekly_Sales, exog=df_exog, order=(1, 1, 0), seasonal_order=(0, 1, 0, 12))
model = model.fit()

# make prediction for the last period and the next n
exog_future = [
                [46.606],
                [43.605],
                [59.256],
                [60.927],
                [68.469],
                [66.201],
                [79.190]
              ]
forecast_sarimax_reduced = model.predict(len(df_agg), len(df_agg) + 6, exog=exog_future)
print(forecast_sarimax_reduced, '\n')

df_fit.loc["SARIMAX reduced"] = [round(model.aic, 2), round(model.bic, 2)]
df_fit

# Output:
# 2012-11-30   173609126.799
# 2012-12-31   291245287.662
# 2013-01-31   217400650.580
# 2013-02-28   248403796.611
# 2013-03-31   273331920.005
# 2013-04-30   199196194.770
# 2013-05-31   216944121.189
# Freq: ME, Name: predicted_mean, dtype: float64
#
#                     AIC	        BIC
# Model
# Moving Average	1227.580	1232.070
3 AutoReg	        1197.020	1202.880
# ARMA	        1229.010	1234.990
# ARIMA	        1220.140	1223.070
# SARIMA	         716.770	 718.660
# SARIMAX	         775.380	 783.350
# SARIMAX reduced	 757.480	 760.470
```

The reduced SARIMAX model improved fit metrics relative to the full model, suggesting that Temperature carries meaningful predictive signal. However, numerical metrics alone are not sufficient; the final step is to visually compare forecasts against observed values, which we will do next.

---

## 19.11 Visual Comparison of Forecasts

AIC and BIC are useful for comparing models, but they are still indirect. A practical next step is to run a small backtest: hold out the last few periods, fit each model on earlier periods, and then visually compare each forecast to the known actual values. This makes it easier to see whether a model captures level, seasonality timing, and amplitude. Also note that the best model for this dataset may not be the best model for a different dataset, because time series differ in trend strength, seasonality, noise, and the usefulness of covariates.

```python
# Visual comparison using a simple holdout backtest (no re-training on forecasted values)
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.statespace.sarimax import SARIMAX

# -----------------------------
# 1) Setup: target and holdout
# -----------------------------
y = df_agg['Weekly_Sales'].copy()

h = 6  # hold out the last h periods for a visual backtest
train_y = y.iloc[:-h]
test_y = y.iloc[-h:]

# Exogenous covariates for SARIMAX models (aligned by index)
exog_cols = ['Holiday_Flag', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']
X = df_agg[exog_cols].copy()
train_X = X.iloc[:-h]
test_X = X.iloc[-h:]

# Helper to store forecasts safely
def _safe_forecast(name, forecast_series, index_like):
  s = pd.Series(np.nan, index=index_like, name=name, dtype='float64')
  s.loc[forecast_series.index] = forecast_series.values
  return s

# -----------------------------
# 2) Fit each model on train set
# -----------------------------
forecasts = {}

# MA(1): ARIMA(0,0,1)
ma_model = ARIMA(train_y, order=(0, 0, 1)).fit()
ma_fc = ma_model.predict(start=test_y.index[0], end=test_y.index[-1])
forecasts['Moving Average'] = ma_fc

# AR: AutoReg with lag=1 (keep trend consistent with earlier chapter examples)
ar_model = AutoReg(train_y, lags=[1], trend='ct', seasonal=False).fit()
ar_fc = ar_model.predict(start=len(train_y), end=len(train_y) + h - 1)
ar_fc.index = test_y.index
forecasts['AutoReg'] = ar_fc

# ARMA(1,1): ARIMA(1,0,1)
arma_model = ARIMA(train_y, order=(1, 0, 1)).fit()
arma_fc = arma_model.predict(start=test_y.index[0], end=test_y.index[-1])
forecasts['ARMA'] = arma_fc

# ARIMA(1,1,1)
arima_model = ARIMA(train_y, order=(1, 1, 1)).fit()
arima_fc = arima_model.predict(start=test_y.index[0], end=test_y.index[-1])
forecasts['ARIMA'] = arima_fc

# SARIMA: SARIMAX with seasonal structure
sarima_model = SARIMAX(train_y, order=(1, 1, 1), seasonal_order=(0, 1, 1, 12)).fit(disp=False)
sarima_fc = sarima_model.predict(start=test_y.index[0], end=test_y.index[-1])
forecasts['SARIMA'] = sarima_fc

# SARIMAX: full exogenous set
sarimax_model = SARIMAX(train_y, exog=train_X, order=(1, 1, 0), seasonal_order=(0, 1, 1, 12)).fit(disp=False)
sarimax_fc = sarimax_model.predict(start=test_y.index[0], end=test_y.index[-1], exog=test_X)
forecasts['SARIMAX'] = sarimax_fc

# SARIMAX reduced: single exogenous variable (Temperature)
sarimaxr_model = SARIMAX(train_y, exog=train_X[['Temperature']], order=(1, 1, 0), seasonal_order=(0, 1, 1, 12)).fit(disp=False)
sarimaxr_fc = sarimaxr_model.predict(start=test_y.index[0], end=test_y.index[-1], exog=test_X[['Temperature']])
forecasts['SARIMAX reduced'] = sarimaxr_fc

# -----------------------------
# 3) Build plot DataFrame
# -----------------------------
df_forecast = pd.DataFrame(index=y.index)
df_forecast['Actual'] = y

# Store each forecast only on the holdout window (keep earlier periods as NaN for clarity)
for k, v in forecasts.items():
  v = v.copy()
  v.index = test_y.index  # ensure alignment
  df_forecast[k] = _safe_forecast(k, v, df_forecast.index)

# -----------------------------
# 4) Plot: full history + holdout forecasts
# -----------------------------
plt.figure(figsize=(15, 5))
plt.plot(df_forecast.index.astype(str), df_forecast['Actual'], label='Actual')

for col in ['Moving Average', 'AutoReg', 'ARMA', 'ARIMA', 'SARIMA', 'SARIMAX', 'SARIMAX reduced']:
  plt.plot(df_forecast.index.astype(str), df_forecast[col], label=col)

plt.xticks(rotation=90)
plt.title('Holdout Backtest: Forecasts vs Actual (last {} periods)'.format(h))
plt.legend()
plt.tight_layout()
plt.show()

# -----------------------------
# 5) Optional: quick numeric check for the holdout window
# -----------------------------
eval_rows = []
for col in ['Moving Average', 'AutoReg', 'ARMA', 'ARIMA', 'SARIMA', 'SARIMAX', 'SARIMAX reduced']:
  yhat = df_forecast.loc[test_y.index, col]
  err = (test_y - yhat).dropna()
  mae = np.mean(np.abs(err))
  rmse = np.sqrt(np.mean(err**2))
  eval_rows.append([col, float(mae), float(rmse)])

df_eval = pd.DataFrame(eval_rows, columns=['Model', 'MAE (holdout)', 'RMSE (holdout)']).sort_values(by='RMSE (holdout)')
df_eval
```

![Line chart of Actual values across the full timeline, with each model’s forecast shown only for the final holdout periods. Some models track the seasonal swings more closely than others.](../Images/Chapter19_images/fit_chart.png)

In the plot, focus on the holdout window at the end: a strong model stays close to the actual series, captures the seasonal timing of peaks and dips, and avoids systematic bias that stays consistently too high or too low. If several models look similar, prefer the simpler one unless the more complex model provides a clear improvement. Also remember that model performance is data-dependent: a method that works well here may perform poorly on a different series with different seasonality, noise, trends, or covariate relationships.

Several of the seasonal models in this comparison achieve very strong AIC and BIC values, yet their forecasts may look unintuitive—sometimes moving sharply up when the actual series moves down, or vice versa. This is not a bug or plotting error. It can happen when complex seasonal autoregressive structures are fit to a relatively short time series.

In these cases, the model may estimate negative autoregressive coefficients, strong seasonal differences, or other parameter combinations that mathematically improve fit but produce oscillating dynamics. This highlights an important forecasting lesson: optimization criteria reward statistical fit, not common sense. Always pair numerical metrics with visual inspection and domain knowledge before trusting a forecast.

Before moving on, take a moment to reflect on the patterns in the holdout forecasts:

- Which model stays closest to the actual values during the holdout window, and which deviates the most?
- Which forecasts appear overly smooth, and which appear overly volatile? What model components explain this behavior?
- Why might a model with a lower AIC or BIC still produce forecasts that seem unrealistic or unstable?
- If you were deploying one of these models in practice, which would you choose and why?

It is also worth noting that seasonal models can often be improved by simplifying their structure, especially when working with short time series. For example, a seasonal model that uses seasonal differencing only—without seasonal autoregressive or moving-average terms—frequently produces more stable forecasts when fewer than four or five full seasonal cycles are available.

In other words, removing seasonal autoregressive and moving-average terms does not mean ignoring seasonality. Instead, it limits how aggressively the model attempts to estimate seasonal dynamics that may not be well-supported by the data. This trade-off between flexibility and stability is a recurring theme in time-series modeling and one of the reasons simpler models often outperform more complex ones in real-world forecasting tasks.

---

## 19.12 Concepts Quiz

Answer the forecast concepts questions:

### 19.12 Forecasting: Concepts Quiz

As implied at the end of the prior section, we can likely improve the Walmart forecast by removing the outlier periods. Remove the first period and last two periods. Then, re-create the appropriate charts to help you determine the best number of lags (p), MA order (q), and differencing (d). Do not worry about scoring actual forecasted values for this practice (that’s part of the next practice). Simply find the best model fit possible across all algorithms. However, to keep this somewhat simpler, use only the Holiday_Flag as a covariate for the SARIMAX model.

When you are done, click the Colab icon in this box to see one potential solution.

Use the models you generated in the prior practice to forecast the next six months of sales. For the models that include a moving average component, store the prior month’s forecast to use in the next month’s forecast. Automate this process in a loop/iteration. Plot the forecasts together along with the original data to see which forecast looks most accurate to you.

When you are done, click the Colab icon in this box to see one potential solution.

---

## 19.13 Assignment

Complete the assignment below (if any):

---
