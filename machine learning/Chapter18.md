# Chapter 18: Monitoring and Managing ML Pipelines

## Learning Objectives

- Students will be able to distinguish between data drift, concept drift, and system failures as post-deployment failure modes
- Students will be able to implement performance monitoring that tracks model metrics across retraining cycles and visualizes trends over time
- Students will be able to detect data drift using statistical measures including Population Stability Index (PSI) and feature distribution comparisons
- Students will be able to design and implement champion/challenger strategies for safe model promotion
- Students will be able to build monitoring systems with alerting thresholds for detecting silent model degradation

---

## 18.1 Introduction

![A wide cinematic banner showing the lifecycle of a deployed ML model over time. On the left, a healthy model dashboard with green metrics and upward-trending accuracy charts. In the center, warning signs appear: amber alert icons, a line chart showing accuracy drifting downward, and a histogram of feature distributions shifting shape. On the right, a recovery scene with a retrained model being swapped in, metrics returning to green, and a log file recording the event. The style is flat, modern, and technical with a color palette transitioning from green through amber to red and back to green, emphasizing the monitoring and recovery cycle.](../Images/Chapter18_images/mon_banner.png)

#### From Deployment to Reliability

In the previous chapter, you built an end-to-end ML pipeline: extracting data from an operational database, training a model, saving it as a file, generating predictions on a schedule, and writing those predictions back into the database for the application to consume. That pipeline works. But will it _keep_ working?

A deployed model is not a finished product. It is a living system that interacts with changing data, changing business conditions, and changing infrastructure. Without monitoring, a model can silently degrade—producing predictions that look normal but have become unreliable. Silent failure is far more dangerous than a visible crash because no one investigates until the damage is already done.

This chapter covers the practices that keep deployed pipelines trustworthy over time. You will learn to track model performance across retraining runs, detect when the input data has shifted in ways that undermine your model, maintain a clear audit trail of every model version, and implement retraining strategies that respond to change rather than hoping nothing breaks.

#### What This Chapter Builds

By the end of this chapter, you will be able to:

- Log model performance metrics after every retraining run and visualize trends over time.
- Detect data drift by comparing feature distributions between training data and new operational data.
- Maintain a model registry that records every model version, its training context, and its evaluation results.
- Implement a champion/challenger retraining strategy that only promotes a new model when it outperforms the current one.
- Manage your pipeline as a system with consistent configuration, clear dependencies, and graceful handling of change.

#### Key Mental Model: Trust Requires Evidence

When you hand-run a model in a notebook, you can inspect every step. When a model runs automatically on a schedule, you lose that visibility. Monitoring restores it. The goal is not to prevent all failures—it is to detect problems quickly, understand their cause, and recover before bad predictions drive bad decisions.

The deployment chapter established a core principle: _your model is a file_. This chapter adds a second: _your trust in that model must be backed by evidence, and that evidence must be refreshed continuously._

#### How This Chapter Connects to Deployment

This chapter extends the pipeline you built in the previous chapter. The same project structure, the same _shop.db_ database, and the same Python scripts form the foundation. You will add monitoring, logging, drift detection, and smarter retraining on top of that existing pipeline rather than starting from scratch.

If the deployment chapter taught you to build the engine, this chapter teaches you to build the dashboard, the maintenance schedule, and the warning lights.

---

## 18.2 What Can Go Wrong After Deployment

A model that performed well during development can fail in production for three fundamentally different reasons. Understanding which category a problem falls into determines how you respond.

![A three-panel illustration showing the three failure modes of deployed ML models. Panel 1 (Data Drift): two overlapping bell curve distributions, one solid blue labeled Training Data and one dashed red labeled Production Data, with the red curve visibly shifted to the right, showing that the input distribution has changed. Panel 2 (Concept Drift): a scatter plot with two different decision boundaries drawn through the same data points, one solid line labeled Original Relationship and one dashed line labeled New Relationship, showing that the mapping from features to target has changed even though the data looks similar. Panel 3 (System Failure): a broken pipeline icon with error symbols, a disconnected database icon, and a red X over a file path, representing infrastructure failures like missing files, schema changes, or crashed processes. Each panel has a clear label and a one-line description underneath.](../Images/Chapter18_images/mon_failure_modes.png)

#### Data Drift

_Data drift_ occurs when the distribution of input features changes over time, even though the underlying relationship between features and the target may remain the same. The model was trained on data that looked one way, and now it is scoring data that looks different.

Examples in the shop.db context:

- A holiday promotional campaign causes a sudden spike in orders with promo codes, changing the distribution of _promo_used_ and _order_total_.
- The store adds a new product category that shifts the distribution of _avg_unit_price_ and _num_items_.
- A new payment gateway introduces a payment method the model has never seen, creating an unknown category in _payment_method_.

Data drift does not necessarily mean the model is wrong. It means the model is being asked to make predictions in territory it has not been trained on. The further the production data drifts from the training data, the less reliable the predictions become.

#### Concept Drift

_Concept drift_ occurs when the relationship between features and the target changes. The input data may look similar, but the patterns the model learned no longer hold.

Examples in the shop.db context:

- A shipping carrier changes its delivery policies, so the factors that used to predict late delivery no longer do.
- The warehouse hires more staff during the holiday season, reducing actual delivery times even for orders that the model flags as high risk.
- A change in fraud detection rules at the payment processor shifts which orders are genuinely fraudulent.

Concept drift is harder to detect than data drift because the inputs may look normal. The only signal is a gradual decline in model performance metrics, which is why tracking those metrics over time is essential.

#### System Failures

_System failures_ are infrastructure problems that prevent the pipeline from running correctly. These are not statistical issues—they are engineering issues.

- The operational database schema changes (a column is renamed or a table is restructured) and the ETL query breaks.
- A disk fills up and the model artifact cannot be saved.
- A Python package is updated and introduces a breaking change in the preprocessing pipeline.
- The scheduler (cron or Task Scheduler) is disabled after a system restart and the pipeline silently stops running.

System failures are often the most common source of pipeline problems. Good logging, clear error messages, and simple health checks catch most of these before they affect predictions.

#### The Common Thread

All three failure modes share one characteristic: _they can be invisible without monitoring_. The pipeline may continue running, the model file may continue loading, and the predictions table may continue receiving values. But the predictions themselves may have become unreliable. Monitoring is the practice of building visibility into this process so that problems are caught early and addressed deliberately.

---

## 18.3 Monitoring Model Performance

The simplest and most important form of monitoring is tracking how well the model performs each time it is retrained. The deployment chapter already saves a _metrics.json_ file after each training run. The problem is that each run overwrites the previous file, so you lose history. In this section, you will extend the pipeline to maintain a persistent performance log.

#### Building a Metrics Log Table

Instead of overwriting _metrics.json_, we append each training run’s metrics to a table in the warehouse database. This creates a time series of model performance that you can query and visualize.

```python
import sqlite3
import json
from datetime import datetime

def log_metrics(wh_db_path, metrics_dict, model_version, feature_list):
  """Append training metrics to the metrics_log table."""
  conn = sqlite3.connect(str(wh_db_path))
  cursor = conn.cursor()

  cursor.execute("""
  CREATE TABLE IF NOT EXISTS metrics_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trained_at TEXT NOT NULL,
    model_version TEXT NOT NULL,
    accuracy REAL,
    f1 REAL,
    roc_auc REAL,
    row_count_train INTEGER,
    row_count_test INTEGER,
    features TEXT
  )
  """)

  cursor.execute("""
  INSERT INTO metrics_log
  (trained_at, model_version, accuracy, f1, roc_auc,
   row_count_train, row_count_test, features)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  """, (
    datetime.utcnow().isoformat(),
    model_version,
    metrics_dict.get("accuracy"),
    metrics_dict.get("f1"),
    metrics_dict.get("roc_auc"),
    metrics_dict.get("row_count_train"),
    metrics_dict.get("row_count_test"),
    json.dumps(feature_list)
  ))

  conn.commit()
  conn.close()
```

This function can be called at the end of _train_model.py_ so that every training run automatically records its results.

#### Integrating with the Training Job

Add the logging call at the end of the _train_and_save()_ function from the deployment chapter:

```python
# At the end of train_and_save() in jobs/train_model.py:
from monitoring import log_metrics
from config import WH_DB_PATH

log_metrics(
  wh_db_path=WH_DB_PATH,
  metrics_dict=metrics,
  model_version=MODEL_VERSION,
  feature_list=NUMERIC_FEATURES + CATEGORICAL_FEATURES
)

print("Metrics logged to warehouse.")
```

#### Querying the Metrics Log

Once several training runs have been logged, you can query the history to see how performance has changed over time.

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("warehouse.db")

metrics_history = pd.read_sql("""
  SELECT trained_at, model_version, accuracy, f1, roc_auc,
         row_count_train
  FROM metrics_log
  ORDER BY trained_at
""", conn)

conn.close()

print(metrics_history.to_string(index=False))
```

#### Visualizing Performance Trends

A simple line chart of metrics over time is often the most effective monitoring tool. Sudden drops or gradual declines become immediately visible.

![A clean line chart with a white background showing model performance metrics over time. The x-axis shows training dates spanning several weeks. Three lines are plotted: Accuracy in blue, F1 Score in green, and ROC AUC in orange. For the first several data points, all three lines remain relatively stable around 0.75 to 0.80. Then around mid-chart, the lines begin to decline gradually, with F1 dropping the most noticeably. A horizontal dashed red line labeled Alert Threshold is drawn at 0.70. The F1 line crosses below this threshold at the most recent data point, with a red circle and exclamation mark highlighting the crossing. The chart title reads Model Performance Over Time and the overall style is minimal and suitable for a textbook.](../Images/Chapter18_images/mon_metrics_trend.png)

```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 5))

ax.plot(metrics_history["trained_at"], metrics_history["accuracy"],
        marker="o", label="Accuracy")
ax.plot(metrics_history["trained_at"], metrics_history["f1"],
        marker="s", label="F1 Score")
ax.plot(metrics_history["trained_at"], metrics_history["roc_auc"],
        marker="^", label="ROC AUC")

# Alert threshold
ax.axhline(y=0.70, color="red", linestyle="--",
           linewidth=1, label="Alert Threshold")

ax.set_xlabel("Training Date")
ax.set_ylabel("Metric Value")
ax.set_title("Model Performance Over Time")
ax.legend()
ax.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.show()
```

#### Setting Alert Thresholds

An alert threshold defines the minimum acceptable performance. When a metric drops below this line, the system should flag the issue for human review. The threshold depends on the business context—there is no universal number.

For the late delivery prediction pipeline, a reasonable approach is to set the threshold based on the initial model’s performance. If the first model achieved an F1 of 0.78, you might set the alert at 0.70—a 10% relative decline—as a signal that something has changed and the model needs attention.

```python
# Simple threshold check after training
ALERT_THRESHOLD_F1 = 0.70

if metrics["f1"] < ALERT_THRESHOLD_F1:
  print(f"WARNING: F1 score ({metrics['f1']:.3f}) is below "
        f"threshold ({ALERT_THRESHOLD_F1}). Investigate.")
else:
  print(f"Model performance is within acceptable range "
        f"(F1 = {metrics['f1']:.3f}).")
```

In production systems, this warning might send an email, create a ticket, or write to a monitoring dashboard. For this chapter, printing to the log is sufficient—the important concept is that the check happens automatically.

---

## 18.4 Detecting Data Drift

Performance monitoring tells you _that_ something has changed. Drift detection helps you understand _what_ changed. By comparing the distribution of input features between the training data and the current operational data, you can identify which variables have shifted and by how much.

#### The Basic Idea

Data drift detection compares two snapshots of data: the _reference_ distribution (typically the data the current model was trained on) and the _current_ distribution (the data the model is now scoring). If the current data looks significantly different from the reference, the model may be making predictions outside the range it was designed for.

#### Summary Statistics Comparison

The simplest approach is to compare basic summary statistics between the training data and recent operational data. This catches large shifts quickly.

```python
import sqlite3
import pandas as pd

# Load reference data (what the model was trained on)
wh_conn = sqlite3.connect("warehouse.db")
df_reference = pd.read_sql("SELECT * FROM fact_orders_ml", wh_conn)
wh_conn.close()

# Load current operational data (what the model is scoring now)
op_conn = sqlite3.connect("shop.db")
df_current = pd.read_sql("""
  SELECT o.order_total, o.shipping_fee, o.promo_used,
         o.payment_method, o.device_type
  FROM orders o
  WHERE o.order_datetime > date('now', '-7 days')
""", op_conn)
op_conn.close()

# Compare numeric feature statistics
numeric_cols = ["order_total", "shipping_fee", "promo_used"]

comparison = pd.DataFrame({
  "reference_mean": df_reference[numeric_cols].mean(),
  "current_mean": df_current[numeric_cols].mean(),
  "reference_std": df_reference[numeric_cols].std(),
  "current_std": df_current[numeric_cols].std()
})

comparison["mean_shift_pct"] = (
  (comparison["current_mean"] - comparison["reference_mean"])
  / comparison["reference_mean"] * 100
).round(1)

print(comparison)
```

A mean shift of more than 10–20% in a key feature warrants investigation. This is a rough heuristic, not a formal test, but it is effective for catching obvious problems.

#### Histogram Comparison

Visual comparison of feature distributions is often the most intuitive way to detect drift. Overlaying histograms from the training data and current data makes shifts immediately visible.

```python
import matplotlib.pyplot as plt

def plot_drift(reference, current, feature_name):
  """Overlay histograms of a feature from reference and current data."""
  fig, ax = plt.subplots(figsize=(7, 4))
  ax.hist(reference, bins=30, alpha=0.6, label="Training",
          density=True, color="steelblue")
  ax.hist(current, bins=30, alpha=0.5, label="Current",
          density=True, color="salmon")
  ax.set_title(f"Distribution Comparison: {feature_name}")
  ax.set_xlabel(feature_name)
  ax.set_ylabel("Density")
  ax.legend()
  plt.tight_layout()
  plt.show()

# Example: compare order_total distribution
plot_drift(
  df_reference["order_total"],
  df_current["order_total"],
  "order_total"
)
```

#### Population Stability Index (PSI)

For a more quantitative approach, the _Population Stability Index (PSI)_ measures how much a distribution has shifted. PSI is widely used in credit risk and financial modeling but applies to any numerical feature.

PSI works by binning both distributions into the same set of buckets, then measuring the divergence between the proportion of values in each bucket.

```python
import numpy as np

def compute_psi(reference, current, bins=10):
  """Compute Population Stability Index between two distributions."""
  # Create bins from reference distribution
  breakpoints = np.percentile(reference, np.linspace(0, 100, bins + 1))
  breakpoints[-1] += 1  # ensure max value falls in last bin

  ref_counts = np.histogram(reference, bins=breakpoints)[0]
  cur_counts = np.histogram(current, bins=breakpoints)[0]

  # Convert to proportions (avoid zero with small epsilon)
  eps = 1e-4
  ref_pct = ref_counts / len(reference) + eps
  cur_pct = cur_counts / len(current) + eps

  psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
  return round(psi, 4)

# Compute PSI for each numeric feature
numeric_features = [
  "order_total", "shipping_fee", "num_items",
  "avg_unit_price", "customer_age"
]

psi_results = {}
for col in numeric_features:
  if col in df_current.columns and col in df_reference.columns:
    psi_results[col] = compute_psi(
      df_reference[col].dropna().values,
      df_current[col].dropna().values
    )

for feature, psi_val in psi_results.items():
  status = "OK" if psi_val < 0.1 else (
    "MODERATE SHIFT" if psi_val < 0.25 else "SIGNIFICANT DRIFT"
  )
  print(f"{feature:<25} PSI = {psi_val:.4f}  [{status}]")
```

#### Interpreting PSI Values

The standard interpretation of PSI values is:

- **PSI < 0.10:** No significant drift. The distribution is stable.
- **0.10 ≤ PSI < 0.25:** Moderate shift. Investigate and monitor closely.
- **PSI ≥ 0.25:** Significant drift. The model should be retrained or the data pipeline should be reviewed.

#### Monitoring Categorical Features

For categorical features like _payment_method_ or _device_type_, drift detection checks whether new categories have appeared or whether the relative proportions have changed significantly.

```python
def check_categorical_drift(reference, current, feature_name):
  """Compare category proportions between reference and current."""
  ref_dist = reference.value_counts(normalize=True).sort_index()
  cur_dist = current.value_counts(normalize=True).sort_index()

  # Check for new categories
  new_cats = set(current.unique()) - set(reference.unique())
  if new_cats:
    print(f"WARNING: New categories in {feature_name}: {new_cats}")

  # Compare proportions
  comparison = pd.DataFrame({
    "reference": ref_dist,
    "current": cur_dist
  }).fillna(0)
  comparison["shift"] = (
    comparison["current"] - comparison["reference"]
  ).round(3)

  print(f"\n{feature_name}:")
  print(comparison.to_string())
  return comparison

check_categorical_drift(
  df_reference["payment_method"],
  df_current["payment_method"],
  "payment_method"
)
```

#### When to Act on Drift

Not all drift requires immediate action. Seasonal patterns (holiday order spikes, summer slowdowns) are expected and may not harm model performance. The key question is always: _has drift caused model performance to degrade?_ Drift detection and performance monitoring work together—drift explains the “why” when metrics explain the “what.”

---

## 18.5 Logging and Auditing

In a production environment, you need to answer questions like: _Which model version generated this prediction? When was it trained? What data was it trained on? How did it perform?_ If you cannot answer these questions, you cannot debug problems, reproduce results, or satisfy compliance requirements.

#### The Model Registry

A _model registry_ is a persistent record of every model version that has been trained. In enterprise systems, this is often a dedicated service (like MLflow or Weights & Biases). For our pipeline, a simple SQLite table does the job.

```python
import sqlite3
import json
from datetime import datetime

def register_model(wh_db_path, model_version, model_path,
                   metrics_dict, feature_list, notes=""):
  """Record a model version in the registry."""
  conn = sqlite3.connect(str(wh_db_path))
  cursor = conn.cursor()

  cursor.execute("""
  CREATE TABLE IF NOT EXISTS model_registry (
    registry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_version TEXT NOT NULL,
    model_path TEXT NOT NULL,
    trained_at TEXT NOT NULL,
    accuracy REAL,
    f1 REAL,
    roc_auc REAL,
    features TEXT,
    notes TEXT,
    is_active INTEGER DEFAULT 0
  )
  """)

  cursor.execute("""
  INSERT INTO model_registry
  (model_version, model_path, trained_at, accuracy, f1, roc_auc,
   features, notes, is_active)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
  """, (
    model_version,
    str(model_path),
    datetime.utcnow().isoformat(),
    metrics_dict.get("accuracy"),
    metrics_dict.get("f1"),
    metrics_dict.get("roc_auc"),
    json.dumps(feature_list),
    notes
  ))

  conn.commit()
  conn.close()
```

The _is_active_ column tracks which model version is currently serving predictions. Only one model should be active at a time.

#### Promoting a Model to Active

```python
def promote_model(wh_db_path, model_version):
  """Set a model version as the active model."""
  conn = sqlite3.connect(str(wh_db_path))
  cursor = conn.cursor()

  # Deactivate all
  cursor.execute(
    "UPDATE model_registry SET is_active = 0"
  )

  # Activate the specified version
  cursor.execute(
    "UPDATE model_registry SET is_active = 1 "
    "WHERE model_version = ?",
    (model_version,)
  )

  conn.commit()
  conn.close()

  print(f"Model {model_version} promoted to active.")
```

#### Querying the Registry

The registry gives you a complete audit trail of every model that has been trained, when it was trained, and how it performed.

```python
import pandas as pd

conn = sqlite3.connect("warehouse.db")

registry = pd.read_sql("""
  SELECT model_version, trained_at, accuracy, f1, roc_auc,
         is_active, notes
  FROM model_registry
  ORDER BY trained_at DESC
""", conn)

conn.close()

print(registry.to_string(index=False))
```

#### Pipeline Logging

Beyond model metrics, each pipeline run should log basic operational information: when it started, whether it succeeded, how many rows it processed, and any errors encountered. Python’s built-in _logging_ module is the standard tool for this.

```python
import logging

logging.basicConfig(
  filename="logs/pipeline.log",
  level=logging.INFO,
  format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("pipeline")

# Use in pipeline scripts:
logger.info("ETL started.")
logger.info(f"Loaded {len(orders)} orders from shop.db.")
logger.info(f"Warehouse updated: {row_count} rows written.")
logger.warning("F1 below threshold: 0.68")
logger.error("Failed to connect to shop.db: file not found.")
```

Structured logging creates a timeline of pipeline activity that you can review when investigating problems. The log file is the first place to look when something goes wrong.

![A diagram showing a model registry table with five rows representing different model versions trained over time. Each row shows: model version number (v1.0.0 through v1.0.4), training date, F1 score, ROC AUC, and an active status indicator. Only the most recent row (v1.0.4) has a green checkmark in the active column. Arrows on the left show the chronological progression from oldest to newest. A sidebar shows a log file excerpt with timestamped entries for ETL, training, and inference events. The overall style is clean and database-like, suggesting an organized audit trail.](../Images/Chapter18_images/mon_model_registry.png)

#### Why This Matters

Logging and registry practices may seem like overhead when you are building a pipeline for the first time. But when a model starts producing unexpected results three months from now, the registry and logs are what let you trace back to the cause. In regulated industries (finance, healthcare, insurance), this kind of audit trail is not optional—it is legally required.

---

## 18.6 Retraining Strategies

In the deployment chapter, the training script runs every night on a schedule. This is the simplest retraining strategy: retrain unconditionally on a fixed schedule, regardless of whether anything has changed. It works, but it is not always the best approach.

This section introduces three progressively smarter retraining strategies. Each adds a layer of intelligence to the decision of _when_ and _whether_ to update the production model.

#### Strategy 1: Scheduled Retraining

This is the approach from the deployment chapter: retrain on a fixed schedule (nightly, weekly, etc.) regardless of whether performance has changed. It is the simplest strategy and the easiest to implement.

- **Advantage:** the model always reflects the latest data.
- **Disadvantage:** wastes compute if nothing has changed. Can also introduce instability if a bad data batch produces a worse model.

#### Strategy 2: Triggered Retraining

With triggered retraining, the pipeline monitors performance metrics and data drift indicators. A new model is trained only when a threshold is crossed—for example, when the F1 score drops below 0.70 or when PSI exceeds 0.25 on a key feature.

```python
# Triggered retraining logic (add to the training job)
import json

def should_retrain(metrics_path, f1_threshold=0.70):
  """Check if the current model needs retraining."""
  try:
    with open(metrics_path, "r") as f:
      current_metrics = json.load(f)
    return current_metrics["f1"] < f1_threshold
  except FileNotFoundError:
    return True  # No model exists yet, train one

if should_retrain("artifacts/metrics.json"):
  print("Performance below threshold. Retraining...")
  train_and_save()
else:
  print("Model performance is acceptable. Skipping retraining.")
```

- **Advantage:** more efficient; avoids unnecessary retraining.
- **Disadvantage:** requires reliable monitoring. If the monitoring check itself is wrong, the model may not retrain when it should.

#### Strategy 3: Champion/Challenger

The champion/challenger pattern is the safest retraining strategy. It works as follows:

1. Train a new model (the _challenger_).
1. Evaluate the challenger on the same test data used to evaluate the current model (the _champion_).
1. If the challenger outperforms the champion, promote it. Otherwise, keep the champion.

This prevents a bad training run from accidentally degrading the production model.

```python
import json
import joblib
from pathlib import Path

def champion_challenger(new_metrics, champion_metrics_path,
                        new_model_path, champion_model_path,
                        metric="f1"):
  """Compare a new model against the current champion."""

  # Load champion metrics
  try:
    with open(champion_metrics_path, "r") as f:
      champion_metrics = json.load(f)
    champion_score = champion_metrics[metric]
  except FileNotFoundError:
    print("No champion found. Promoting new model.")
    return True

  challenger_score = new_metrics[metric]

  print(f"Champion {metric}: {champion_score:.4f}")
  print(f"Challenger {metric}: {challenger_score:.4f}")

  if challenger_score > champion_score:
    print("Challenger wins. Promoting new model.")
    return True
  else:
    print("Champion retains. New model discarded.")
    return False
```

#### Integrating Champion/Challenger into the Pipeline

To use this pattern, modify the training script to train to a temporary location, compare, and only overwrite the production model if the challenger wins:

```python
# Modified training flow with champion/challenger
import shutil

TEMP_MODEL = ARTIFACTS_DIR / "challenger_model.sav"
TEMP_METRICS = ARTIFACTS_DIR / "challenger_metrics.json"

# Train challenger (save to temp location)
joblib.dump(model, str(TEMP_MODEL))
with open(TEMP_METRICS, "w") as f:
  json.dump(metrics, f, indent=2)

# Compare against champion
should_promote = champion_challenger(
  new_metrics=metrics,
  champion_metrics_path=METRICS_PATH,
  new_model_path=TEMP_MODEL,
  champion_model_path=MODEL_PATH
)

if should_promote:
  shutil.move(str(TEMP_MODEL), str(MODEL_PATH))
  shutil.move(str(TEMP_METRICS), str(METRICS_PATH))
  print("Production model updated.")
else:
  TEMP_MODEL.unlink(missing_ok=True)
  TEMP_METRICS.unlink(missing_ok=True)
  print("Production model unchanged.")
```

#### Which Strategy to Use

For most student projects and early-stage systems, scheduled retraining is sufficient. As the system matures and the stakes increase, champion/challenger provides the strongest safety guarantee. The table below summarizes the tradeoffs:

---

## 18.7 Managing the Pipeline as a System

A deployed pipeline is not just a collection of scripts. It is a system with dependencies, configuration, and state. Managing it well means keeping its components in sync and making it resilient to the kinds of changes that inevitably happen over time.

#### Version Control

All pipeline code should live in version control (Git). This includes the ETL script, training script, inference script, configuration, and utility modules. It does _not_ include data files, model artifacts, or credentials—those should be listed in _.gitignore_.

```python
# .gitignore for a typical ML pipeline project
data/*.db
artifacts/*.sav
artifacts/*.json
logs/*.log
__pycache__/
.env
```

When you change a feature, fix a bug, or update the model architecture, committing the change to Git creates a permanent record that you can trace back to if something goes wrong.

#### Environment Management

Python packages change over time. A scikit-learn update might change default hyperparameters or preprocessing behavior, causing your model to produce different results even without any code changes. Pinning your dependencies prevents this.

```python
# Generate a requirements file from your current environment
pip freeze > requirements.txt

# Recreate the environment later
pip install -r requirements.txt
```

At minimum, pin the versions of scikit-learn, pandas, joblib, and any other library your pipeline imports. This ensures that the training and inference environments are identical.

#### Configuration Management

The deployment chapter introduced a _config.py_ file with shared paths and settings. As monitoring is added, the configuration grows to include thresholds, model versions, and feature lists. Keeping all of these in one place makes the system easier to understand and modify.

```python
# Extended config.py with monitoring settings
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Paths
DATA_DIR = PROJECT_ROOT / "data"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
LOGS_DIR = PROJECT_ROOT / "logs"

OP_DB_PATH = DATA_DIR / "shop.db"
WH_DB_PATH = DATA_DIR / "warehouse.db"

MODEL_PATH = ARTIFACTS_DIR / "late_delivery_model.sav"
METRICS_PATH = ARTIFACTS_DIR / "metrics.json"

# Monitoring thresholds
F1_ALERT_THRESHOLD = 0.70
PSI_DRIFT_THRESHOLD = 0.25

# Feature definitions (single source of truth)
NUMERIC_FEATURES = [
  "order_total", "shipping_fee",
  "num_items", "num_products", "avg_unit_price",
  "customer_age", "customer_order_count",
  "order_dow", "order_month", "promo_used"
]

CATEGORICAL_FEATURES = [
  "payment_method", "device_type"
]

LABEL_COL = "late_delivery"
```

Notice that feature definitions are now in the config file rather than duplicated across ETL, training, and inference scripts. This eliminates the risk of feature lists getting out of sync—a common and dangerous source of bugs in ML pipelines.

#### Handling Schema Changes

Operational databases change over time. A column may be renamed, a new table may be added, or a data type may change. When this happens, the ETL script will fail—and that is the correct behavior. A hard failure is better than a silent data quality problem.

To handle schema changes gracefully:

- Wrap database queries in try/except blocks and log clear error messages when a query fails.
- Run a lightweight schema validation check at the start of the ETL job (verify expected tables and columns exist before proceeding).
- When a schema change is intentional, update the ETL script, retrain the model, and bump the model version.

```python
def validate_schema(conn, expected_tables):
  """Check that expected tables exist in the database."""
  cursor = conn.cursor()
  cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
  )
  actual_tables = {row[0] for row in cursor.fetchall()}
  missing = set(expected_tables) - actual_tables
  if missing:
    raise RuntimeError(
      f"Schema validation failed. Missing tables: {missing}"
    )

# Use at the start of ETL
validate_schema(conn, ["orders", "customers", "order_items",
                       "shipments", "products"])
```

#### Keeping Scripts in Sync

The most dangerous pipeline bug is a mismatch between training and inference. If the ETL script engineers features differently than the inference script, the model will receive inputs it was not trained on and produce unreliable predictions.

The best defense is to share feature engineering code between scripts. Extract the feature logic into a shared module that both the ETL job and the inference job import. If the logic changes, it changes everywhere at once.

---

## 18.8 When to Retire or Replace a Model

Every model has a lifecycle. It is built, deployed, monitored, retrained, and eventually retired. Knowing when to retire a model is just as important as knowing how to build one.

![A circular lifecycle diagram showing five phases of a model's life: Build (represented by a code editor icon), Deploy (represented by a rocket icon), Monitor (represented by a dashboard with charts), Retrain (represented by a circular arrow with a gear), and Retire (represented by an archive box icon). Arrows connect each phase in a clockwise direction. Between Monitor and Retrain, a decision diamond asks Is performance acceptable? with Yes looping back to Monitor and No proceeding to Retrain. Between Retrain and Retire, another decision diamond asks Is the model still relevant? with Yes looping back to Deploy and No proceeding to Retire. The overall style is clean, circular, and professional.](../Images/Chapter18_images/mon_model_lifecycle.png)

#### Signs That a Model Should Be Retired

- **The business question has changed.** The model predicts late delivery, but the business now cares more about fraud detection. A model that answers the wrong question is not useful, regardless of its accuracy.
- **The data has fundamentally changed.** If the operational database is restructured, a new system replaces the old one, or the underlying process changes so much that retraining cannot recover performance, the model should be rebuilt from scratch.
- **Performance cannot be recovered.** After multiple rounds of retraining with different features and algorithms, the model still cannot meet the minimum performance threshold. This suggests that the available data no longer contains enough signal to support the prediction task.
- **The predictions are no longer used.** If the application has changed and no one consumes the predictions anymore, continuing to run the pipeline wastes resources and creates maintenance risk.

#### How to Retire a Model

Retiring a model is not just deleting files. It is a deliberate process:

1. **Communicate.** Notify stakeholders that the model will be retired and explain why.
1. **Disable the scheduler.** Remove or comment out the cron jobs that run the pipeline.
1. **Archive the artifacts.** Move the final model file, metadata, metrics, and logs to an archive location. Do not delete them—you may need them later for auditing or comparison.
1. **Mark the registry entry.** Update the model registry to show that the model is retired and no longer active.
1. **Clean up.** Remove the predictions table from the operational database if it is no longer needed, or leave it with a note that predictions are no longer being refreshed.

#### Replacing a Model

Replacing a model with a new version (different features, different algorithm, different target) follows the same champion/challenger pattern described earlier. Train the replacement, compare it to the current model on a shared evaluation set, and promote it only if it performs better.

When the replacement involves a completely different prediction task (for example, switching from late delivery to fraud detection), the old model should be retired and the new model should be deployed as a separate pipeline with its own ETL, training, inference, and monitoring scripts.

#### Key Idea

Models are not permanent. They are tools that serve a purpose for a period of time. Building the habit of deliberately retiring models—rather than letting them silently decay—is a sign of a mature, well-managed ML practice.

---

## 18.9 Practice

![An illustration showing a student extending their ML pipeline with monitoring capabilities. On the left, the existing pipeline from the deployment chapter (ETL, training, inference scripts). On the right, three new additions connected by arrows: a metrics log table icon with a growing chart, a drift detection script icon with histogram overlays, and a champion/challenger comparison icon with two model files being compared. The overall style suggests building on top of existing work rather than starting over.](../Images/Chapter18_images/mon_practice.png)

In this chapter, you learned the practices that keep a deployed ML pipeline reliable over time: performance monitoring, drift detection, model registration, retraining strategies, and system management.

In this practice section, you will extend the pipeline you built in the deployment chapter with monitoring capabilities.

#### Required Extensions

Add the following to your deployment pipeline:

1. **Metrics logging.** Modify _train_model.py_ to append metrics to a _metrics_log_ table in _warehouse.db_ after every training run. Include at least: training timestamp, model version, accuracy, F1 score, and ROC AUC.
1. **Drift detection script.** Create a new script (_jobs/check_drift.py_) that compares feature distributions between the warehouse training data and recent operational data. Compute PSI for at least three numeric features and report the results.
1. **Champion/challenger check.** Modify the training flow so that a new model is only promoted to production if it outperforms the current model on the primary metric (F1 score). If the new model is worse, the production model file should remain unchanged.

#### Optional Extensions

If time permits, consider adding:

- A model registry table in _warehouse.db_ with an _is_active_ column.
- A performance visualization (line chart of F1 over time) generated from the metrics log.
- A schema validation function that runs at the beginning of the ETL job and logs an error if expected tables or columns are missing.
- Structured logging using Python’s _logging_ module in all three pipeline scripts.

#### Applying to a Second Target

If you completed the deployment chapter practice exercise (building a pipeline for _is_fraud_ or _risk_score_), extend that pipeline with the same monitoring capabilities. This reinforces the idea that monitoring patterns are reusable across different prediction tasks.

#### Reflection Questions

After completing the exercise, answer the following:

- How would you detect concept drift (a change in the relationship between features and the target) as opposed to data drift (a change in feature distributions)?
- Under what circumstances would you prefer scheduled retraining over champion/challenger?
- If you could add one monitoring feature to the vibe-coded web app from the deployment chapter, what would it be and why?

---

## 18.10 Assignment

Complete the assignment below:

---
