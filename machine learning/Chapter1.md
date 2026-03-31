# Chapter 1: Data Mining Project Methodology

## Learning Objectives

- Students will be able to explain the six iterative phases of CRISP-DM and describe how data influences every phase of the process
- Students will be able to evaluate project feasibility by assessing practical impact, data availability, and analytical feasibility
- Students will be able to distinguish between decision support (explanatory) and machine learning pipeline (predictive) project types and select appropriate approaches
- Students will be able to compare CRISP-DM with alternative project methodologies (TDSP, SEMMA, KDD, OSEMN) and identify strengths and limitations of each

---

## 1.1 Introduction

#### Making Sense of the Pieces: CRISP-DM

One of the most common questions students ask is, “How does everything we’re learning fit together, and why are we doing it?” A practical way to address this question is by introducing a framework that is widely used in real-world data projects: the **Cross-Industry Standard Process for Data Mining (CRISP-DM)**.

**CRISP-DM** — A methodology for understanding how business problems are solved using data-based solutions. provides a structured, iterative approach for translating business objectives into data-driven outcomes. Although the tools and techniques used in data analytics continue to evolve, this framework remains relevant because it emphasizes adaptable principles rather than rigid, tool-specific procedures.

![A cycle with data at the center and six phases: business understanding, data understanding, data preparation, modeling, evaluation, and deployment. Arrows show iteration between adjacent phases and feedback loops that allow the process to restart based on evaluation results.](../Images/Chapter1_images/CRISP-DM_Process_Diagram.png)

#### The CRISP-DM Cycle

At its core, CRISP-DM is an iterative cycle with data at the center, emphasizing the role data plays in every phase of a project. The framework consists of six interconnected phases:

1. **Business Understanding:** Define the business problem, objectives, and success criteria.
1. **Data Understanding:** Identify available data sources, explore the data, and assess data quality.
1. **Data Preparation:** Clean, transform, and organize data so it is suitable for analysis or modeling.
1. **Modeling:** Select modeling techniques, algorithms, and features appropriate for the problem.
1. **Evaluation:** Evaluate model performance relative to business objectives and baseline approaches.
1. **Deployment:** Deliver results to stakeholders or integrate the solution into operational systems.

Importantly, CRISP-DM is not a linear checklist. Teams frequently revisit earlier phases as new insights emerge, data limitations are discovered, or business objectives evolve.

#### Characteristics of CRISP-DM

1. **Data Is Central:** Data influences every phase of the process, from shaping business questions to constraining modeling choices and deployment options.
1. **Iterative, Not Linear:** Data projects rarely proceed in a straight line. You often do not know whether the available data can support your objectives until you begin exploring and experimenting.
1. **"SatisficingA decision-making concept in which an option is selected not because it is optimal, but because the additional cost of improving it outweighs the additional value gained." Over Perfection:** While accuracy is important, there is a point at which additional refinements no longer justify their time and cost. Data scientists must balance technical improvements with business value.

In practice, this means that although data scientists may revisit nearly every phase multiple times, they are not expected to achieve perfection. At some point, a model must be selected and deployed. A model that is slightly less accurate but timely and actionable may be far more valuable than a marginally better model that arrives too late or costs too much to develop.

#### Flattening the Cycle: Questions for Each Phase

Another way to understand CRISP-DM is by focusing on the key questions each phase is designed to answer. This question-based view helps clarify the purpose of each phase, especially for those new to data projects.

![A diagram mapping each CRISP-DM phase to a guiding question: business understanding asks what the business needs; data understanding asks what data exists and how clean it is; data preparation asks how data should be cleaned and organized; modeling asks which techniques and features are appropriate; evaluation asks whether performance meets objectives and surpasses baselines; deployment asks how results are delivered to users.](../Images/Chapter1_images/CRISP_DM_questions.png)

Together, the process-based and question-based views reinforce that CRISP-DM is a flexible framework intended to guide thinking, not prescribe rigid steps.

#### Putting It Into Practice

Throughout this course, you have already been applying the CRISP-DM process, even if it was not explicitly labeled. Exploratory data analysis projects, dashboards, and predictive modeling assignments all naturally map to its phases.

The goal is for this framework to become second nature, ensuring that you always understand what you are doing, why it matters, and how each step contributes to a successful outcome. In the following sections, we examine each phase in greater detail and later compare CRISP-DM to alternative project methodologies.

---

## 1.2 Phase 1: Business Understanding

#### Understanding the Business Problem

The Business Understanding phase is critical for identifying opportunities where data mining can address business problems. Organizations often begin with a clear sense of potential opportunities, but many of the most successful companies intentionally dedicate time to brainstorming creative and high-impact possibilities before committing technical resources.

#### What Makes a Successful Data Project?

This phase defines the scope of the project by evaluating several key criteria that determine whether a data project is likely to succeed:

1. **Business Problem:** What specific challenge or opportunity is being addressed, and why does it matter?
1. **Data Availability:** Do we have the necessary data, or can it be feasibly acquired at a reasonable cost?
1. **Analytical Feasibility:** Can the available data support reliable analysis or prediction using appropriate statistical or machine learning methods?
1. **Process Integration:** Can the results be reasonably integrated into existing systems, workflows, or decision-making processes?
1. **Operational Disruption:** Will the project introduce inefficiencies or disruptions, particularly in sensitive environments such as healthcare or emergency services?

While all of these criteria are important, the first three primarily fall within the analytical responsibilities of the data scientist. The final two are organizational constraints that must be considered before a project moves forward. The diagram below focuses on the analytical core of project feasibility.

![Identifying Opportunities for Data Mining](../Images/Chapter1_images/ven_opportunities.png)

The yellow circle, labeled "**high practical impact**," represents projects that address meaningful business problems. Data projects typically begin here, as even technically sophisticated solutions provide little value if they do not address an important organizational need.

The blue circle, labeled "**data availability**," highlights the importance of accessible and affordable data. When relevant data already exists within organizational systems, costs are typically low. However, projects that rely on third-party data purchases, extensive surveys, or large-scale web scraping may become infeasible if acquisition costs exceed expected benefits.

The green circle, labeled "**analytical feasibility**," reflects whether the available data can support reliable analysis or prediction. For predictive modeling projects, this often means achieving sufficient predictive accuracy. For non-predictive projects, such as exploratory analysis or dashboards, feasibility may instead depend on interpretability, stability, or decision support value.

A data scientist leading a data project must ensure that these three analytical criteria are met to a reasonable degree before advancing to later phases.

#### Example: Nurse Scheduling in a Healthcare Setting

A healthcare provider sought to improve nurse scheduling by predicting required nurse hours in an emergency room. Data collection involved tracking two indicators: whether lab results were abnormal and the charge nurse's subjective assessment of case complexity. Initial analysis suggested a potential 60 percent improvement in predictive performance.

- **Data Collection Cost:** Nurses were required to manually enter data into a separate system.
- **Workflow Disruption:** Even a streamlined mobile application proved too disruptive in a fast-paced emergency room environment.

Despite promising analytical results, the project was discontinued because the operational costs and workflow disruptions outweighed the expected benefits. This example illustrates how a project can fail during the Business Understanding phase even when predictive performance appears strong.

#### Deliverables of the Business Understanding Phase

The Business Understanding phase resembles the initiation stage of traditional project management and requires a careful feasibility analysis. Typical deliverables include:

1. **Project Scope and Objectives:** Clearly define the problem, goals, and success criteria, such as increasing sales, reducing costs, or improving retention.
1. **Feasibility Analysis:** Evaluate whether the project can be realistically executed given organizational constraints.
1. **Cost-Benefit Analysis:** Compare anticipated benefits to both direct project costs and the opportunity cost of alternative initiatives.
1. **Project Plan:** Develop a high-level plan that outlines tasks, dependencies, timelines, resources, and expected iteration using appropriate project management practices.

#### Why the Business Understanding Phase Matters

The Business Understanding phase is foundational to CRISP-DM because it ensures that data projects are valuable, feasible, and aligned with organizational goals. By rigorously defining objectives and constraints at the outset, organizations reduce the risk of investing in technically sound projects that ultimately fail to deliver business value.

#### Related Articles

- How to ask the right questions

---

## 1.3 Phase 2: Data Understanding

#### Exploring the Data: The Data Understanding Phase

The **data understanding phase** — The second phase of the CRISP-DM, which focuses on initial data collection and activities designed to help you become familiar with the data, identify data quality problems, discover early insights, and detect interesting subsets for hypothesis generation., often referred to as **exploratory data analysis (EDA)** — The process of performing initial data investigations to discover patterns, spot anomalies, test hypotheses, and check assumptions using summary statistics and visualizations., centers on learning what the data contains and what it can support. This phase builds directly on the Business Understanding phase and provides the analytical foundation for later data preparation and modeling.

#### Key Objectives of the Data Understanding Phase

1. **Understand the Data:** Become familiar with the dataset’s structure, variables, scale, and limitations.
1. **Identify Target Variables:** Identify the target variable (also called the dependent variable or label) that represents the outcome of interest, such as sales, employee turnover, or patient costs.
1. **Analyze Relationships:** Explore how input variables (features) relate to the target variable using summary statistics and visualizations.
1. **Detect Quality Issues:** Identify missing, inconsistent, or anomalous data values and document potential remediation strategies.

It is important to note that the Data Understanding phase focuses on identifying data quality issues, not fixing them. Most data cleaning and transformation activities occur in the Data Preparation phase.

#### Example: Health Insurance Dataset Exploration

Consider the snapshot of a health insurance dataset shown below, which contains demographic and behavioral variables used to analyze medical insurance charges.

A common first step in Data Understanding is generating univariate statistics to examine each variable independently. This helps analysts understand distributions, ranges, data types, and potential anomalies before examining relationships between variables.

From this table, analysts can quickly identify important characteristics of the data. For example, insurance charges are highly right-skewed, indicating the presence of extreme values that may influence modeling. The mix of numeric and categorical variables also signals the need for appropriate encoding and transformation strategies in later phases.

Further exploration may involve multivariate visualizations, such as a 3D scatterplot examining how insurance charges relate jointly to body mass index (BMI) and age. These visualizations help identify nonlinear patterns, interactions, and potential feature relationships.

![3D Scatterplot Created During Data Understanding Phase](../Images/Chapter1_images/scatter.gif)

#### Importance of Data Understanding

Data Understanding plays a critical tactical role in a data project by revealing limitations, assumptions, and risks early. Without a solid grasp of the data, subsequent preparation and modeling decisions may be misguided or invalid.

#### Outputs of the Data Understanding Phase

1. **Initial data collection report**: Document data sources, acquisition methods, encountered issues, and resolutions.
1. **Data description report**: Summarize dataset structure, size, variable types, and surface-level characteristics.
1. **Data exploration report**: Present early insights, hypotheses, and supporting visualizations.
1. **Data quality report**: Identify missing or inconsistent data and document recommended remediation strategies.

Together, these outputs directly inform the Data Preparation and Modeling phases by guiding cleaning decisions, feature selection, and modeling strategy.

#### Why the Data Understanding Phase (EDA) Matters

The Data Understanding phase establishes the analytical foundation for the entire project. By thoroughly exploring and documenting the dataset, analysts reduce downstream risk and improve the reliability of modeling results. As a general rule, time invested in understanding the data pays dividends throughout the remainder of the project.

Suppose a dataset shows strong correlations between features and a target variable during the Data Understanding phase, but the data was collected using a process that may influence outcomes. Should the project proceed to modeling, or should concerns about data quality and potential bias stop the project early? Why?

---

## 1.4 Phase 3: Data Preparation

![Phase 3: Data Preparation](../Images/Chapter1_images/data_prep_banner.png)

#### Preparing the Data: The Data Preparation Phase

The **data preparation phase** — The third phase of the CRISP-DM, which covers all activities used to construct the final dataset that will be fed into modeling tools. involves transforming raw data into a refined dataset suitable for analysis and modeling. This phase is inherently iterative and does not follow a strict sequence, as preparation decisions often depend on insights uncovered during Data Understanding. The goal is to produce a smaller, relevant, and computationally efficient dataset that supports accurate and reliable modeling.

#### Key Objectives of Data Preparation

1. **Selection:** Choose the most relevant tables, records, and attributes (features) while discarding unnecessary or redundant data.
1. **Transformation:** Convert data into formats better suited for analysis, such as encoding categorical variables or applying mathematical transformations.
1. **Cleaning:** Address data quality issues such as missing values, outliers, and skewed distributions.
1. **Efficiency:** Improve processing speed and reduce storage costs by optimizing data volume and structure.

Unlike the Data Understanding phase, which focuses on identifying data issues, the Data Preparation phase performs the actual modifications needed to make the data usable for modeling.

#### Example: Data Preparation in Action

Common data preparation activities include the following:

- Converting categorical variables (for example, “High,” “Medium,” and “Low”) into ordinal or indicator values.
- Applying non-linear transformations, such as logarithmic or exponential functions, to reduce skewness.
- Creating new features, such as a debt-to-income ratio, that capture more meaningful relationships than raw variables alone.

These steps are often performed manually during exploration and experimentation, but they are later automated within an ETL (Extract, Transform, Load) pipeline to ensure consistency, reproducibility, and scalability across repeated analyses and production environments.

The primary outputs of the Data Preparation phase include the following artifacts:

1. **Selecting Data (Feature Selection):** Determine which variables and records are retained for modeling.
1. **Cleaning Data:** Address data quality issues identified earlier.
1. **Creating New Attributes and Records:** Enhance the dataset through feature engineering and record generation.
1. **Data Integration:** Combine data from multiple sources into a unified dataset.

#### Why the Data Preparation Phase Matters

The Data Preparation phase is critical because model performance is tightly coupled to data quality and structure. Effective preparation reduces computational overhead, improves predictive accuracy by emphasizing relevant features, and prevents downstream errors caused by poor data quality.

Carefully documenting preparation decisions ensures transparency, reproducibility, and a clear rationale for the modeling results that follow.

Can aggressive data preparation improve model performance while simultaneously reducing how well the model represents reality? Where should data scientists draw the line between useful transformation and distortion?

---

## 1.5 Phase 4: Modeling

#### Building the Model: The Modeling Phase

In the **modeling phase** — The fourth phase of the CRISP-DM, which involves applying statistical and machine learning algorithms to prepared data in order to predict, classify, or discover patterns in an outcome of interest., mathematical and computational techniques are applied to the prepared dataset to create candidate models. This phase typically involves selecting algorithms, tuning parameters, and iteratively refining data preparation decisions in response to model behavior.

- Selecting and calibrating appropriate modeling algorithms.
- Optimizing parameters and hyperparameters to improve performance.
- Iterating between modeling and data preparation to meet algorithm-specific requirements.

Because different algorithms impose different assumptions and data requirements, close coordination between the Data Preparation and Modeling phases is expected. It is common for modeling results to reveal the need for additional transformations, feature engineering, or data filtering before acceptable performance is achieved.

This phase also introduces the risk of **overfitting**, which occurs when a model learns noise or idiosyncrasies in the training data rather than generalizable patterns. Overfitting is especially likely during repeated experimentation and parameter tuning and must be carefully managed through disciplined testing and validation strategies.

#### Key Outputs of the Modeling Phase

1. **Modeling Technique Selection**
1. **Test Design Specification**
1. **Trained Model Artifacts**
1. **Preliminary Model Performance Summary**

At this stage, performance metrics are used to compare candidate models rather than to make final acceptance decisions. Formal evaluation against business objectives occurs in the Evaluation phase.

#### Example: Statistical Inference from Modeling

Suppose you are building a model to predict which products customers are likely to purchase at an online bicycle retailer. After testing multiple algorithms, the following patterns may emerge:

- Income and purchase history strongly influence purchasing behavior.
- Gender and ethnicity contribute little explanatory power.
- High correlation between age and income introduces masking effects.

These findings not only inform feature selection and model refinement but also provide actionable business insight, such as identifying which customer attributes should guide marketing or personalization strategies.

#### Why Model Documentation Matters

Comprehensive documentation during the Modeling phase supports multiple downstream objectives:

- Transparency, allowing stakeholders to understand modeling decisions.
- Reproducibility, enabling future teams to replicate or extend results.
- Governance and compliance, supporting auditing, monitoring, and responsible deployment.

Well-documented models are easier to evaluate, deploy, monitor, and update, reducing long-term risk and increasing organizational trust in analytical systems.

Should a business ever choose a less accurate model because it is easier to interpret or explain? In what situations might interpretability be more valuable than predictive performance?

---

## 1.6 Phase 5: Evaluation

#### Finalizing the Model: The Evaluation Phase

The **evaluation phase** — The fifth phase of the CRISP-DM, which focuses on determining whether candidate models meet technical, business, and operational criteria required to justify deployment. ensures that selected models perform well not only from a technical perspective but also in terms of business value, feasibility, and risk. This phase involves a structured review of modeling results, assumptions, and constraints to determine whether the solution should proceed to deployment, be refined further, or be discontinued.

While the Modeling phase answers the question of which techniques perform best technically, the Evaluation phase answers a different and more consequential question: should the organization move forward with this solution?

#### Key Objectives of the Evaluation Phase

- **Validate Business Alignment:** Confirm that the model meaningfully addresses the original business problem and supports organizational goals.
- **Assess Practical Feasibility:** Ensure that required data, infrastructure, and expertise remain available, affordable, and sustainable.
- **Quantify Business Impact:** Estimate how the model is expected to influence revenue, costs, risk, or operational performance.

#### Example: The Netflix Prize

In 2009, Netflix launched a competition offering $1,000,000 to improve its movie recommendation algorithm by 10%. Interim prizes of $50,000 were awarded for smaller 1% improvements. This structure reflected a deliberate evaluation strategy: Netflix explicitly defined performance thresholds that justified further investment.

This example highlights an important evaluation principle: performance improvements exhibit diminishing returns. At some point, additional accuracy gains no longer justify their cost, complexity, or implementation risk. Evaluation establishes these decision gates.

A common mistake during this phase is metric fixation, where small improvements in technical metrics are pursued without sufficient consideration of business impact. Effective evaluation balances quantitative performance with strategic judgment.

#### Outputs of the Evaluation Phase

1. **Evaluation Results Summary:** Synthesize technical performance and expected business impact.
1. **Deployment Readiness Review:** Assess organizational and technical readiness.
1. **Decision and Action Plan:** Define the approved path forward.

Importantly, deciding not to proceed with deployment can represent a successful evaluation outcome. By identifying misalignment or infeasibility early, organizations avoid costly implementation failures.

#### Why the Evaluation Phase Matters

The Evaluation phase ensures that technical success translates into business value. By integrating performance metrics, feasibility analysis, and strategic judgment, this phase mitigates risk and maximizes the return on data-driven initiatives.

If a model demonstrates strong technical performance but raises concerns related to cost, risk, or ethics, who should have the authority to stop the project? Should business leaders be able to override data scientists, or vice versa?

---

## 1.7 Phase 6: Deployment

![Phase 6: Deployment](../Images/Chapter1_images/deployment.jpg)

#### Deploying the Model: The Deployment Phase

The **Deployment Phase** — The sixth phase of the CRISP-DM process, in which analytical results are delivered as decision-support artifacts or operationalized as models embedded within production systems. ensures that the outputs of a data mining project are translated into usable, accessible, and actionable forms. Deployment may involve sharing insights through reports or dashboards, or integrating predictive models into live systems that automate or support decisions.

Importantly, deployment is not the end of a data project. Instead, it marks the beginning of an operational lifecycle in which models must be monitored, maintained, and periodically reassessed as business conditions and data evolve.

#### Key Objectives of the Deployment Phase

1. **Deliver Usable Insights:** Present findings and model outputs in formats that support decision-making by stakeholders such as managers, executives, or frontline staff.
1. **Operationalize Models:** Embed predictive models into information systems that automate or augment decisions, such as recommendation engines or risk-scoring tools.
1. **Enable Ongoing Learning:** Capture new data generated by deployed systems and use it to periodically retrain and refine models while maintaining governance and validation controls.

For example, Amazon’s product recommendation systems use deployed models to predict customer preferences based on behavioral data. These predictions guide actions while recording new interactions, such as clicks and purchases, which later inform model updates.

#### Outputs of the Deployment Phase

1. **Deployment Plan:** Define how model outputs will be delivered and consumed.
1. **Monitoring and Maintenance Plan:** Establish procedures to ensure continued model effectiveness.
1. **Final Report and Presentation:** Deliver a comprehensive summary of the entire project.
1. **Deployment Documentation:** Provide technical and operational details required for ongoing use.

A critical risk during deployment is silent failure, in which models continue to produce outputs even though data pipelines have broken or input patterns have changed. Without monitoring, these failures may go undetected and cause greater harm than not deploying a model at all.

Deployment is a shared responsibility among data scientists, engineers, and business stakeholders. Clear ownership and accountability are essential to ensure that models are used appropriately and maintained responsibly.

Ethical and risk considerations also become more prominent at deployment. Models may introduce bias, unintended consequences, or compliance risks when exposed to real-world users, making ongoing review and governance critical.

#### Why the Deployment Phase Matters

Deployment transforms analytical work into real-world impact. By ensuring accessibility, reliability, and ongoing oversight, this phase maximizes the value of data projects while reducing long-term technical, operational, and ethical risk.

If a deployed model begins to cause harm due to bias, misuse, or unexpected behavior, who should be held accountable, and what actions should be taken? Should some models never be deployed, even if they perform well technically?

---

## 1.8 Alternative Frameworks

While CRISP-DM is one of the most widely taught and widely adopted data mining process models, it is not the only framework used in practice. Organizations select or adapt methodologies based on industry context, project scale, regulatory constraints, uncertainty, and the need for agility.

In real-world settings, teams rarely follow a single framework rigidly. Instead, they blend elements from multiple methodologies, using CRISP-DM as a conceptual baseline while adopting specialized practices from more focused frameworks.

#### Agile Data Science

Agile Data Science, popularized by Russell Jurney, adapts agile software development principles to data science work. It emphasizes short experimentation cycles, incremental delivery, and frequent stakeholder feedback, making it well suited for problems with evolving requirements. Its flexibility is a strength, but without sufficient structure it can lead to scope creep and inconsistent documentation.

#### TDSP: Team Data Science Process

Team Data Science Process (TDSP) is a framework developed by Microsoft to support data science projects embedded within larger software engineering efforts. TDSP emphasizes documentation, standardized workflows, collaboration between data scientists and engineers, and integration with the software development lifecycle. It excels in production environments requiring governance and scalability, though it can be less flexible than agile approaches. Many organizations apply agile execution practices within the broader TDSP structure.

#### SEMMA: Sample, Explore, Modify, Model, Assess

SEMMA, developed by SAS, places heavy emphasis on data preparation and modeling activities. It is most effective when data sources and objectives are already well defined and predictive modeling is the primary goal. SEMMA is highly structured and closely aligned with SAS tooling, but it provides limited guidance for business framing and long-term deployment.

#### KDD: Knowledge Discovery in Databases

Knowledge Discovery in Databases (KDD), formalized by Fayyad, Piatetsky-Shapiro, and Smyth, emphasizes extracting novel and useful knowledge from data. It is commonly used in research and academic contexts where interpretation and theory-building are prioritized. KDD provides limited operational guidance, reflecting its focus on discovery rather than deployment.

#### OSEMN: Obtain, Scrub, Explore, Model, Interpret

OSEMN, associated with Hilary Mason and Chris Wiggins, presents a simplified and accessible data science lifecycle. Its clarity makes it useful for small-scale or exploratory projects, but it lacks guidance for governance, deployment, and long-term maintenance required in production environments.

#### DMAIC: Define, Measure, Analyze, Improve, Control

DMAIC, developed as part of Six Sigma, is a highly structured framework focused on process improvement and optimization. It is effective for projects with well-defined key performance indicators and repeatable processes. However, its rigor can be time-consuming for projects that require extensive exploratory analysis or rapid iteration.

#### CRISP-ML(Q): CRISP-DM for Machine Learning Quality

CRISP-ML(Q) extends CRISP-DM to address the realities of deployed machine learning systems. It explicitly incorporates quality assurance, risk management, and lifecycle considerations such as monitoring and maintenance. CRISP-ML(Q) is particularly valuable for projects where models are expected to operate continuously and degrade gracefully over time.

#### ASUM-DM: Analytics Solutions Unified Method

ASUM-DM, developed by IBM, is an enterprise-oriented extension of CRISP-DM. It provides more detailed guidance, templates, and governance structures for analytics projects implemented at scale. ASUM-DM is commonly used in large organizations that require consistency, documentation, and auditability.

#### Engineering and Operations Frameworks

In modern organizations, lifecycle frameworks are often complemented by engineering and operational disciplines that focus on reliability, automation, and governance rather than project structure.

Continuous Delivery for Machine Learning (CD4ML) adapts continuous delivery principles to machine learning, emphasizing automated pipelines for training, testing, and deploying models. CD4ML focuses on reducing deployment risk and accelerating iteration through automation.

DataOps applies DevOps principles to data analytics pipelines, improving reliability, collaboration, and speed in data preparation and reporting workflows.

ModelOps extends governance and lifecycle management to a broad range of decision models, including machine learning, statistical models, and rules-based systems. It is especially relevant in regulated or enterprise environments.

The table below summarizes key tradeoffs among these frameworks and highlights situations where each approach is most effective.

In this book, we follow CRISP-DM because it provides a clear and generalizable structure for learning how data projects progress from business understanding through deployment. In practice, effective data professionals adapt their approach by blending frameworks and operational practices to fit the project context. Learning one framework deeply makes it easier to understand and apply others.

How would your choice of methodology differ for a startup experimenting with a new product versus a regulated organization deploying machine learning in healthcare or finance? When does structure improve outcomes, and when does it hinder innovation?

---

## 1.9 Types of Data Projects

Data mining projects can produce a wide variety of deliverables, ranging from descriptive reports to fully automated machine learning systems. Although these projects differ in complexity, scope, and risk, most can be classified into one of two fundamental categories based on their primary objective: explaining outcomes or predicting outcomes.

Correctly identifying which type of project you are working on is critical because it determines which CRISP-DM phases receive the most emphasis, which methodologies are most appropriate, and how results are ultimately delivered to stakeholders.

#### The Two Fundamental Types of Data Projects

1. **Decision Support Projects (Explanatory Analytics)**
1. **Machine Learning Pipelines (Predictive Analytics)**

Many real-world projects begin as decision support analyses to understand drivers, feasibility, and business value and later evolve into machine learning pipelines once automation is justified. The distinction between these project types reflects differences in usage, risk, and deployment rather than differences in mathematical sophistication.

#### Adapting CRISP-DM to Different Project Types

CRISP-DM is intentionally flexible and can be applied to both explanatory and predictive projects. However, the emphasis placed on each phase differs substantially depending on the project type. The table below illustrates how the same CRISP-DM framework is adapted for decision support projects versus machine learning pipelines.

#### Frameworks and Methodologies by Project Type

Different data project methodologies align more naturally with different project types. Decision support projects emphasize interpretation and communication, while machine learning pipelines emphasize engineering, automation, and governance.

- **Decision Support Projects:** CRISP-DM, OSEMN, KDD, SEMMA
- **Machine Learning Pipelines:** TDSP, CRISP-ML(Q), Agile Data Science combined with MLOps, CD4ML, and ModelOps

If you ever find yourself asking, “Why are we doing this step?” the answer can almost always be traced back to which type of project you are working on and which CRISP-DM phase requires the most attention.

---

## 1.10 Reading Quiz

Take the quiz below to make sure you remember each of the key terms and concepts in this chapter.

### 1.10 Reading Quiz: CRISP-DM

---
