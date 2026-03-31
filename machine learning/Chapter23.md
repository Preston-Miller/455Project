# Chapter 23: NLP: Large Language Models (in progress)

## Learning Objectives

- Students will be able to explain LLM architecture including tokenization, embeddings, context windows, and attention mechanisms
- Students will be able to generate text using pretrained language models and control output behavior through temperature and sampling parameters
- Students will be able to design effective prompts for different NLP tasks using instruction-based and few-shot learning approaches
- Students will be able to generate and use text embeddings for semantic similarity measurement and downstream classification tasks

---

## 23.1 Introduction: From Text Features to Language Models

In the previous chapters, you learned how to transform unstructured text into structured features that traditional machine learning models can use. These techniques included linguistic features such as tokens, parts of speech, named entities, sentiment, and statistical representations such as n-grams and topic models.

These approaches share a common goal: converting raw language into numeric representations that capture meaning well enough for prediction, classification, or exploration. However, they require substantial manual design decisions, including feature selection, vocabulary construction, and preprocessing rules.

Large Language Models (LLMs) represent a major shift in this workflow. Instead of relying primarily on hand-engineered text features, LLMs learn rich language representations directly from massive collections of text through self-supervised training.

Rather than asking analysts to decide which linguistic features matter most, LLMs learn how words, phrases, and sentences relate to one another across many contexts. As a result, tasks that once required separate feature pipelines can often be solved using a single pretrained model.

This does not mean that earlier text analytics techniques are obsolete. Instead, LLMs should be understood as a higher-level abstraction that builds on the same foundational ideas introduced earlier: tokenization, frequency, context, and semantic similarity.

In this chapter, you will learn how Large Language Models fit into the broader Natural Language Processing (NLP) pipeline, how they differ from traditional text analytics methods, and how they can be used responsibly and effectively in real analytics workflows.

We will focus on practical usage rather than model training from scratch. You will interact with pretrained models, explore embeddings, design prompts, and evaluate outputs—skills that are increasingly essential for modern data analysts and data scientists.

Before diving into tools and code, the next section establishes a clear conceptual foundation by answering a simple but important question: what exactly is a Large Language Model?

---

## 23.2 What Is a Large Language Model?

A _Large Language Model (LLM)_ is a computer model designed to work with human language by learning statistical patterns in text. At its core, an LLM is a type of _language model_, meaning it estimates the probability of one piece of text following another.

Despite the name, a language model does not “understand” language in a human sense. Instead, it learns how likely certain sequences of words or symbols are based on patterns observed during training.

More specifically, an LLM is trained to perform a single fundamental task: _predict the next token in a sequence_. A token is a unit of text, which may represent a word, part of a word, punctuation, or special symbol.

By repeatedly predicting the next token across billions or trillions of examples, the model learns grammar, style, facts, associations, and contextual relationships embedded in language. Complex behaviors such as summarization, translation, and question answering emerge from this simple prediction objective.

The term _large_ refers primarily to scale. LLMs are trained using very large neural networks with millions, billions, or even trillions of parameters. Parameters are learned numerical values that store information about patterns in the data.

Scale also applies to training data. LLMs are trained on massive collections of text drawn from books, articles, websites, code repositories, and other publicly available sources. This exposure allows them to model a wide range of language styles and domains.

As models grow larger, their behavior changes in important ways. Certain capabilities—such as following complex instructions, performing multi-step reasoning, or adapting to new tasks with minimal examples—appear only once a model reaches sufficient size. This phenomenon is often referred to as _emergent behavior_.

Although these behaviors can appear impressive, it is important to understand what LLMs are not. They are not databases that retrieve stored answers, and they do not possess understanding, intent, or consciousness.

LLMs also do not reason in the same way humans do. When they appear to reason, they are generating sequences of text that statistically resemble reasoning patterns seen during training.

The outputs of an LLM are therefore best understood as _probabilistic text generation_ rather than factual guarantees. This is why LLMs can sometimes produce fluent but incorrect or inconsistent responses.

By the end of this chapter, you should view Large Language Models as powerful probabilistic sequence models that can assist with language-related tasks, but which must be used carefully, critically, and in combination with human judgment.

---

## 23.3 Tokens, Embeddings, and Context Windows

This section connects your earlier text analytics work (tokenization, n-grams, and topic modeling) to the way modern Large Language Models represent and process language. The big idea is simple: LLMs convert text into _tokens_, convert tokens into _embeddings_, and then use a limited working memory called a _context window_ to generate the next token.

_Tokenization_ in LLMs is similar in spirit to tokenization from earlier chapters, but the units are usually different. In traditional NLP, you often tokenize into words or sentences. LLMs typically use _subword tokens_, which means a single word may be split into multiple tokens, especially for rare words, long words, or unfamiliar names.

Subword tokenization is useful because it balances two goals: it keeps common words as single tokens (efficient), while still allowing the model to represent any new word by breaking it into smaller pieces (flexible). This is one reason LLMs can handle new vocabulary, misspellings, and domain-specific language better than word-only tokenization.

Once text is tokenized, each token is mapped to an integer ID from a vocabulary. Those IDs are not meaningful by themselves, so the model immediately converts them into _embeddings_.

An _embedding_ is a learned numeric vector that represents a token (or sometimes an entire sequence) in a way that preserves meaning and relationships. You can think of embeddings as a modern version of features: instead of manually engineering linguistic features, the model learns dense representations that capture patterns such as similarity, association, and context-dependent usage.

This is where the conceptual payoff happens. Earlier in this textbook, you used methods like _n-grams_ and _topic modeling_ to create structure from unstructured text. Those approaches represent documents using counts or probabilities over words and topics. Embeddings do something similar, but in a more flexible way: they represent text in a continuous space where semantic similarity can be measured directly.

In other words, many classic NLP tools are based on _sparse representations_ (large vectors with mostly zeros). Modern models rely heavily on _dense semantic representations_ (compact vectors where every dimension contributes information).

The third key idea is the _context window_. An LLM does not read your entire document, your entire chat history, or “everything it knows” all at once. Instead, it can only consider a limited number of tokens at a time. This limit is the context window.

When your prompt (plus system instructions and conversation history) exceeds the context window, older content must be truncated or summarized. This is why prompt length, document chunking, and retrieval strategies matter so much in real-world applications.

Inside the context window, LLMs use a mechanism commonly described as _attention_. You do not need the math to understand the intuition: attention allows the model to weigh which earlier tokens are most relevant to predicting the next token. This is how an LLM can “refer back” to details from earlier in the prompt, follow multi-part instructions, and maintain short-term coherence.

These three components form a pipeline:

1. **Tokenization**: break text into subword pieces that the model can represent reliably.
1. **Embeddings**: convert token IDs into learned feature vectors that capture usage and meaning.
1. **Context Window + Attention**: process a limited span of tokens and focus on what matters most to predict the next token.

If you remember nothing else from this section, remember this: LLMs do not “see words,” they see tokens; they do not “store meanings,” they learn embeddings; and they do not “remember forever,” they operate within a context window.

This framing will help you understand why prompts work, why models can lose track of earlier content, and why embeddings are central to modern workflows like semantic search and retrieval-augmented generation.

---

## 23.4 Using Pretrained LLMs

In this section, you will begin working directly with pretrained large language models. The goal is not to train models from scratch, but to understand how modern NLP systems are accessed, configured, and used in practice.

All examples in this section are designed to run in either Google Colab or a local Python environment (such as Cursor). When working with larger models or training steps, Colab is recommended due to GPU availability.

We will use a small set of widely adopted libraries that reflect current industry and research practice. These same tools are used internally by many commercial AI systems.

#### Installing Required Packages

Before loading or using pretrained models, you must install the required Python packages. These commands work in both Colab and local environments.

```python
pip install transformers datasets torch accelerate evaluate peft
```

If you plan to use hosted models through an API (instead of running models locally), you will also need the appropriate client library. For example:

```python
pip install openai
```

After installation, you can verify that your environment is set up correctly by importing the core libraries:

```python
import torch
from transformers import pipeline
print(torch.__version__)
```

If a GPU is available (such as in Colab Pro), PyTorch will automatically detect it. You do not need to write different code for CPU and GPU execution.

#### What “Pretrained” Means

A pretrained language model has already been trained on a very large corpus of text using the next-token prediction objective introduced earlier in this chapter. This training process can take weeks or months and requires specialized hardware.

When you use a pretrained model, you are not retraining it. Instead, you are loading learned parameters and using them to generate predictions, embeddings, or classifications.

This is analogous to using a pretrained image model or a pretrained regression model: the learning has already happened, and you are applying the model to new data.

#### A First Example: Text Generation

The simplest way to work with a pretrained model is through a high-level pipeline. Pipelines bundle tokenization, model inference, and output decoding into a single interface.

```python
generator = pipeline("text-generation", model="distilgpt2")
output = generator("Large language models are powerful because", max_length=50)
print(output[0]["generated_text"])
```

Although this example looks simple, several important steps are happening behind the scenes: text is tokenized, tokens are converted to embeddings, attention is applied over the context window, and probabilities are used to generate the next tokens.

At this stage, you should focus on understanding what the model produces and how changes to prompts affect the output. You do not need to understand the internal transformer math to use these systems effectively.

#### Hands-On: Determinism and Randomness

Language model outputs are not fixed. They are generated by sampling from probability distributions over tokens. Parameters such as temperature control how much randomness is introduced during generation.

```python
generator = pipeline("text-generation", model="distilgpt2")

prompt = "Artificial intelligence will change business by"

low_temp = generator(prompt, max_length=50, temperature=0.3, do_sample=True)
high_temp = generator(prompt, max_length=50, temperature=1.2, do_sample=True)

print("Low temperature:")
print(low_temp[0]["generated_text"])
print("\nHigh temperature:")
print(high_temp[0]["generated_text"])
```

Lower temperatures produce more conservative, repetitive outputs, while higher temperatures produce more diverse and unpredictable text. Neither setting is “correct”—the choice depends on the task.

#### Hands-On: Prompt Sensitivity

Small changes in prompt wording can significantly affect model output. This is because prompts shift the probability distribution over future tokens.

```python
prompt_a = "Explain machine learning in simple terms."
prompt_b = "Explain machine learning in simple terms for a business executive."

out_a = generator(prompt_a, max_length=60)
out_b = generator(prompt_b, max_length=60)

print("Prompt A:")
print(out_a[0]["generated_text"])
print("\nPrompt B:")
print(out_b[0]["generated_text"])
```

Notice that the model does not “understand” intent in a human sense. Instead, different prompts bias the model toward different regions of its learned distribution.

#### Hands-On: Repetition and Failure Modes

Running the same prompt multiple times can produce different outputs, even with identical settings. This variability is a feature, not a bug.

```python
for i in range(3):
  out = generator("Data analytics is important because", max_length=40, do_sample=True)
  print(f"Run {i+1}:")
  print(out[0]["generated_text"])
  print()
```

This behavior highlights an important limitation: prompts do not create memory, logic, or guarantees. They influence probabilities, not rules.

As you continue through this chapter, you will learn when prompting is sufficient, when embeddings are more appropriate, and when fine-tuning is required.

In the next section, we formalize these ideas by treating prompting as a form of interface design rather than a collection of tricks.

---

## 23.5 Prompting as Interface Design

Prompting is often presented as a collection of tricks or recipes. In practice, prompting is better understood as _interface design_ for probabilistic models.

A prompt does not give an LLM rules to follow. Instead, it shapes the probability distribution over possible next tokens. Small changes in wording can shift model behavior because they change statistical expectations, not because the model is executing logic.

This perspective mirrors earlier chapters on feature engineering. Just as feature choices influence model behavior without guaranteeing outcomes, prompt design influences generation without enforcing correctness.

#### Instructions vs. Examples

Prompts typically include two distinct elements: _instructions_ and _examples_. Instructions describe what the model should do, while examples demonstrate the desired behavior.

Instructions rely on the model recognizing familiar task patterns (for example, summarization or classification). Examples reduce ambiguity by anchoring the prompt to concrete input–output pairs.

When a task is underspecified or subjective, examples often matter more than detailed instructions. This is known as _few-shot prompting_.

#### Hands-On: Instructions Alone vs. Instructions with Examples

In this experiment, you will compare a prompt that relies only on instructions with one that includes examples. Notice how examples constrain model behavior.

```python
from transformers import pipeline

generator = pipeline("text-generation", model="distilgpt2")

instruction_only = "Classify the sentiment of this review as Positive or Negative: The product was fine but nothing special."
with_examples = """Classify the sentiment of the review.

Review: I loved this product and would buy it again.
Sentiment: Positive

Review: This was a terrible experience and a waste of money.
Sentiment: Negative

Review: The product was fine but nothing special.
Sentiment:"""

out1 = generator(instruction_only, max_length=40)
out2 = generator(with_examples, max_length=40)

print("Instruction only:")
print(out1[0]["generated_text"])
print("\nWith examples:")
print(out2[0]["generated_text"])
```

The second prompt provides a clearer pattern for the model to continue. The model is not reasoning about sentiment; it is continuing a learned text pattern.

#### System Messages vs. User Messages

Many LLM interfaces separate prompts into different message roles, most commonly _system_ messages and _user_ messages.

System messages establish high-level context such as role, tone, or constraints. User messages provide the task-specific input. Importantly, both are part of the same context window and are processed together.

System messages do not create hard rules. They bias generation in the same probabilistic way as any other text, although models are often trained to weight them more strongly.

#### Few-Shot Prompting

Few-shot prompting provides the model with a small number of examples inside the prompt. Each example functions like a training observation, but only within the temporary context window.

This approach is conceptually similar to nearest-neighbor methods or prototype-based learning: the model extrapolates from recent patterns rather than learning permanently.

Because examples consume context window space, there is a tradeoff between providing clarity and preserving room for new input.

#### Hands-On: Prompt Sensitivity and Drift

Small wording changes can lead to noticeably different outputs. This experiment illustrates how prompts influence probabilities rather than enforce logic.

```python
prompt_a = "Summarize the following text in one sentence:"
prompt_b = "Summarize the following text for an executive audience in one sentence:"

text = "The company experienced moderate revenue growth but faced rising costs and increased competition during the fiscal year."

out_a = generator(prompt_a + " " + text, max_length=60)
out_b = generator(prompt_b + " " + text, max_length=60)

print("Generic summary:")
print(out_a[0]["generated_text"])
print("\nExecutive-focused summary:")
print(out_b[0]["generated_text"])
```

Neither prompt is objectively better. Each simply biases the model toward different stylistic and semantic regions of its training distribution.

#### Common Failure Modes

Understanding failure modes is more valuable than memorizing prompt templates. Common issues include hallucination, overconfidence, instruction drift, and sensitivity to phrasing.

These failures occur because the model optimizes for plausibility, not truth. A well-written prompt reduces risk but does not eliminate it.

#### Hands-On: Repeated Runs and Inconsistency

Running the same prompt multiple times often produces different outputs. This variability highlights why prompts cannot be treated as deterministic logic.

```python
for i in range(3):
  out = generator("Explain why data quality matters in analytics.", max_length=50, do_sample=True)
  print(f"Run {i+1}:")
  print(out[0]["generated_text"])
  print()
```

This behavior reinforces a critical lesson: prompting does not create memory, rules, or guarantees. It only shifts probabilities.

#### Key Mental Models

- Prompting is _probabilistic control_, not logical control.
- Prompts do not create memory, learning, or reasoning.
- Examples influence behavior temporarily through context, not training.
- Prompt quality affects reliability, but correctness must still be verified.

Seen through this lens, prompting becomes less mysterious. It is a form of feature engineering for language models, where text replaces numeric features.

In the next section, you will move beyond prompting alone and explore how embeddings allow language models to represent meaning numerically and support downstream tasks such as similarity, clustering, and retrieval.

---

## 23.6 Embeddings for NLP Tasks

Embeddings are the most important bridge between large language models and traditional analytics workflows. They transform unstructured text into numeric feature vectors that can be analyzed using the same tools you have used throughout this book.

From an analytics perspective, an embedding is simply a learned representation of text in a high-dimensional space. Each sentence, paragraph, or document becomes a dense vector that captures semantic meaning.

This idea directly connects earlier chapters on feature engineering, topic modeling, clustering, and classification. The difference is that embeddings replace sparse, manually engineered text features with compact, learned representations.

#### From Bag-of-Words to Embedding Spaces

Earlier text analytics approaches relied on bag-of-words, TF-IDF, n-grams, and topic models. These methods represent text as sparse vectors based on word counts or probabilities.

Embeddings shift this paradigm. Instead of counting words, models learn to place semantically similar texts near each other in a continuous vector space.

This allows similarity, clustering, and classification tasks to be performed using distance metrics rather than explicit linguistic rules.

#### Hands-On: Generating Sentence Embeddings

In this first experiment, you will generate embeddings for a small set of sentences using a pretrained sentence transformer model.

```python
from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

sentences = [
    "Customer service was excellent and fast.",
    "The support team resolved my issue quickly.",
    "The product broke after one day of use."
]

inputs = tokenizer(sentences, padding=True, truncation=True, return_tensors="pt")
with torch.no_grad():
    outputs = model(**inputs)

embeddings = outputs.last_hidden_state.mean(dim=1)
print(embeddings.shape)
```

Each sentence is now represented as a numeric vector. These vectors can be treated like any other feature matrix in analytics workflows.

#### Sentence and Document Embeddings

Modern embedding models can generate vectors for sentences, paragraphs, or entire documents. These embeddings are typically produced by pretrained transformer models.

In practice, you will most often use sentence- or document-level embeddings as general-purpose features for downstream tasks rather than training models from scratch.

#### Semantic Similarity

Once text is represented as embeddings, semantic similarity can be measured using distance metrics such as cosine similarity.

This enables use cases such as finding similar customer reviews, matching support tickets, deduplicating text records, or identifying related documents without keyword matching.

#### Hands-On: Measuring Semantic Similarity

```python
from sklearn.metrics.pairwise import cosine_similarity

similarity_matrix = cosine_similarity(embeddings)
print(similarity_matrix)
```

Sentences with similar meaning produce higher cosine similarity scores, even when they share few or no keywords.

#### Clustering and Retrieval

Embeddings make clustering text conceptually identical to clustering numeric data. Algorithms such as k-means or hierarchical clustering can be applied directly to embedding vectors.

This mirrors topic modeling, but with an important distinction: clusters emerge from semantic distance rather than probabilistic word distributions.

Embedding-based retrieval systems search for nearest neighbors in vector space, forming the foundation of modern search, recommendation, and retrieval-augmented generation systems.

#### Hands-On: Clustering Text with Embeddings

```python
from sklearn.cluster import KMeans

kmeans = KMeans(n_clusters=2, random_state=42)
labels = kmeans.fit_predict(embeddings)

for sentence, label in zip(sentences, labels):
  print(label, "-", sentence)
```

This clustering task is mathematically identical to clustering numeric customer or transaction data, reinforcing the continuity with earlier analytics chapters.

#### Classification with Embeddings

Embeddings can also be used as input features for supervised learning. Instead of training a text-specific model, you can train a standard classifier on top of embedding vectors.

This approach is often faster, more stable, and easier to interpret than end-to-end fine-tuning, especially when labeled data is limited.

#### Hands-On: Classification Using Embeddings

```python
from sklearn.linear_model import LogisticRegression

labels = [1, 1, 0]  # positive, positive, negative
clf = LogisticRegression()
clf.fit(embeddings, labels)

preds = clf.predict(embeddings)
print(preds)
```

Here, the language model provides representations, while a traditional classifier performs the supervised learning.

#### Key Conceptual Links

- Topic modeling identifies themes probabilistically; embeddings organize text geometrically.
- Bag-of-words produces sparse vectors; embeddings produce dense vectors.
- Distance in embedding space replaces keyword overlap.
- Classic ML algorithms remain useful when paired with modern text representations.

#### Tools and Packages

In this chapter, embeddings are generated using pretrained models from the Hugging Face ecosystem. The core packages include _transformers_ and _torch_.

For downstream analytics tasks such as similarity measurement, clustering, and classification, familiar tools such as _scikit-learn_ can be used without modification.

This combination allows you to integrate modern language models into traditional analytics pipelines while preserving interpretability, flexibility, and computational efficiency.

---

## 23.7 Fine-Tuning a Pretrained Model

Fine-tuning is the process of adapting a pretrained language model to perform a specific task using labeled data. Conceptually, it is no different from supervised learning applied elsewhere in analytics: the model adjusts its parameters to reduce prediction error on a defined objective.

This section focuses only on _fine-tuning_, not training models from scratch. Training a large language model from raw text requires massive datasets, specialized hardware, and budgets far beyond the scope of this course.

Instead, you will work with small pretrained models, small datasets, and clearly defined tasks to understand what learning looks like in modern NLP systems.

#### Why Fine-Tune Instead of Prompting?

Prompting is a powerful interface for controlling model behavior, but it does not change the model itself. Fine-tuning is appropriate when you need consistent, repeatable behavior that prompting alone cannot reliably achieve.

Common reasons to fine-tune include domain-specific language, specialized classification tasks, constrained output formats, or performance requirements that exceed what prompting can deliver.

Fine-tuning trades flexibility for stability. Once trained, the model behaves predictably for the task it was optimized to perform.

#### What Fine-Tuning Actually Changes

During fine-tuning, the model’s parameters are updated using labeled examples and a loss function, just like any supervised machine learning model.

The pretrained model provides a strong starting point: it already understands language structure, syntax, and general semantics. Fine-tuning nudges this representation toward your specific task.

Importantly, fine-tuning does not give the model new reasoning abilities, memory, or awareness. It simply reshapes probability distributions based on observed examples.

#### Parameter-Efficient Fine-Tuning (LoRA)

Full fine-tuning updates all model parameters, which can be computationally expensive even for relatively small models.

Parameter-efficient fine-tuning methods, such as _LoRA (Low-Rank Adaptation)_, reduce this cost by training a small number of additional parameters while freezing the original model weights.

This approach dramatically lowers memory usage, training time, and cost while still achieving strong task performance.

For most applied analytics use cases, parameter-efficient fine-tuning is the preferred approach.

#### Hands-On: Fine-Tuning a Small Sentiment Classifier

In this lab, you will fine-tune a small pretrained model for binary sentiment classification. The dataset is intentionally small, and the goal is to observe the supervised learning workflow rather than to maximize accuracy.

```python
from datasets import load_dataset

dataset = load_dataset("imdb", split="train[:2000]")
dataset = dataset.train_test_split(test_size=0.2, seed=42)
print(dataset)
```

This dataset contains text reviews and binary sentiment labels, similar to classification problems you have seen earlier in the book.

```python
from transformers import AutoTokenizer, AutoModelForSequenceClassification

model_name = "distilbert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2)
```

```python
from peft import LoraConfig, get_peft_model

lora_config = LoraConfig(
    r=8,
    lora_alpha=16,
    target_modules=["q_lin", "v_lin"],
    lora_dropout=0.1,
    bias="none",
    task_type="SEQ_CLS"
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()
```

Only a small number of parameters will be trained. The original model weights remain frozen.

```python
from transformers import Trainer, TrainingArguments

def tokenize(batch):
    return tokenizer(batch["text"], truncation=True, padding=True)

tokenized = dataset.map(tokenize, batched=True)

training_args = TrainingArguments(
  output_dir="./results",
  evaluation_strategy="epoch",
  learning_rate=2e-4,
  per_device_train_batch_size=16,
  per_device_eval_batch_size=16,
  num_train_epochs=2,
  logging_steps=50,
  report_to="none"
)

trainer = Trainer(
  model=model,
  args=training_args,
  train_dataset=tokenized["train"],
  eval_dataset=tokenized["test"]
)

trainer.train()
```

This training loop mirrors classic supervised learning: batches, loss minimization, and evaluation on held-out data.

#### Warnings About Scale and Cost

Fine-tuning scales quickly in cost as model size and dataset size increase. Even moderate models can require significant compute resources if misconfigured.

In practice, many production systems rely on embeddings, prompting, or hybrid approaches rather than fine-tuning large models.

Understanding fine-tuning is essential for literacy and informed decision-making, even when it is not the right tool for a given problem.

#### Key Learning Outcome

Fine-tuning is supervised learning applied to pretrained language representations. It is powerful, but it is not magic.

---

## 23.8 Evaluation and Failure Analysis

Despite their sophistication, large language models are still machine learning models. As a result, the same evaluation principles you learned earlier in this course continue to apply.

LLMs can appear fluent, confident, and persuasive even when they are wrong. This makes disciplined evaluation especially important, because failures are often subtle rather than obvious.

#### Train/Test Splits Still Matter

Fine-tuned language models must be evaluated on data that were not seen during training. Without a proper train/test split, it is impossible to tell whether the model has learned a generalizable pattern or simply memorized examples.

This is particularly important for small datasets, where memorization can occur quickly and produce deceptively strong results.

#### Overfitting Still Happens

Overfitting is not eliminated by scale or pretraining. A fine-tuned model can easily overfit to a narrow dataset, especially when training for too many epochs or using overly aggressive learning rates.

Symptoms of overfitting include strong performance on training data, degraded performance on validation data, and brittle behavior when inputs change slightly.

#### Bias and Brittleness

Language models inherit biases from both their pretraining data and any datasets used during fine-tuning. These biases may surface as systematic errors, uneven performance across groups, or unexpected associations.

Brittleness refers to the tendency of models to fail when inputs are phrased differently, include rare terms, or fall outside the narrow patterns seen during training.

Because LLM outputs are probabilistic, failures may be inconsistent and difficult to reproduce, reinforcing the need for structured testing.

#### When Fine-Tuning Hurts Performance

Fine-tuning does not guarantee improvement. In some cases, it can degrade performance by overwriting useful general representations learned during pretraining.

This risk is highest when datasets are small, noisy, or poorly aligned with the task objective.

In practice, prompting or embedding-based approaches may outperform fine-tuning for many real-world applications.

#### Diagnostic Lab: Evaluating a Fine-Tuned Model

In this lab, you will evaluate the fine-tuned sentiment classifier from the previous section. The goal is not to optimize metrics, but to diagnose model behavior and failure modes.

```python
import torch
from torch.nn.functional import softmax

def predict(model, tokenizer, texts):
  inputs = tokenizer(texts, truncation=True, padding=True, return_tensors="pt")
  with torch.no_grad():
    outputs = model(**inputs)
  probs = softmax(outputs.logits, dim=1)
  preds = torch.argmax(probs, dim=1)
  return preds, probs

texts = tokenized["test"]["text"]
labels = tokenized["test"]["label"]

preds, probs = predict(model, tokenizer, texts)
```

```python
import evaluate

accuracy = evaluate.load("accuracy")
f1 = evaluate.load("f1")

acc_result = accuracy.compute(predictions=preds, references=labels)
f1_result = f1.compute(predictions=preds, references=labels)

print("Accuracy:", acc_result)
print("F1 Score:", f1_result)
```

These metrics should look familiar. Fine-tuned language models are evaluated using the same performance measures as any other classifier.

```python
import pandas as pd

results = pd.DataFrame({
  "text": texts,
  "true_label": labels,
  "predicted_label": preds.numpy()
})

errors = results[results["true_label"] != results["predicted_label"]]
errors.sample(5)
```

Manual inspection often reveals brittleness, ambiguous language, sarcasm, or domain-specific phrasing that the model struggles to interpret.

```python
examples = [
  "This movie was great.",
  "I thought this movie was great.",
  "This movie was actually great.",
  "This movie was not bad at all."
]

preds, probs = predict(model, tokenizer, examples)

for text, pred in zip(examples, preds):
  print(f"{text} -> Predicted label: {pred.item()}")
```

Small wording changes can shift predictions, even when human interpretation remains stable. This is a common source of silent failure.

#### Evaluating LLM Outputs

Evaluation of language models often combines quantitative metrics with qualitative inspection. Classification tasks may use familiar metrics such as accuracy, precision, recall, and F1 score.

For generation tasks, evaluation may involve similarity metrics, task-specific scoring rules, or structured human review.

The _evaluate_ package provides a standardized way to compute and compare evaluation metrics across experiments.

#### Key Takeaways

LLMs do not replace evaluation. They increase the need for it.

Fine-tuned models can fail silently, producing confident outputs that hide bias, brittleness, or overfitting.

Treat language models as you would any other predictive system: test them rigorously, inspect their errors, and remain skeptical of apparent fluency.

---

## 23.9 When Not to Use an LLM

As powerful as large language models are, they are not always the right tool. Mature analytics practice requires knowing when _not_ to apply a complex solution.

This section reinforces a theme that appears throughout this book: better models are not always bigger models.

#### Cost Versus Benefit

LLMs are computationally expensive. Inference costs, infrastructure requirements, and fine-tuning expenses can quickly exceed the value provided by the model.

For many tasks, simpler approaches such as linear models, decision trees, or classical NLP techniques can achieve comparable or better results at a fraction of the cost.

#### Latency and Reliability Constraints

LLMs often introduce latency that is unacceptable for real-time systems, high-throughput pipelines, or user-facing applications with strict response-time requirements.

Systems that require deterministic, fast, and repeatable outputs may perform better with traditional models or rule-based logic.

#### When Simpler Models Outperform

For structured data, well-defined prediction targets, and stable feature relationships, traditional machine learning models often outperform LLMs in both accuracy and interpretability.

This is especially true for tabular data, where feature-engineered models remain the industry standard.

#### Ethical and Privacy Considerations

LLMs raise additional ethical concerns, including data leakage, unintended memorization, bias amplification, and lack of transparency.

Sensitive data, regulated environments, and privacy-critical applications may prohibit the use of external APIs or opaque pretrained models.

In these settings, simpler models that are auditable, explainable, and fully controlled may be the responsible choice.

#### Key Takeaway

LLMs expand what is possible, but they do not eliminate the need for judgment.

Choosing not to use an LLM can be a sign of analytical maturity rather than technical limitation.

The best practitioners understand both the power of LLMs and their limits—and design systems accordingly.

---

## 23.10 Summary: LLMs as NLP Systems

This chapter reframed large language models as _natural language processing systems_, not intelligent agents or reasoning engines. Their apparent capabilities emerge from scale, training data, and probabilistic sequence modeling—not understanding or intent.

Across the chapter, you followed a consistent progression: tokens are transformed into embeddings, embeddings shape model behavior, and behavior can be influenced through prompting, embeddings, or fine-tuning.

This mirrors earlier chapters in the book. Just as feature engineering shapes traditional models, text representation and interface design shape language model outputs.

#### Key Conceptual Synthesis

- LLMs are _probabilistic sequence models_, not databases or reasoning engines.
- Pretraining provides general language capability; fine-tuning applies _supervised learning_ to specific tasks.
- Prompting influences behavior temporarily through context, not permanent learning.
- Embeddings connect unstructured text to classic analytics workflows.
- Evaluation, overfitting, and bias remain central concerns.

Perhaps most importantly, this chapter reinforced a recurring design principle from earlier in the book: _dataset-specific logic should be isolated, while core modeling and evaluation logic should remain generalizable_.

#### Student Checklist: Using LLMs in Analytics Projects

Before using an LLM in a project, work through the following checklist.

- Is the problem fundamentally _text-based_?
- Would embeddings or classic NLP features solve this more simply?
- Can prompting alone achieve reliable behavior?
- Is labeled data available to justify fine-tuning?
- Have you defined a proper train/test split?
- How will outputs be evaluated and monitored for failure?
- Are cost, latency, privacy, and ethics acceptable?

If you cannot answer these questions clearly, an LLM is likely premature or unnecessary.

#### Choosing the Right Tool: A Decision Framework

The table below summarizes common paths through modern NLP workflows.

#### Final Perspective

LLMs are best understood as powerful extensions of existing NLP and analytics techniques—not replacements for them.

Students who master when and how to use language models, and when not to, will be better prepared to design reliable, responsible, and scalable systems.

In the next assignment, you will apply these principles by selecting an appropriate modeling approach and justifying that choice—not simply using an LLM by default.

---

## 23.11 Practice

The following practice exercises are designed to reinforce both the _conceptual_ and _practical_ skills introduced in this chapter. Some exercises involve writing and running code, while others emphasize evaluation, critique, and judgment.

Unless otherwise stated, all coding exercises can be completed in either Google Colab or a local Python environment.

#### Practice 1: Prompt Design as Interface Control

**Task:** You are using a pretrained text-generation model to summarize customer feedback.

Write two prompts for the same task:

- One prompt that relies only on instructions
- One prompt that includes at least two examples (few-shot prompting)

Run both prompts using the same pretrained model and compare the outputs. Note differences in tone, structure, and consistency.

**Reflection:** In 3–5 sentences, explain which prompt produced more reliable output and why.

Few-shot prompts typically reduce ambiguity by anchoring the model to concrete patterns. Students should observe more consistent structure and task adherence in the example-based prompt, even though both prompts use the same model.

#### Practice 2: Embedding-Based Semantic Similarity

**Task:** You are given a list of short text snippets such as product reviews or support tickets.

Using a pretrained embedding model, perform the following steps:

1. Generate embeddings for each text snippet
1. Compute pairwise cosine similarity
1. Identify the two most semantically similar texts

**Extension:** Change one text slightly (for example, replace synonyms or reorder phrases) and observe how similarity scores change.

Students should use a sentence-level embedding model from the Hugging Face ecosystem and cosine similarity from scikit-learn. Minor wording changes should result in small similarity shifts, reinforcing that embeddings capture meaning rather than exact wording.

#### Practice 3: Classification with Embeddings

**Task:** Build a simple text classifier using embeddings as input features.

Steps:

1. Generate embeddings for labeled text examples
1. Split the data into training and test sets
1. Train a simple classifier (such as logistic regression)
1. Evaluate performance using accuracy or F1 score

**Reflection:** Why might this approach be preferable to fine-tuning when labeled data are limited?

Expected answers should mention stability, reduced overfitting risk, faster training, and easier debugging. Embedding-based classifiers reuse pretrained semantic structure without modifying model weights.

#### Practice 4: Evaluation and Failure Analysis

**Task:** You are shown the following results:

A fine-tuned model achieves 95% accuracy on the training set and 62% accuracy on the test set.

- Identify the most likely issue
- Propose at least two corrective actions

This pattern strongly indicates overfitting. Corrective actions may include reducing training epochs, lowering learning rates, increasing regularization, using parameter-efficient fine-tuning, or switching to an embedding-based approach.

#### Practice 5: Ethics and Appropriate Use

**Scenario:** A company wants to send all customer support emails to a third-party LLM API to auto-generate responses.

Answer the following questions:

- What privacy or ethical risks are present?
- Would embeddings or classical ML be a safer alternative?
- Under what conditions might an LLM still be appropriate?

Students should identify data leakage risks, regulatory concerns, and lack of transparency. Safer alternatives include on-premise models or embedding-based retrieval systems. LLMs may be appropriate if data are anonymized, contracts permit usage, and outputs are carefully monitored.

Together, these exercises reinforce a central theme of the chapter: large language models are powerful tools, but effective analytics requires careful design, evaluation, and judgment.

### Practice: Solutions

This section includes solutions for the Practice problems. Problems that require coding include runnable reference code. Conceptual questions are answered directly in the text.

#### Prompt Design Exercises: Solutions

Most prompt design exercises are conceptual because the goal is to practice writing and revising prompts, not to “get the same output.” Since LLM generation is probabilistic, two students can write good prompts and still see different wording in results.

Solution pattern for instruction prompts: include (1) the task, (2) constraints, (3) the desired format, and (4) a reminder that uncertainty should be stated rather than invented.

Example instruction prompt (good): “Summarize the following customer review in 1 sentence. Use neutral tone. Do not add facts not in the text. If the review is unclear, say what is unclear. Review: …”

Solution pattern for few-shot prompts: add 2–3 short examples that match the task format exactly, then provide the new input and leave a clear “Answer:” or “Output:” slot. Use examples when the task is subjective (tone, rubric, style) or when the desired output structure is strict.

Common failure mode diagnoses: hallucination (model invents details), instruction drift (ignores constraints), sensitivity (small wording changes cause different outputs), and overconfidence (assertive language without evidence). The fix is usually tighter constraints, shorter prompts, clearer formats, and a “don’t guess” clause.

#### Embedding Similarity Tasks: Solutions (Code)

The code below shows a complete, minimal workflow: generate sentence embeddings, compute cosine similarities, and retrieve the most similar items. This can be used for “find the closest pair,” “rank by similarity,” or “retrieve top-k matches” problems.

```python
pip install transformers torch scikit-learn
```

```python
import torch
import numpy as np
from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity

model_name = "sentence-transformers/all-MiniLM-L6-v2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

texts = [
  "The delivery was fast and the product works perfectly.",
  "Shipping was quick and everything arrived in great condition.",
  "Customer service was unhelpful and did not solve my problem.",
  "The item broke after two days and I want a refund.",
  "Support helped me fix the issue within minutes."
]

def mean_pooling(model_output, attention_mask):
  token_embeddings = model_output.last_hidden_state
  input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
  summed = torch.sum(token_embeddings * input_mask_expanded, dim=1)
  counts = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
  return summed / counts

def embed_texts(text_list):
  encoded = tokenizer(text_list, padding=True, truncation=True, return_tensors="pt")
  with torch.no_grad():
    output = model(**encoded)
  emb = mean_pooling(output, encoded["attention_mask"])
  return emb.cpu().numpy()

embeddings = embed_texts(texts)
sim = cosine_similarity(embeddings, embeddings)

# Find the most similar pair (excluding the diagonal)
best_i, best_j, best_score = None, None, -1
n = len(texts)
for i in range(n):
  for j in range(i + 1, n):
    if sim[i, j] > best_score:
      best_score = sim[i, j]
      best_i, best_j = i, j

print("Most similar pair:")
print("A:", texts[best_i])
print("B:", texts[best_j])
print("Cosine similarity:", round(float(best_score), 4))

# Top-k retrieval example: most similar to a query
query = "My package arrived quickly and in perfect shape."
query_emb = embed_texts([query])
scores = cosine_similarity(query_emb, embeddings)[0]
ranked = np.argsort(-scores)

k = 3
print("\nQuery:", query)
print(f"Top-{k} matches:")
for idx in ranked[:k]:
  print("-", texts[idx], "(score:", round(float(scores[idx]), 4), ")")
```

#### Evaluation Critique: Solutions

If training performance is high but test performance is much lower, the correct diagnosis is overfitting (the model memorized patterns that do not generalize). The correct response is to reduce training intensity (fewer epochs, smaller learning rate), add validation-based early stopping, improve data quality/quantity, or switch to a simpler baseline such as embeddings + a classic classifier.

If accuracy is high but recall for the important class is low, accuracy is a misleading metric. The correct critique is to prioritize precision/recall, F1, and confusion matrices, and to evaluate class imbalance and decision thresholds.

If a generation system “looks good” in a few examples but fails under minor rewording, the correct critique is brittleness. The fix is structured test sets (paraphrases, edge cases), constrained outputs, and evaluation that includes adversarial or stress-test prompts.

#### Ethics Scenarios: Solutions

If the scenario includes sensitive or regulated data, the safest recommendation is to avoid external LLM APIs unless you have explicit approval, a documented data agreement, and a clear retention policy. A common alternative is local embeddings (or classical ML) plus rules/templates for high-control outputs.

If the task requires deterministic, auditable decisions (for example, compliance routing or eligibility decisions), the correct recommendation is to use interpretable, controlled methods (rules or classic supervised models) and treat any LLM output as non-authoritative assistance that must be verified.

If the system could cause harm when wrong (medical, legal, financial, or high-stakes HR decisions), the correct recommendation is to treat the LLM as optional drafting support at most, require human review, log decisions, and implement monitoring for bias and systematic failure patterns.

---

## 23.12 Assignment

Complete the assignment below:

---
