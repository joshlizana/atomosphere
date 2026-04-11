# Sentiment Model Research — cardiffnlp/twitter-xlm-roberta-base-sentiment

## Model Details
- **HuggingFace ID:** cardiffnlp/twitter-xlm-roberta-base-sentiment
- **Architecture:** XLM-RoBERTa-base, cross-lingual transformer
- **Training data:** ~198M tweets, fine-tuned on 8 languages (Ar, En, Fr, De, Hi, It, Sp, Pt)
- **Model size:** ~1.1 GB
- **Max tokens:** 512 (XLM-RoBERTa default)
- **Downloads:** ~918K/month

## Label Mapping (id2label)
- 0 → Negative
- 1 → Neutral
- 2 → Positive

## Pipeline Usage (simplest)
```python
from transformers import pipeline
model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)
sentiment_task("T'estimo!")
# Output: [{'label': 'Positive', 'score': 0.66}]
```

Pipeline supports `batch_size=64` and `top_k=None` to get all 3 scores:
```python
results = sentiment_task(texts, batch_size=64, top_k=None, truncation=True, max_length=512)
# Each result: [{'label': 'Negative', 'score': 0.03}, {'label': 'Neutral', 'score': 0.20}, {'label': 'Positive', 'score': 0.77}]
```

## Manual Usage (for custom batching)
```python
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax

MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

text = preprocess(text)  # replace @mentions → @user, URLs → http
encoded = tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
output = model(**encoded)
scores = softmax(output[0][0].detach().numpy())
```

## Preprocessing
The model expects:
- Replace @mentions (starting with `@`, length > 1) with `@user`
- Replace URLs (starting with `http`) with `http`

```python
def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)
```

## mapInPandas Pattern (PySpark 4.x)
```python
def predict_sentiment(iterator):
    # Load model ONCE per worker (stays in memory across batches)
    pipe = pipeline("sentiment-analysis", model=MODEL_PATH, device=device, ...)
    for batch_df in iterator:
        # Process batch
        texts = batch_df["text"].fillna("").tolist()
        results = pipe(texts, batch_size=64, top_k=None, truncation=True, max_length=512)
        # Extract scores and add columns
        yield output_df

df.mapInPandas(predict_sentiment, schema=output_schema)
```

Key: The function receives an **iterator of pandas DataFrames** and yields pandas DataFrames.
Model is loaded once at the top of the function (before the for loop), so it persists across batches within the same worker.

## Docker Image Strategy
- Base: NVIDIA CUDA 12.x runtime + Spark 4.x
- Install: PyTorch (CPU+CUDA), transformers, accelerate
- Bake model at build time:
  ```dockerfile
  RUN python -c "from transformers import pipeline; pipeline('sentiment-analysis', model='cardiffnlp/twitter-xlm-roberta-base-sentiment')"
  ```
- This downloads and caches the model in the image (~1.1 GB)
- Container starts with zero runtime downloads

## GPU/CPU Adaptation
```python
import torch
device = 0 if torch.cuda.is_available() else -1  # 0=first GPU, -1=CPU
pipe = pipeline("sentiment-analysis", model=MODEL_PATH, device=device)
```

## Sources
- https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html
- https://docs.databricks.com/en/machine-learning/train-model/huggingface/model-inference-nlp.html
