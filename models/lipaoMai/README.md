---
license: apache-2.0
base_model: distilbert-base-uncased
tags:
- generated_from_trainer
metrics:
- accuracy
- f1
- precision
- recall
model-index:
- name: BERT-sentiment-analysis-portuguese
  results: []
---

<!-- This model card has been generated automatically according to the information the Trainer had access to. You
should probably proofread and complete it, then remove this comment. -->

# BERT-sentiment-analysis-portuguese

This model is a fine-tuned version of [distilbert-base-uncased](https://huggingface.co/distilbert-base-uncased) on the None dataset.
It achieves the following results on the evaluation set:
- Loss: 0.2375
- Accuracy: 0.9229
- F1: 0.9219
- Precision: 0.9215
- Recall: 0.9229

## Model description

More information needed

## Intended uses & limitations

More information needed

## Training and evaluation data

More information needed

## Training procedure

### Training hyperparameters

The following hyperparameters were used during training:
- learning_rate: 2e-05
- train_batch_size: 16
- eval_batch_size: 16
- seed: 42
- optimizer: Adam with betas=(0.9,0.999) and epsilon=1e-08
- lr_scheduler_type: linear
- lr_scheduler_warmup_ratio: 0.03
- num_epochs: 2

### Training results

| Training Loss | Epoch | Step | Validation Loss | Accuracy | F1     | Precision | Recall |
|:-------------:|:-----:|:----:|:---------------:|:--------:|:------:|:---------:|:------:|
| 0.1335        | 1.0   | 3340 | 0.2300          | 0.9171   | 0.9151 | 0.9149    | 0.9171 |
| 0.3973        | 2.0   | 6680 | 0.2375          | 0.9229   | 0.9219 | 0.9215    | 0.9229 |


### Framework versions

- Transformers 4.42.4
- Pytorch 2.3.1+cu121
- Datasets 2.20.0
- Tokenizers 0.19.1
