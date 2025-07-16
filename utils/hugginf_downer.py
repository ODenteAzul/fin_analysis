from huggingface_hub import snapshot_download

# Primeiro modelo
snapshot_download(
    repo_id="pierreguillou/bert-base-cased-sentiment-analysis-sst2-pt",
    local_dir="models/pierreguillou",
    local_dir_use_symlinks=False
)
