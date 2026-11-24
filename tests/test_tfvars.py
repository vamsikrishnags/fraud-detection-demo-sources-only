from scripts.tfvars import write_tfvars_for_deployment


def test_requires_key_and_secret(tmp_path):
    (tmp_path / "terraform").mkdir()
    assert not write_tfvars_for_deployment(tmp_path, {})
    assert not write_tfvars_for_deployment(tmp_path, {"TF_VAR_confluent_cloud_api_key": "k"})


def test_writes_tfvars(tmp_path):
    (tmp_path / "terraform").mkdir()
    creds = {"TF_VAR_confluent_cloud_api_key": "k", "TF_VAR_confluent_cloud_api_secret": "s"}
    assert write_tfvars_for_deployment(tmp_path, creds)
    text = (tmp_path / "terraform" / "terraform.tfvars").read_text()
    assert 'confluent_cloud_api_key    = "k"' in text and 'confluent_cloud_api_secret = "s"' in text
