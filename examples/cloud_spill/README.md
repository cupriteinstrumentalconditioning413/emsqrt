# Cloud Spill Example

Demonstrates how to configure spills to write to S3 via the `config` block instead of CLI flags.

```bash
emsqrt run \
  --pipeline examples/cloud_spill/pipeline.yaml \
  --memory-cap 536870912 \
  --spill-aws-access-key-id AKIA... \
  --spill-aws-secret-access-key SECRET... \
  --spill-aws-session-token optional-session
```

The YAML pipeline sets `spill_uri`, `spill_aws_region`, and a retry timeout so you only need to provide credentials on the CLI or via environment variables (`EMSQRT_SPILL_AWS_ACCESS_KEY_ID`, etc.).

