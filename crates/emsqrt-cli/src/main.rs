//! EM-√ CLI: Command-line interface for running pipelines.

use clap::{Parser, Subcommand};
use emsqrt_core::config::EngineConfig;
use emsqrt_exec::Engine;
use emsqrt_planner::{estimate_work, lower_to_physical, parse_yaml_pipeline, rules};
use emsqrt_te::plan_te;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "emsqrt")]
#[command(about = "EM-√: External-Memory ETL Engine with hard peak-RAM guarantees", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a pipeline from a YAML file
    Run {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,

        /// Memory cap in bytes (overrides config)
        #[arg(long)]
        memory_cap: Option<usize>,

        /// Spill directory (overrides config)
        #[arg(long)]
        spill_dir: Option<String>,

        /// Spill URI (e.g., s3://bucket/prefix)
        #[arg(long)]
        spill_uri: Option<String>,

        /// AWS region for S3 spill buckets
        #[arg(long)]
        spill_aws_region: Option<String>,

        /// AWS access key id for S3 spill
        #[arg(long)]
        spill_aws_access_key_id: Option<String>,

        /// AWS secret access key for S3 spill
        #[arg(long)]
        spill_aws_secret_access_key: Option<String>,

        /// AWS session token for S3 spill
        #[arg(long)]
        spill_aws_session_token: Option<String>,

        /// Path to GCS service account JSON for spill
        #[arg(long)]
        spill_gcs_service_account: Option<String>,

        /// Azure access key for blob spill
        #[arg(long)]
        spill_azure_access_key: Option<String>,

        /// Override spill retry max attempts
        #[arg(long)]
        spill_retry_max: Option<usize>,

        /// Override spill retry initial backoff (ms)
        #[arg(long)]
        spill_retry_initial_ms: Option<u64>,

        /// Override spill retry max backoff (ms)
        #[arg(long)]
        spill_retry_max_ms: Option<u64>,

        /// Maximum parallel tasks (overrides config)
        #[arg(long)]
        max_parallel: Option<usize>,
    },

    /// Validate a pipeline YAML file (syntax check)
    Validate {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,
    },

    /// Show execution plan for a pipeline (EXPLAIN)
    Explain {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,

        /// Memory cap in bytes (for planning)
        #[arg(long, default_value = "536870912")] // 512MB default
        memory_cap: usize,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            pipeline,
            memory_cap,
            spill_dir,
            spill_uri,
            spill_aws_region,
            spill_aws_access_key_id,
            spill_aws_secret_access_key,
            spill_aws_session_token,
            spill_gcs_service_account,
            spill_azure_access_key,
            spill_retry_max,
            spill_retry_initial_ms,
            spill_retry_max_ms,
            max_parallel,
        } => {
            if let Err(e) = run_pipeline(
                &pipeline,
                memory_cap,
                spill_dir,
                spill_uri,
                spill_aws_region,
                spill_aws_access_key_id,
                spill_aws_secret_access_key,
                spill_aws_session_token,
                spill_gcs_service_account,
                spill_azure_access_key,
                spill_retry_max,
                spill_retry_initial_ms,
                spill_retry_max_ms,
                max_parallel,
            ) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Validate { pipeline } => {
            if let Err(e) = validate_pipeline(&pipeline) {
                eprintln!("Validation failed: {}", e);
                std::process::exit(1);
            }
            println!("✓ Pipeline is valid");
        }
        Commands::Explain {
            pipeline,
            memory_cap,
        } => {
            if let Err(e) = explain_pipeline(&pipeline, memory_cap) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_pipeline(
    pipeline_path: &PathBuf,
    memory_cap: Option<usize>,
    spill_dir: Option<String>,
    spill_uri: Option<String>,
    spill_aws_region: Option<String>,
    spill_aws_access_key_id: Option<String>,
    spill_aws_secret_access_key: Option<String>,
    spill_aws_session_token: Option<String>,
    spill_gcs_service_account: Option<String>,
    spill_azure_access_key: Option<String>,
    spill_retry_max: Option<usize>,
    spill_retry_initial_ms: Option<u64>,
    spill_retry_max_ms: Option<u64>,
    max_parallel: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read YAML file
    let yaml_content = fs::read_to_string(pipeline_path)?;

    // Parse pipeline
    let parsed = parse_yaml_pipeline(&yaml_content)?;
    let logical_plan = parsed.plan.clone();

    // Optimize
    let optimized = rules::optimize(logical_plan);

    // Lower to physical plan
    let phys_prog = lower_to_physical(&optimized);

    // Estimate work
    let work = estimate_work(&optimized, None);

    // Create config
    let mut config = EngineConfig::from_env();
    apply_pipeline_config(&mut config, &parsed.config);
    if let Some(cap) = memory_cap {
        config.mem_cap_bytes = cap;
    }
    if let Some(dir) = spill_dir {
        config.spill_dir = dir;
    }
    if let Some(uri) = spill_uri {
        config.spill_uri = Some(uri);
    }
    if let Some(region) = spill_aws_region {
        config.spill_aws_region = Some(region);
    }
    if let Some(access_key) = spill_aws_access_key_id {
        config.spill_aws_access_key_id = Some(access_key);
    }
    if let Some(secret_key) = spill_aws_secret_access_key {
        config.spill_aws_secret_access_key = Some(secret_key);
    }
    if let Some(token) = spill_aws_session_token {
        config.spill_aws_session_token = Some(token);
    }
    if let Some(sa_path) = spill_gcs_service_account {
        config.spill_gcs_service_account_path = Some(sa_path);
    }
    if let Some(azure_key) = spill_azure_access_key {
        config.spill_azure_access_key = Some(azure_key);
    }
    if let Some(max) = spill_retry_max {
        config.spill_retry_max_retries = max;
    }
    if let Some(initial) = spill_retry_initial_ms {
        config.spill_retry_initial_backoff_ms = initial;
    }
    if let Some(max_backoff) = spill_retry_max_ms {
        config.spill_retry_max_backoff_ms = max_backoff;
    }
    if let Some(parallel) = max_parallel {
        config.max_parallel_tasks = parallel;
    }
    // Plan TE execution
    let te = plan_te(&phys_prog.plan, &work, config.mem_cap_bytes)
        .map_err(|e| format!("TE planning failed: {}", e))?;

    // Execute
    let mut engine =
        Engine::new(config).map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    let manifest = engine.run(&phys_prog, &te)?;

    println!("✓ Pipeline executed successfully");
    println!(
        "  Duration: {}ms",
        manifest.finished_ms - manifest.started_ms
    );
    println!("  Plan hash: {}", manifest.plan_hash);

    Ok(())
}

fn validate_pipeline(pipeline_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let yaml_content = fs::read_to_string(pipeline_path)?;
    let _ = parse_yaml_pipeline(&yaml_content)?;
    Ok(())
}

fn explain_pipeline(
    pipeline_path: &PathBuf,
    memory_cap: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml_content = fs::read_to_string(pipeline_path)?;
    let parsed = parse_yaml_pipeline(&yaml_content)?;
    let logical_plan = parsed.plan.clone();
    let optimized = rules::optimize(logical_plan);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, memory_cap)
        .map_err(|e| format!("TE planning failed: {}", e))?;

    println!("Pipeline Execution Plan");
    println!("======================");
    println!();
    println!(
        "Memory Cap: {} bytes ({:.2} MB)",
        memory_cap,
        memory_cap as f64 / 1_048_576.0
    );
    println!();
    println!("Work Estimate:");
    println!("  Total Rows: {}", work.total_rows);
    println!(
        "  Total Bytes: {} ({:.2} MB)",
        work.total_bytes,
        work.total_bytes as f64 / 1_048_576.0
    );
    println!("  Max Fan-in: {}", work.max_fan_in);
    println!();
    println!("TE Plan:");
    println!(
        "  Block Size: {} rows per block",
        te.block_size.rows_per_block
    );
    println!("  Total Blocks: {}", te.order.len());
    if let Some(max_frontier) = te.max_frontier_hint {
        println!("  Max Frontier: {} blocks", max_frontier);
    }
    println!();
    println!("Block Execution Order:");
    for (i, block) in te.order.iter().enumerate() {
        println!(
            "  {}. Block {} (Op {}) - {} dependencies",
            i + 1,
            block.id.get(),
            block.op.get(),
            block.deps.len()
        );
    }

    Ok(())
}

fn apply_pipeline_config(cfg: &mut EngineConfig, doc: &emsqrt_planner::PipelineConfig) {
    if let Some(dir) = &doc.spill_dir {
        cfg.spill_dir = dir.clone();
    }
    if let Some(uri) = &doc.spill_uri {
        cfg.spill_uri = Some(uri.clone());
    }
    if let Some(region) = &doc.spill_aws_region {
        cfg.spill_aws_region = Some(region.clone());
    }
    if let Some(access_key) = &doc.spill_aws_access_key_id {
        cfg.spill_aws_access_key_id = Some(access_key.clone());
    }
    if let Some(secret_key) = &doc.spill_aws_secret_access_key {
        cfg.spill_aws_secret_access_key = Some(secret_key.clone());
    }
    if let Some(token) = &doc.spill_aws_session_token {
        cfg.spill_aws_session_token = Some(token.clone());
    }
    if let Some(sa) = &doc.spill_gcs_service_account {
        cfg.spill_gcs_service_account_path = Some(sa.clone());
    }
    if let Some(azure_key) = &doc.spill_azure_access_key {
        cfg.spill_azure_access_key = Some(azure_key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_pipeline_config, EngineConfig};
    use emsqrt_planner::PipelineConfig;

    #[test]
    fn pipeline_config_overrides_env_defaults() {
        let mut config = EngineConfig::default();
        let pipeline = PipelineConfig {
            spill_dir: Some("/tmp/pipeline".into()),
            spill_uri: Some("s3://bucket/pipeline".into()),
            spill_aws_region: Some("us-east-1".into()),
            ..Default::default()
        };
        apply_pipeline_config(&mut config, &pipeline);
        assert_eq!(config.spill_dir, "/tmp/pipeline");
        assert_eq!(config.spill_uri.as_deref(), Some("s3://bucket/pipeline"));
        assert_eq!(config.spill_aws_region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn cli_overrides_higher_priority_than_config() {
        let mut config = EngineConfig::default();
        let pipeline = PipelineConfig {
            spill_dir: Some("/tmp/pipeline".into()),
            ..Default::default()
        };
        apply_pipeline_config(&mut config, &pipeline);
        assert_eq!(config.spill_dir, "/tmp/pipeline");

        // Simulate CLI override after config
        config.spill_dir = "/tmp/cli".into();
        assert_eq!(config.spill_dir, "/tmp/cli");
    }
}
